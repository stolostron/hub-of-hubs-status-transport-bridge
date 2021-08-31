package kafkaclient

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaClient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

const (
	commitRescheduleDelay = time.Second * 5
	bufferedChannelSize   = 500
)

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger) (*Consumer, error) {
	msgChan := make(chan *kafka.Message, bufferedChannelSize)

	kafkaConsumer, err := kafkaClient.NewKafkaConsumer(msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &Consumer{
		log:                             log,
		kafkaConsumer:                   kafkaConsumer,
		bundleCommitsChan:               make(chan interface{}, bufferedChannelSize),
		bundleCommitsRetryChan:          make(chan interface{}, bufferedChannelSize),
		msgChan:                         msgChan,
		msgIDToChanMap:                  make(map[string]chan bundle.Bundle),
		msgIDToRegistrationMap:          make(map[string]*transport.BundleRegistration),
		partitionIDToCommittedOffsetMap: make(map[int32]kafka.Offset),
		bundleToMsgMap:                  make(map[interface{}]*kafka.Message),
		stopChan:                        make(chan struct{}),
	}, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log                    logr.Logger
	kafkaConsumer          *kafkaClient.KafkaConsumer
	bundleCommitsChan      chan interface{}
	bundleCommitsRetryChan chan interface{}
	msgChan                chan *kafka.Message
	msgIDToChanMap         map[string]chan bundle.Bundle
	msgIDToRegistrationMap map[string]*transport.BundleRegistration

	partitionIDToCommittedOffsetMap map[int32]kafka.Offset
	bundleToMsgMap                  map[interface{}]*kafka.Message

	stopChan  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
}

// Register function registers a msgID to the bundle updates channel.
func (c *Consumer) Register(registration *transport.BundleRegistration, bundleUpdatesChan chan bundle.Bundle) {
	c.msgIDToChanMap[registration.MsgID] = bundleUpdatesChan
	c.msgIDToRegistrationMap[registration.MsgID] = registration
}

// CommitAsync commits a transported message that was processed locally.
func (c *Consumer) CommitAsync(bundle interface{}) {
	c.bundleCommitsChan <- bundle
}

// Start function starts the consumer.
func (c *Consumer) Start() error {
	if err := c.kafkaConsumer.Subscribe(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	c.startOnce.Do(func() {
		go c.handleCommits()
		go c.handleKafkaMessages()
		go c.handleCommitRetries()
	})

	return nil
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		c.kafkaConsumer.Close()
		close(c.stopChan)
		c.kafkaConsumer.Close()
		close(c.msgChan)
		close(c.bundleCommitsChan)
	})
}

func (c *Consumer) handleKafkaMessages() {
	for {
		select {
		case <-c.stopChan:
			return

		case msg := <-c.msgChan:
			c.processMessage(msg)
		}
	}
}

func (c *Consumer) handleCommits() {
	for {
		select {
		case <-c.stopChan:
			return

		case offset := <-c.bundleCommitsChan:
			c.commitOffset(offset)
		}
	}
}

func (c *Consumer) handleCommitRetries() {
	ticker := time.NewTicker(commitRescheduleDelay)

	type bundleInfo struct {
		bundle interface{}
		offset kafka.Offset
	}

	partitionIDToHighestBundleMap := make(map[int32]bundleInfo)

	for {
		select {
		case <-c.stopChan:
			return

		case <-ticker.C:
			for partition, highestBundle := range partitionIDToHighestBundleMap {
				go func() {
					// TODO reschedule, should use exponential back off
					c.bundleCommitsChan <- highestBundle.bundle
				}()

				delete(partitionIDToHighestBundleMap, partition)
			}

		case bundleToRetry := <-c.bundleCommitsRetryChan:
			msg, exists := c.bundleToMsgMap[bundleToRetry]
			if !exists {
				continue
			}

			msgPartition := msg.TopicPartition.Partition
			msgOffset := msg.TopicPartition.Offset

			highestBundle, found := partitionIDToHighestBundleMap[msgPartition]
			if found && highestBundle.offset > msgOffset {
				c.log.Info("transport dropped low commit retry request",
					"topic", *msg.TopicPartition.Topic,
					"partition", msg.TopicPartition.Partition,
					"offset", msgOffset)

				continue
			}

			// update partition mapping
			partitionIDToHighestBundleMap[msgPartition] = bundleInfo{
				bundle: bundleToRetry,
				offset: msgOffset,
			}
		}
	}
}

func (c *Consumer) commitOffset(bundle interface{}) {
	msg, exists := c.bundleToMsgMap[bundle]
	if !exists {
		return
	}

	msgOffset := msg.TopicPartition.Offset
	msgPartition := msg.TopicPartition.Partition

	if msgOffset > c.partitionIDToCommittedOffsetMap[msgPartition] {
		if err := c.kafkaConsumer.Commit(msg); err != nil {
			c.log.Error(err, "transport failed to commit message",
				"topic", *msg.TopicPartition.Topic,
				"partition", msg.TopicPartition.Partition,
				"offset", msgOffset)

			go func() {
				c.bundleCommitsRetryChan <- bundle
			}()

			return
		}

		delete(c.bundleToMsgMap, bundle)
		c.partitionIDToCommittedOffsetMap[msgPartition] = msgOffset
		c.log.Info("transport committed message", "topic", *msg.TopicPartition.Topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msgOffset)
	} else {
		// a more recent message was committed, drop current
		delete(c.bundleToMsgMap, bundle)
		c.log.Info("transport dropped low commit request", "topic", *msg.TopicPartition.Topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msgOffset)
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) {
	transportMsg := &transport.Message{}

	err := json.Unmarshal(msg.Value, transportMsg)
	if err != nil {
		c.log.Error(err, "failed to parse bundle", "object id", msg.Key)

		return
	}

	msgID := strings.Split(transportMsg.ID, ".")[1] // msg id is LH_ID.MSG_ID
	if _, found := c.msgIDToRegistrationMap[msgID]; !found {
		c.log.Info("no registration available, not sending bundle", "object id", transportMsg.ID)
		// no one registered for this msg id
		return
	}

	if !c.msgIDToRegistrationMap[msgID].Predicate() {
		c.log.Info("predicate is false, not sending bundle", "object id", transportMsg.ID)
		return // registration predicate is false, do not send the update in the channel
	}

	receivedBundle := c.msgIDToRegistrationMap[msgID].CreateBundleFunc()
	if err := json.Unmarshal(transportMsg.Payload, receivedBundle); err != nil {
		c.log.Error(err, "failed to parse bundle", "object id", msg.Key)
		return
	}

	// map bundle internally
	c.bundleToMsgMap[receivedBundle] = msg
	c.msgIDToChanMap[msgID] <- receivedBundle
}
