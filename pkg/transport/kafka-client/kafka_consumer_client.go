package kafkaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger) (*Consumer, error) {
	kc := &Consumer{
		log:                    log,
		kafkaConsumer:          nil,
		commitsChan:            make(chan interface{}),
		msgChan:                make(chan *kafka.Message),
		msgIDToChanMap:         make(map[string]chan bundle.Bundle),
		msgIDToRegistrationMap: make(map[string]*transport.BundleRegistration),
		bundleToMsgMap:         make(map[interface{}]*kafka.Message),
	}

	kafkaConsumer, err := kclient.NewKafkaConsumer(kc.msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	kc.kafkaConsumer = kafkaConsumer

	return kc, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log                    logr.Logger
	kafkaConsumer          *kclient.KafkaConsumer
	commitsChan            chan interface{}
	msgChan                chan *kafka.Message
	msgIDToChanMap         map[string]chan bundle.Bundle
	msgIDToRegistrationMap map[string]*transport.BundleRegistration

	committedOffset int64
	bundleToMsgMap  map[interface{}]*kafka.Message
}

// Register function registers a msgID to the bundle updates channel.
func (c *Consumer) Register(registration *transport.BundleRegistration, bundleUpdatesChan chan bundle.Bundle) {
	c.msgIDToChanMap[registration.MsgID] = bundleUpdatesChan
	c.msgIDToRegistrationMap[registration.MsgID] = registration
}

// CommitAsync commits a transported message that was processed locally.
func (c *Consumer) CommitAsync(bundle interface{}) {
	c.commitsChan <- bundle
}

// Start function starts Consumer.
func (c *Consumer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	// begin subscription
	err := c.kafkaConsumer.Subscribe()
	if err != nil {
		c.log.Error(err, "failed to start kafka consumer: subscribe failed")
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	go c.handleCommits(ctx)
	go c.handleKafkaMessages(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel.
		cancelContext()

		c.kafkaConsumer.Close()
		close(c.msgChan)
		close(c.commitsChan)

		c.log.Info("stopped Consumer")

		return nil
	}
}

func (c *Consumer) handleKafkaMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.log.Info("stopped kafka message handler")
			return

		case msg := <-c.msgChan:
			c.processMessage(msg)
		}
	}
}

func (c *Consumer) handleCommits(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.log.Info("stopped offset committing handler")
			return

		case offset := <-c.commitsChan:
			c.commitOffset(offset)
		}
	}
}

func (c *Consumer) commitOffset(bundle interface{}) {
	msg, exists := c.bundleToMsgMap[bundle]
	if !exists {
		return
	}

	offset := int64(msg.TopicPartition.Offset)
	if offset > c.committedOffset {
		if _, err := c.kafkaConsumer.Consumer().CommitMessage(msg); err != nil {
			c.log.Info("transport failed to commit message", "topic", *msg.TopicPartition.Topic,
				"partition", msg.TopicPartition.Partition,
				"offset", offset)

			return
		}

		delete(c.bundleToMsgMap, bundle)
		c.committedOffset = offset
		c.log.Info("transport committed message",
			"topic", *msg.TopicPartition.Topic,
			"partition", msg.TopicPartition.Partition,
			"offset", offset)
	} else {
		// a more recent message was committed, drop current
		delete(c.bundleToMsgMap, bundle)
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) {
	transportMsg := &transport.Message{}

	err := json.Unmarshal(msg.Value, transportMsg)
	if err != nil {
		c.log.Error(err, "failed to parse bundle",
			"ObjectId", msg.Key)

		return
	}

	msgID := strings.Split(transportMsg.ID, ".")[1] // msg id is LH_ID.MSG_ID
	if _, found := c.msgIDToRegistrationMap[msgID]; !found {
		c.log.Info("no registration available, not sending bundle", "ObjectId",
			transportMsg.ID)
		// no one registered for this msg id
		return
	}

	if !c.msgIDToRegistrationMap[msgID].Predicate() {
		c.log.Info("Predicate is false, not sending bundle", "ObjectId",
			transportMsg.ID)
		return // registration predicate is false, do not send the update in the channel
	}

	receivedBundle := c.msgIDToRegistrationMap[msgID].CreateBundleFunc()
	if err := json.Unmarshal(transportMsg.Payload, receivedBundle); err != nil {
		c.log.Error(err, "failed to parse bundle", "ObjectId", msg.Key)
		return
	}

	// map bundle internally
	c.bundleToMsgMap[receivedBundle] = msg
	c.msgIDToChanMap[msgID] <- receivedBundle
}
