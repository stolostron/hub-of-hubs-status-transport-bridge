package kafka

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaClient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	bufferedChannelSize = 500
)

// CommitPositionsFunc is the function the kafka transport provides for committing positions.
type CommitPositionsFunc func(positions map[int32]*transport.BundleMetadata) error

// NewConsumer creates a new instance of User.
func NewConsumer(log logr.Logger, conflationManager *conflator.ConflationManager) (*Consumer, error) {
	msgChan := make(chan *kafka.Message, bufferedChannelSize)

	kafkaConsumer, err := kafkaClient.NewKafkaConsumer(msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	consumer := &Consumer{
		log:                    log,
		kafkaConsumer:          kafkaConsumer,
		conflationManager:      conflationManager,
		msgChan:                msgChan,
		msgIDToRegistrationMap: make(map[string]*transport.BundleRegistration),
		commitsMap:             make(map[string]map[int32]kafka.Offset),
		stopChan:               make(chan struct{}),
	}

	// create committer
	consumer.committer, err = NewCommitter(ctrl.Log.WithName("kafka transport committer"), consumer.commitPositions)
	if err != nil {
		return nil, fmt.Errorf("failed to create committer: %w", err)
	}

	return consumer, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log                    logr.Logger
	kafkaConsumer          *kafkaClient.KafkaConsumer
	conflationManager      *conflator.ConflationManager
	committer              *Committer
	msgChan                chan *kafka.Message
	msgIDToRegistrationMap map[string]*transport.BundleRegistration
	commitsMap             map[string]map[int32]kafka.Offset
	stopChan               chan struct{}
	startOnce              sync.Once
	stopOnce               sync.Once
}

// Register function registers a msgID to the bundle updates channel.
func (c *Consumer) Register(registration *transport.BundleRegistration) {
	c.msgIDToRegistrationMap[registration.MsgID] = registration
}

// Start function starts the consumer.
func (c *Consumer) Start() error {
	if err := c.kafkaConsumer.Subscribe(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	c.startOnce.Do(func() {
		go c.committer.Start(c.stopChan)
		go c.handleKafkaMessages()
	})

	return nil
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		c.kafkaConsumer.Close()
		close(c.stopChan)
	})
}

// commitPositions commits the given positions (by metadata) per partition mapped.
func (c *Consumer) commitPositions(positions map[int32]*transport.BundleMetadata) error {
	// go over positions and commit
	for _, metadata := range positions {
		if err := c.commitAsync(metadata); err != nil {
			return fmt.Errorf("failed to commit offset, stopping bulk commit - %w", err)
		}
	}

	return nil
}

func (c *Consumer) commitAsync(metadata *transport.BundleMetadata) error {
	// skip request if already committed this data
	if partitionsMap, found := c.commitsMap[metadata.Topic]; found {
		if committedOffset, found := partitionsMap[metadata.Partition]; found {
			if int64(committedOffset) >= metadata.Offset {
				return nil
			}
		}
	}

	offsets := []kafka.TopicPartition{
		{
			Topic:     &metadata.Topic,
			Partition: metadata.Partition,
			Offset:    kafka.Offset(metadata.Offset),
		},
	}

	if _, err := c.kafkaConsumer.Consumer().CommitOffsets(offsets); err != nil {
		return fmt.Errorf("%w", err)
	}

	// log success and update commitsMap
	c.log.Info("committed offset", "topic", metadata.Topic, "partition", metadata.Partition, "offset",
		metadata.Offset)
	c.updateOffsetsMap(c.commitsMap, metadata.Topic, metadata.Partition, kafka.Offset(metadata.Offset))

	return nil
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

func (c *Consumer) processMessage(msg *kafka.Message) {
	transportMsg := &transport.Message{}

	err := json.Unmarshal(msg.Value, transportMsg)
	if err != nil {
		c.log.Error(err, "failed to parse transport message", "object id", msg.Key)

		return
	}

	msgID := strings.Split(transportMsg.ID, ".")[1] // msg id is LH_ID.MSG_ID
	if _, found := c.msgIDToRegistrationMap[msgID]; !found {
		c.log.Info("no registration available, not sending bundle", "bundle id", transportMsg.ID,
			"bundle version", transportMsg.Version)
		// no one registered for this msg id
		return
	}

	if !c.msgIDToRegistrationMap[msgID].Predicate() {
		c.log.Info("predicate is false, not sending bundle", "bundle id", transportMsg.ID,
			"bundle version", transportMsg.Version)

		return // registration predicate is false, do not send the update in the channel
	}

	receivedBundle := c.msgIDToRegistrationMap[msgID].CreateBundleFunc()
	if err := json.Unmarshal(transportMsg.Payload, receivedBundle); err != nil {
		c.log.Error(err, "failed to parse bundle",
			"bundle id", transportMsg.ID,
			"bundle version", transportMsg.Version)

		return
	}

	// add conflation unit to committer (committer takes care of adding it once).
	c.committer.AddUser(c.conflationManager.Insert(receivedBundle, transport.BundleMetadata{
		Topic:     *msg.TopicPartition.Topic,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Processed: false,
	}))
}

func (c *Consumer) updateOffsetsMap(offsetsMap map[string]map[int32]kafka.Offset, topic string, partition int32,
	offset kafka.Offset) {
	// check if topic is in the map
	if partitionsMap, found := offsetsMap[topic]; found {
		// check if partition is in topic map
		if offsetInMap, found := partitionsMap[partition]; !found || (found && offsetInMap < offset) {
			// update partition's offset if partition hasn't an offset yet or the new offset is higher.
			partitionsMap[partition] = offset
		}
	} else {
		// create topic and insert pair
		offsetsMap[topic] = map[int32]kafka.Offset{partition: offset}
	}
}
