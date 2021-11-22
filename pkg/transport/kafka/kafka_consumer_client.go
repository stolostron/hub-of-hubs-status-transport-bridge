package kafka

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaClient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	kafkaHeaderTypes "github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
	"github.com/open-cluster-management/hub-of-hubs-message-compression"
	"github.com/open-cluster-management/hub-of-hubs-message-compression/compressors"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/statistics"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	bufferedChannelSize = 500
)

// CommitPositionsFunc is the function the kafka transport provides for committing positions.
type CommitPositionsFunc func(positions map[int32]*BundleMetadata) error

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, conflationManager *conflator.ConflationManager,
	statistics *statistics.Statistics) (*Consumer, error) {
	msgChan := make(chan *kafka.Message, bufferedChannelSize)

	kafkaConsumer, err := kafkaClient.NewKafkaConsumer(msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	consumer := &Consumer{
		log:                    log,
		kafkaConsumer:          kafkaConsumer,
		compressorsMap:         make(map[compressor.CompressionType]compressors.Compressor),
		conflationManager:      conflationManager,
		statistics:             statistics,
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

	// add conflation manager to committer consumers
	consumer.committer.AddTransportConsumer(conflationManager)

	return consumer, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log                    logr.Logger
	kafkaConsumer          *kafkaClient.KafkaConsumer
	compressorsMap         map[compressor.CompressionType]compressors.Compressor
	conflationManager      *conflator.ConflationManager
	statistics             *statistics.Statistics
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
func (c *Consumer) commitPositions(positions map[int32]*BundleMetadata) error {
	// go over positions and commit
	for _, metadata := range positions {
		if err := c.commitAsync(metadata); err != nil {
			return fmt.Errorf("failed to commit offset, stopping bulk commit - %w", err)
		}
	}

	return nil
}

func (c *Consumer) commitAsync(metadata *BundleMetadata) error {
	// skip request if already committed this data
	if partitionsMap, found := c.commitsMap[*metadata.Topic]; found {
		if committedOffset, found := partitionsMap[metadata.Partition]; found {
			if committedOffset >= metadata.Offset {
				return nil
			}
		}
	}

	topicPartition := kafka.TopicPartition{
		Topic:     metadata.Topic,
		Partition: metadata.Partition,
		// offset + 1 because when kafka commits offset X, on next load it starts from offset X, not after it.
		Offset: metadata.Offset + 1,
	}

	if _, err := c.kafkaConsumer.Consumer().CommitOffsets([]kafka.TopicPartition{topicPartition}); err != nil {
		return fmt.Errorf("%w", err)
	}

	// log success and update commitsMap
	c.log.Info("committed offset", "topic", topicPartition.Topic, "partition", topicPartition.Partition, "offset",
		topicPartition.Offset)
	c.updateOffsetsMap(c.commitsMap, topicPartition.Topic, topicPartition.Partition, topicPartition.Offset)

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
	compressionType := compressor.NoOp

	compressionTypeHeader, found := c.lookupHeader(msg, kafkaHeaderTypes.HeaderCompressionType)
	if found {
		compressionType = compressor.CompressionType(compressionTypeHeader.Value)
	}

	decompressedPayload, err := c.decompressPayload(msg.Value, compressionType)
	if err != nil {
		c.log.Error(err, "failed to decompress bundle bytes", "message id", string(msg.Key),
			"topic", msg.TopicPartition.Topic, "partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset)
		return
	}

	transportMsg := &transport.Message{}
	if err := json.Unmarshal(decompressedPayload, transportMsg); err != nil {
		c.log.Error(err, "failed to parse transport message", "message key", string(msg.Key),
			"topic", msg.TopicPartition.Topic, "partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset)

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

	c.statistics.IncrementNumberOfReceivedBundles(receivedBundle)

	c.conflationManager.Insert(receivedBundle, &BundleMetadata{
		Topic:     msg.TopicPartition.Topic,
		Partition: msg.TopicPartition.Partition,
		Offset:    msg.TopicPartition.Offset,
		Processed: false,
	})
}

func (c *Consumer) updateOffsetsMap(offsetsMap map[string]map[int32]kafka.Offset, topic *string, partition int32,
	offset kafka.Offset) {
	// check if topic is in the map
	if partitionsMap, found := offsetsMap[*topic]; found {
		// check if partition is in topic map
		if offsetInMap, found := partitionsMap[partition]; !found || (found && offsetInMap < offset) {
			// update partition's offset if partition hasn't an offset yet or the new offset is higher.
			partitionsMap[partition] = offset
		}
	} else {
		// create topic and insert pair
		offsetsMap[*topic] = map[int32]kafka.Offset{partition: offset}
	}
}

func (c *Consumer) lookupHeader(msg *kafka.Message, headerKey string) (*kafka.Header, bool) {
	for _, header := range msg.Headers {
		if header.Key == headerKey {
			return &header, true
		}
	}

	return nil, false
}

func (c *Consumer) decompressPayload(payload []byte, msgCompressorType compressor.CompressionType) ([]byte, error) {
	msgCompressor, found := c.compressorsMap[msgCompressorType]
	if !found {
		newCompressor, err := compressor.NewCompressor(msgCompressorType)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}

		msgCompressor = newCompressor
		c.compressorsMap[msgCompressorType] = msgCompressor
	}

	uncompressedBytes, err := msgCompressor.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	return uncompressedBytes, nil
}
