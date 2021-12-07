package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaconsumer "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	kafkaHeaderTypes "github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
	compressor "github.com/open-cluster-management/hub-of-hubs-message-compression"
	"github.com/open-cluster-management/hub-of-hubs-message-compression/compressors"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/statistics"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	envVarKafkaConsumerID       = "KAFKA_CONSUMER_ID"
	envVarKafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
	envVarKafkaTopic            = "KAFKA_TOPIC"
	msgIDTokensLength           = 2
	defaultCompressionType      = compressor.NoOp
)

var (
	errEnvVarNotFound       = errors.New("environment variable not found")
	errMessageIDWrongFormat = errors.New("message ID format is bad")
)

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, conflationManager *conflator.ConflationManager,
	statistics *statistics.Statistics) (*Consumer, error) {
	kafkaConfigMap, topic, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	msgChan := make(chan *kafka.Message)

	kafkaConsumer, err := kafkaconsumer.NewKafkaConsumer(kafkaConfigMap, msgChan, log)
	if err != nil {
		close(msgChan)
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if err := kafkaConsumer.Subscribe([]string{topic}); err != nil {
		close(msgChan)
		kafkaConsumer.Close()

		return nil, fmt.Errorf("failed to subscribe to requested topic - %v: %w", topic, err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	consumer := &Consumer{
		log:                    log,
		kafkaConsumer:          kafkaConsumer,
		compressorsMap:         make(map[compressor.CompressionType]compressors.Compressor),
		conflationManager:      conflationManager,
		statistics:             statistics,
		msgChan:                msgChan,
		msgIDToRegistrationMap: make(map[string]*transport.BundleRegistration),
		commitsMap:             make(map[string]map[int32]kafka.Offset),
		ctx:                    ctx,
		cancelFunc:             cancelFunc,
	}

	// create committer
	consumer.committer, err = NewCommitter(ctrl.Log.WithName("kafka transport committer"), consumer.commitPositions)
	if err != nil {
		close(msgChan)
		kafkaConsumer.Close()

		return nil, fmt.Errorf("failed to create committer: %w", err)
	}

	// add conflation manager to committer consumers
	consumer.committer.AddTransportConsumer(conflationManager)

	return consumer, nil
}

func readEnvVars() (*kafka.ConfigMap, string, error) {
	consumerID, found := os.LookupEnv(envVarKafkaConsumerID)
	if !found {
		return nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerID)
	}

	bootstrapServers, found := os.LookupEnv(envVarKafkaBootstrapServers)
	if !found {
		return nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaBootstrapServers)
	}

	topic, found := os.LookupEnv(envVarKafkaTopic)
	if !found {
		return nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaTopic)
	}

	kafkaConfigMap := &kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"client.id":          consumerID,
		"group.id":           consumerID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}

	return kafkaConfigMap, topic, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log               logr.Logger
	kafkaConsumer     *kafkaconsumer.KafkaConsumer
	compressorsMap    map[compressor.CompressionType]compressors.Compressor
	conflationManager *conflator.ConflationManager
	statistics        *statistics.Statistics
	committer         *Committer

	msgChan                chan *kafka.Message
	msgIDToRegistrationMap map[string]*transport.BundleRegistration
	commitsMap             map[string]map[int32]kafka.Offset

	ctx        context.Context
	cancelFunc context.CancelFunc
	startOnce  sync.Once
	stopOnce   sync.Once
}

// Start function starts the consumer.
func (c *Consumer) Start() {
	c.startOnce.Do(func() {
		go c.committer.Start(c.ctx)
		go c.handleKafkaMessages(c.ctx)
	})
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		c.cancelFunc()
		close(c.msgChan)
		c.kafkaConsumer.Close()
	})
}

// Register function registers a msgID to the bundle updates channel.
func (c *Consumer) Register(registration *transport.BundleRegistration) {
	c.msgIDToRegistrationMap[registration.MsgID] = registration
}

// commitPositions commits the given positions (by metadata) per partition mapped.
func (c *Consumer) commitPositions(positions map[int32]*BundleMetadata) error {
	// go over positions and commit
	for _, metadata := range positions {
		// skip request if already committed this data
		if partitionsMap, found := c.commitsMap[*metadata.topic]; found {
			if committedOffset, found := partitionsMap[metadata.partition]; found {
				if committedOffset >= metadata.offset {
					return nil
				}
			}
		}

		topicPartition := kafka.TopicPartition{
			Topic:     metadata.topic,
			Partition: metadata.partition,
			// offset + 1 because when kafka commits offset X, on next load it starts from offset X, not after it.
			Offset: metadata.offset + 1,
		}

		if _, err := c.kafkaConsumer.Consumer().CommitOffsets([]kafka.TopicPartition{topicPartition}); err != nil {
			return fmt.Errorf("failed to commit offset, stopping bulk commit - %w", err)
		}

		// log success and update commitsMap
		c.log.Info("committed offset", "topic", topicPartition.Topic, "partition", topicPartition.Partition, "offset",
			topicPartition.Offset)
		c.updateOffsetsMap(c.commitsMap, topicPartition.Topic, topicPartition.Partition, topicPartition.Offset)
	}

	return nil
}

func (c *Consumer) handleKafkaMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-c.msgChan:
			c.processMessage(msg)
		}
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) {
	compressionType := defaultCompressionType

	if compressionTypeBytes, found := c.lookupHeaderValue(msg, kafkaHeaderTypes.HeaderCompressionType); found {
		compressionType = compressor.CompressionType(compressionTypeBytes)
	}

	decompressedPayload, err := c.decompressPayload(msg.Value, compressionType)
	if err != nil {
		c.logError(err, "failed to decompress bundle bytes", msg)
		return
	}

	transportMsg := &transport.Message{}
	if err := json.Unmarshal(decompressedPayload, transportMsg); err != nil {
		c.logError(err, "failed to parse transport message", msg)
		return
	}

	// get msgID
	msgIDTokens := strings.Split(transportMsg.ID, ".") // object id is LH_ID.MSG_ID
	if len(msgIDTokens) != msgIDTokensLength {
		c.logError(errMessageIDWrongFormat, "expecting MessageID of format LH_ID.MSG_ID", msg)
		return
	}

	msgID := msgIDTokens[1]
	if _, found := c.msgIDToRegistrationMap[msgID]; !found {
		c.log.Info("no bundle-registration available, not sending bundle", "bundleId", transportMsg.ID,
			"bundleType", transportMsg.MsgType, "version", transportMsg.Version)
		// no one registered for this msg id
		return
	}

	if !c.msgIDToRegistrationMap[msgID].Predicate() {
		c.log.Info("predicate is false, not sending bundle", "bundleId", transportMsg.ID,
			"bundleType", transportMsg.MsgType, "version", transportMsg.Version)

		return // bundle-registration predicate is false, do not send the update in the channel
	}

	receivedBundle := c.msgIDToRegistrationMap[msgID].CreateBundleFunc()
	if err := json.Unmarshal(transportMsg.Payload, receivedBundle); err != nil {
		c.logError(err, "failed to parse bundle", msg)
		return
	}

	c.statistics.IncrementNumberOfReceivedBundles(receivedBundle)

	c.conflationManager.Insert(receivedBundle, &BundleMetadata{
		processed: false,
		topic:     msg.TopicPartition.Topic,
		partition: msg.TopicPartition.Partition,
		offset:    msg.TopicPartition.Offset,
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

func (c *Consumer) logError(err error, errMessage string, msg *kafka.Message) {
	c.log.Error(err, errMessage, "MessageKey", string(msg.Key), "TopicPartition", msg.TopicPartition)
}

func (c *Consumer) lookupHeaderValue(msg *kafka.Message, headerKey string) ([]byte, bool) {
	for _, header := range msg.Headers {
		if header.Key == headerKey {
			return header.Value, true
		}
	}

	return nil, false
}

func (c *Consumer) decompressPayload(payload []byte, msgCompressorType compressor.CompressionType) ([]byte, error) {
	msgCompressor, found := c.compressorsMap[msgCompressorType]
	if !found {
		newCompressor, err := compressor.NewCompressor(msgCompressorType)
		if err != nil {
			return nil, fmt.Errorf("failed to create compressor: %w", err)
		}

		msgCompressor = newCompressor
		c.compressorsMap[msgCompressorType] = msgCompressor
	}

	decompressedBytes, err := msgCompressor.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress message: %w", err)
	}

	return decompressedBytes, nil
}
