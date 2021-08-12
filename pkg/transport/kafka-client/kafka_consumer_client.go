package kafkaclient

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// NewHOHConsumer creates a new instance of HOHConsumer.
func NewHOHConsumer(log logr.Logger) (*HOHConsumer, error) {
	kc := &HOHConsumer{
		kafkaConsumer:          nil,
		commitsChan:            make(chan interface{}),
		msgChan:                make(chan *kafka.Message),
		msgIDToChanMap:         make(map[string]chan bundle.Bundle),
		msgIDToRegistrationMap: make(map[string]*transport.BundleRegistration),
		stopChan:               make(chan struct{}, 1),
		log:                    log,

		availableTracker:   1,
		committedTracker:   0,
		trackerToMsgMap:    make(map[uint32]*kafka.Message),
		bundleToTrackerMap: make(map[interface{}]uint32),
	}

	kafkaConsumer, err := kclient.NewKafkaConsumer(kc.msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	kc.kafkaConsumer = kafkaConsumer

	return kc, nil
}

// HOHConsumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type HOHConsumer struct {
	log                    logr.Logger
	kafkaConsumer          *kclient.KafkaConsumer
	commitsChan            chan interface{}
	msgChan                chan *kafka.Message
	msgIDToChanMap         map[string]chan bundle.Bundle
	msgIDToRegistrationMap map[string]*transport.BundleRegistration
	stopChan               chan struct{}
	startOnce              sync.Once
	stopOnce               sync.Once

	availableTracker   uint32
	committedTracker   uint32
	trackerToMsgMap    map[uint32]*kafka.Message
	bundleToTrackerMap map[interface{}]uint32
}

// Start function starts HOHConsumer.
func (c *HOHConsumer) Start() {
	c.startOnce.Do(func() {
		err := c.kafkaConsumer.Subscribe(c.log)
		if err != nil {
			c.log.Error(err, "failed to start kafka consumer: subscribe failed")
			return
		}

		go func() {
			for {
				select {
				case tracker := <-c.commitsChan:
					c.commitMessage(tracker)
				case msg := <-c.msgChan:
					c.processMessage(msg)
				case <-c.stopChan:
					return
				}
			}
		}()
	})
}

// Stop function stops HOHConsumer.
func (c *HOHConsumer) Stop() {
	c.stopOnce.Do(func() {
		c.kafkaConsumer.Close()
		c.stopChan <- struct{}{}
		close(c.msgChan)
		close(c.stopChan)
		close(c.commitsChan)

		for k := range c.trackerToMsgMap {
			delete(c.trackerToMsgMap, k)
		}
	})
}

// CommitAsync commits a transported message that was processed locally.
func (c *HOHConsumer) CommitAsync(bundle interface{}) {
	c.commitsChan <- bundle
}

// Register function registers a msgID to the bundle updates channel.
func (c *HOHConsumer) Register(registration *transport.BundleRegistration, bundleUpdatesChan chan bundle.Bundle) {
	c.msgIDToChanMap[registration.MsgID] = bundleUpdatesChan
	c.msgIDToRegistrationMap[registration.MsgID] = registration
}

// generateMessageTracker assigns a tracker to a message in order to commit it when needed.
func (c *HOHConsumer) generateMessageTracker(msg *kafka.Message) uint32 {
	/*
		TODO: consider optimizing Tracker assignment and moving to uint16.
			(commitMessage currently depends on incremental Tracker assignment)
	*/
	c.trackerToMsgMap[c.availableTracker] = msg
	c.availableTracker++

	return c.availableTracker - 1
}

func (c *HOHConsumer) commitMessage(bundle interface{}) {
	tracker := c.bundleToTrackerMap[bundle]

	if tracker > c.committedTracker {
		msg, exists := c.trackerToMsgMap[tracker]
		if !exists {
			return
		}

		_, err := c.kafkaConsumer.Consumer().CommitMessage(msg)
		if err != nil {
			// Schedule for retry.
			// If a more recent msg gets committed before retry,
			// this message would be dropped.
			c.commitsChan <- tracker
			return
		}

		delete(c.bundleToTrackerMap, bundle)
		delete(c.trackerToMsgMap, tracker)
		c.committedTracker = tracker
	} else {
		_, exists := c.trackerToMsgMap[tracker]
		if exists {
			// a more recent message was committed, drop current
			delete(c.bundleToTrackerMap, bundle)
			delete(c.trackerToMsgMap, tracker)
		}
	}
}

func (c *HOHConsumer) processMessage(msg *kafka.Message) {
	// TODO: replace msg.Key with headers
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
			msg.Key)
		// no one registered for this msg id
		return
	}

	receivedBundle := c.msgIDToRegistrationMap[msgID].CreateBundleFunc()
	if err := json.Unmarshal(transportMsg.Payload, receivedBundle); err != nil {
		c.log.Error(err, "failed to parse bundle", "ObjectId", msg.Key)
		return
	}

	// map bundle internally
	c.bundleToTrackerMap[receivedBundle] = c.generateMessageTracker(msg)
	c.msgIDToChanMap[msgID] <- receivedBundle
}
