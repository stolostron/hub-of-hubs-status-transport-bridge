package syncservice

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	compressor "github.com/open-cluster-management/hub-of-hubs-message-compression"
	"github.com/open-cluster-management/hub-of-hubs-message-compression/compressors"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/statistics"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"github.com/open-horizon/edge-sync-service-client/client"
)

const (
	envVarSyncServiceProtocol        = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost            = "SYNC_SERVICE_HOST"
	envVarSyncServicePort            = "SYNC_SERVICE_PORT"
	envVarSyncServicePollingInterval = "SYNC_SERVICE_POLLING_INTERVAL"
)

var (
	errEnvVarNotFound        = errors.New("not found environment variable")
	errSyncServiceReadFailed = errors.New("sync service error")
)

// SyncService abstracts Sync Service client.
type SyncService struct {
	log                    logr.Logger
	client                 *client.SyncServiceClient
	compressorsMap         map[string]compressors.Compressor
	pollingInterval        int
	objectsMetaDataChan    chan *client.ObjectMetaData
	stopChan               chan struct{}
	conflationManager      *conflator.ConflationManager
	statistics             *statistics.Statistics
	msgIDToRegistrationMap map[string]*transport.BundleRegistration
	startOnce              sync.Once
	stopOnce               sync.Once
}

// NewSyncService creates a new instance of SyncService.
func NewSyncService(log logr.Logger, conflationManager *conflator.ConflationManager,
	statistics *statistics.Statistics) (*SyncService, error) {
	serverProtocol, host, port, pollingInterval, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sync service - %w", err)
	}

	syncServiceClient := client.NewSyncServiceClient(serverProtocol, host, port)

	syncServiceClient.SetOrgID("myorg")
	syncServiceClient.SetAppKeyAndSecret("user@myorg", "")

	return &SyncService{
		log:                    log,
		client:                 syncServiceClient,
		compressorsMap:         make(map[string]compressors.Compressor),
		pollingInterval:        pollingInterval,
		objectsMetaDataChan:    make(chan *client.ObjectMetaData),
		conflationManager:      conflationManager,
		statistics:             statistics,
		msgIDToRegistrationMap: make(map[string]*transport.BundleRegistration),
		stopChan:               make(chan struct{}, 1),
	}, nil
}

func readEnvVars() (string, string, uint16, int, error) {
	protocol, found := os.LookupEnv(envVarSyncServiceProtocol)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServiceProtocol)
	}

	host, found := os.LookupEnv(envVarSyncServiceHost)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServiceHost)
	}

	portString, found := os.LookupEnv(envVarSyncServicePort)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServicePort)
	}

	port, err := strconv.Atoi(portString)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("the environment var %s is not valid port - %w", envVarSyncServicePort,
			err)
	}

	pollingIntervalString, found := os.LookupEnv(envVarSyncServicePollingInterval)
	if !found {
		return "", "", 0, 0, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarSyncServicePollingInterval)
	}

	pollingInterval, err := strconv.Atoi(pollingIntervalString)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("the environment var %s is not valid port - %w",
			envVarSyncServicePollingInterval, err)
	}

	return protocol, host, uint16(port), pollingInterval, nil
}

// Start function starts sync service.
func (s *SyncService) Start() error {
	s.startOnce.Do(func() {
		go s.handleBundles()
	})

	return nil
}

// Stop function stops sync service.
func (s *SyncService) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopChan)
		close(s.objectsMetaDataChan)
	})
}

// Register function registers a msgID for sync service to know how to create the bundle, and use predicate.
func (s *SyncService) Register(registration *transport.BundleRegistration) {
	s.msgIDToRegistrationMap[registration.MsgID] = registration
}

func (s *SyncService) handleBundles() {
	// register for updates for spec bundles, this include all types of spec bundles each with a different id.
	s.client.StartPollingForUpdates(datatypes.StatusBundle, s.pollingInterval, s.objectsMetaDataChan)

	for {
		select {
		case <-s.stopChan:
			return
		case objectMetaData := <-s.objectsMetaDataChan:
			var buffer bytes.Buffer
			if !s.client.FetchObjectData(objectMetaData, &buffer) {
				s.log.Error(errSyncServiceReadFailed, "failed to read bundle from sync service",
					"ObjectId", objectMetaData.ObjectID)
				continue
			}

			msgID := strings.Split(objectMetaData.ObjectID, ".")[1] // object id is LH_ID.MSG_ID
			if _, found := s.msgIDToRegistrationMap[msgID]; !found {
				s.log.Info("no registration available, not sending bundle", "ObjectId",
					objectMetaData.ObjectID)
				continue // no one registered for this msg id
			}

			if !s.msgIDToRegistrationMap[msgID].Predicate() {
				s.log.Info("Predicate is false, not sending bundle", "ObjectId",
					objectMetaData.ObjectID)
				continue // registration predicate is false, do not send the update in the channel
			}

			msgCompressorType := strings.Split(objectMetaData.Description, ":")[1] // obj desc is compression-type:type

			uncompressedPayload, err := s.decompressPayload(buffer.Bytes(), msgCompressorType)
			if err != nil {
				s.log.Error(err, "failed to decompress bundle bytes", "ObjectId", objectMetaData.ObjectID)
				continue
			}

			receivedBundle := s.msgIDToRegistrationMap[msgID].CreateBundleFunc()
			if err := json.Unmarshal(uncompressedPayload, receivedBundle); err != nil {
				s.log.Error(err, "failed to parse bundle", "ObjectId", objectMetaData.ObjectID)
				continue
			}

			s.statistics.IncrementNumberOfReceivedBundles(receivedBundle)

			s.conflationManager.Insert(receivedBundle, &BundleMetadata{})

			if err := s.client.MarkObjectReceived(objectMetaData); err != nil {
				s.log.Error(err, "failed to report object received to sync service")
			}
		}
	}
}

func (s *SyncService) decompressPayload(payload []byte, msgCompressorType string) ([]byte, error) {
	msgCompressor, found := s.compressorsMap[msgCompressorType]
	if !found {
		newCompressor, err := compressor.NewCompressor(compressor.CompressionType(msgCompressorType))
		if err != nil {
			return nil, fmt.Errorf("failed to create compressor: %w", err)
		}

		msgCompressor = newCompressor
		s.compressorsMap[msgCompressorType] = msgCompressor
	}

	uncompressedBytes, err := msgCompressor.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	return uncompressedBytes, nil
}
