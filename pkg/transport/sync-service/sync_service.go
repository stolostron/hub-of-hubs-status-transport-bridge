package syncservice

import (
	"bytes"
	"context"
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
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/statistics"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"github.com/open-horizon/edge-sync-service-client/client"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	envVarSyncServiceProtocol        = "SYNC_SERVICE_PROTOCOL"
	envVarSyncServiceHost            = "SYNC_SERVICE_HOST"
	envVarSyncServicePort            = "SYNC_SERVICE_PORT"
	envVarSyncServicePollingInterval = "SYNC_SERVICE_POLLING_INTERVAL"
	msgIDHeaderTokensLength          = 2
	compressionHeaderTokensLength    = 2
	defaultCompressionType           = compressor.NoOp
)

var (
	errEnvVarNotFound         = errors.New("environment variable not found")
	errSyncServiceReadFailed  = errors.New("sync service error")
	errMessageIDWrongFormat   = errors.New("message ID format is bad")
	errMissingCompressionType = errors.New("compression type is missing from message description")
)

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

	// create committer
	committer, err := NewCommitter(ctrl.Log.WithName("sync-service transport committer"), syncServiceClient,
		conflationManager)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sync service - %w", err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &SyncService{
		log:                    log,
		client:                 syncServiceClient,
		compressorsMap:         make(map[compressor.CompressionType]compressors.Compressor),
		conflationManager:      conflationManager,
		statistics:             statistics,
		committer:              committer,
		pollingInterval:        pollingInterval,
		objectsMetaDataChan:    make(chan *client.ObjectMetaData),
		msgIDToRegistrationMap: make(map[string]*transport.BundleRegistration),
		ctx:                    ctx,
		cancelFunc:             cancelFunc,
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

// SyncService abstracts Sync Service client.
type SyncService struct {
	log               logr.Logger
	client            *client.SyncServiceClient
	compressorsMap    map[compressor.CompressionType]compressors.Compressor
	conflationManager *conflator.ConflationManager
	statistics        *statistics.Statistics

	committer *Committer

	pollingInterval        int
	objectsMetaDataChan    chan *client.ObjectMetaData
	msgIDToRegistrationMap map[string]*transport.BundleRegistration

	ctx        context.Context
	cancelFunc context.CancelFunc
	startOnce  sync.Once
	stopOnce   sync.Once
}

// Start function starts sync service.
func (s *SyncService) Start() {
	s.startOnce.Do(func() {
		go s.committer.Start(s.ctx)
		go s.handleBundles(s.ctx)
	})
}

// Stop function stops sync service.
func (s *SyncService) Stop() {
	s.stopOnce.Do(func() {
		s.cancelFunc()
		close(s.objectsMetaDataChan)
	})
}

// Register function registers a msgID for sync service to know how to create the bundle, and use predicate.
func (s *SyncService) Register(registration *transport.BundleRegistration) {
	s.msgIDToRegistrationMap[registration.MsgID] = registration
}

func (s *SyncService) handleBundles(ctx context.Context) {
	// register for updates for spec bundles, this includes all types of spec bundles each with a different id.
	s.client.StartPollingForUpdates(datatypes.StatusBundle, s.pollingInterval, s.objectsMetaDataChan)

	for {
		select {
		case <-ctx.Done():
			return
		case objectMetaData := <-s.objectsMetaDataChan:
			var buffer bytes.Buffer
			if !s.client.FetchObjectData(objectMetaData, &buffer) {
				s.logError(errSyncServiceReadFailed, "failed to read bundle from sync service", objectMetaData)
				continue
			}

			// get msgID
			msgIDTokens := strings.Split(objectMetaData.ObjectID, ".") // object id is LH_ID.MSG_ID
			if len(msgIDTokens) != msgIDHeaderTokensLength {
				s.logError(errMessageIDWrongFormat, "expecting ObjectID of format LH_ID.MSG_ID", objectMetaData)
				continue
			}

			msgID := msgIDTokens[1]
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

			receivedBundle := s.msgIDToRegistrationMap[msgID].CreateBundleFunc()
			if err := s.unmarshalPayload(receivedBundle, objectMetaData, buffer.Bytes()); err != nil {
				s.logError(err, "failed to get object payload", objectMetaData)
				continue
			}

			s.statistics.IncrementNumberOfReceivedBundles(receivedBundle)

			s.conflationManager.Insert(receivedBundle, &BundleMetadata{
				BaseBundleMetadata: transport.BaseBundleMetadata{
					Processed: false,
				},
				objectMetadata: objectMetaData,
			})

			if err := s.client.MarkObjectReceived(objectMetaData); err != nil {
				s.logError(err, "failed to report object received to sync service", objectMetaData)
			}
		}
	}
}

func (s *SyncService) logError(err error, errMsg string, objectMetaData *client.ObjectMetaData) {
	s.log.Error(err, errMsg, "ObjectID", objectMetaData.ObjectID, "ObjectType", objectMetaData.ObjectType,
		"ObjectDescription", objectMetaData.Description, "Version", objectMetaData.Version)
}

func (s *SyncService) unmarshalPayload(bundleShell bundle.Bundle, objectMetaData *client.ObjectMetaData,
	payload []byte) error {
	compressionType := defaultCompressionType

	if objectMetaData.Description != "" {
		compressionTokens := strings.Split(objectMetaData.Description, ":") // obj desc is Content-Encoding:type
		if len(compressionTokens) != compressionHeaderTokensLength {
			return fmt.Errorf("invalid compression header (Description) - %w", errMissingCompressionType)
		}

		compressionType = compressor.CompressionType(compressionTokens[1])
	}

	decompressedPayload, err := s.decompressPayload(payload, compressionType)
	if err != nil {
		return fmt.Errorf("failed to decompress bundle bytes - %w", err)
	}

	if err := json.Unmarshal(decompressedPayload, bundleShell); err != nil {
		return fmt.Errorf("failed to parse bundle - %w", err)
	}

	return nil
}

func (s *SyncService) decompressPayload(payload []byte, msgCompressorType compressor.CompressionType) ([]byte, error) {
	msgCompressor, found := s.compressorsMap[msgCompressorType]
	if !found {
		newCompressor, err := compressor.NewCompressor(msgCompressorType)
		if err != nil {
			return nil, fmt.Errorf("failed to create compressor: %w", err)
		}

		msgCompressor = newCompressor
		s.compressorsMap[msgCompressorType] = msgCompressor
	}

	decompressedBytes, err := msgCompressor.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress message: %w", err)
	}

	return decompressedBytes, nil
}
