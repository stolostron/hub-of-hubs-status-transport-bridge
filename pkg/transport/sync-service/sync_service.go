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

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
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
	pollingInterval        int
	objectsMetaDataChan    chan *client.ObjectMetaData
	msgIDToChanMap         map[string]chan bundle.Bundle
	msgIDToRegistrationMap map[string]*transport.BundleRegistration
}

// NewSyncService creates a new instance of SyncService.
func NewSyncService(log logr.Logger) (*SyncService, error) {
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
		pollingInterval:        pollingInterval,
		objectsMetaDataChan:    make(chan *client.ObjectMetaData),
		msgIDToChanMap:         make(map[string]chan bundle.Bundle),
		msgIDToRegistrationMap: make(map[string]*transport.BundleRegistration),
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
func (s *SyncService) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go s.handleBundles(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel.

		cancelContext()

		close(s.objectsMetaDataChan)

		s.log.Info("stopped sync service")

		return nil
	}
}

// Register function registers a msgID to the bundle updates channel.
func (s *SyncService) Register(registration *transport.BundleRegistration, bundleUpdatesChan chan bundle.Bundle) {
	s.msgIDToChanMap[registration.MsgID] = bundleUpdatesChan
	s.msgIDToRegistrationMap[registration.MsgID] = registration
}

func (s *SyncService) handleBundles(ctx context.Context) {
	// register for updates for spec bundles, this include all types of spec bundles each with a different id.
	s.client.StartPollingForUpdates(datatypes.StatusBundle, s.pollingInterval, s.objectsMetaDataChan)

	for {
		select {
		case <-ctx.Done():
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

			receivedBundle := s.msgIDToRegistrationMap[msgID].CreateBundleFunc()
			if err := json.Unmarshal(buffer.Bytes(), receivedBundle); err != nil {
				s.log.Error(err, "failed to parse bundle", "ObjectId", objectMetaData.ObjectID)
				continue
			}

			s.msgIDToChanMap[msgID] <- receivedBundle
			if err := s.client.MarkObjectReceived(objectMetaData); err != nil {
				s.log.Error(err, "failed to report object received to sync service")
			}
		}
	}
}
