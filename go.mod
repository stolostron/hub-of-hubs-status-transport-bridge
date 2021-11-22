module github.com/open-cluster-management/hub-of-hubs-status-transport-bridge

go 1.16

require (
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/deckarep/golang-set v1.7.1
	github.com/go-logr/logr v0.2.1
	github.com/jackc/pgx/v4 v4.11.0
	github.com/open-cluster-management/api v0.0.0-20210527013639-a6845f2ebcb1
	github.com/open-cluster-management/hub-of-hubs-data-types v0.1.1-0.20211107173706-45961ba2d1fa
	github.com/open-cluster-management/hub-of-hubs-data-types/apis/config v0.1.0
	github.com/open-cluster-management/hub-of-hubs-kafka-transport v0.0.0-20211111134632-7afa0e46878e
	github.com/open-cluster-management/hub-of-hubs-message-compression v0.0.0-20211109173922-665ce93f86b7
	github.com/open-horizon/edge-sync-service-client v0.0.0-20190711093406-dc3a19905da2
	github.com/open-horizon/edge-utilities v0.0.0-20190711093331-0908b45a7152 // indirect
	github.com/operator-framework/operator-sdk v0.19.4
	github.com/spf13/pflag v1.0.5
	k8s.io/apimachinery v0.20.5
	k8s.io/client-go v12.0.0+incompatible
	sigs.k8s.io/controller-runtime v0.6.2
)

replace (
	github.com/open-cluster-management/hub-of-hubs-message-compression => ../hub-of-hubs-message-compression
	k8s.io/client-go => k8s.io/client-go v0.20.5
)
