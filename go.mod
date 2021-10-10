module github.com/open-cluster-management/hub-of-hubs-status-transport-bridge

go 1.16

require (
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/go-logr/logr v0.2.1
	github.com/go-logr/zapr v0.2.0 // indirect
	github.com/jackc/pgx/v4 v4.11.0
	github.com/open-cluster-management/api v0.0.0-20210527013639-a6845f2ebcb1
	github.com/open-cluster-management/governance-policy-propagator v0.0.0-20210520203318-a78632de1e26
	github.com/open-cluster-management/hub-of-hubs-data-types v0.1.1-0.20210926123616-36d90502da94
	github.com/open-cluster-management/hub-of-hubs-data-types/apis/config v0.1.0
	github.com/open-cluster-management/hub-of-hubs-kafka-transport v0.0.0-20211005065437-ad2cc212eb1d
	github.com/open-horizon/edge-sync-service-client v0.0.0-20190711093406-dc3a19905da2
	github.com/open-horizon/edge-utilities v0.0.0-20190711093331-0908b45a7152 // indirect
	github.com/operator-framework/operator-sdk v0.19.4
	github.com/spf13/pflag v1.0.5
	golang.org/x/net v0.0.0-20210226172049-e18ecbb05110 // indirect
	k8s.io/apimachinery v0.20.5
	k8s.io/client-go v12.0.0+incompatible
	sigs.k8s.io/controller-runtime v0.6.2
)

replace k8s.io/client-go => k8s.io/client-go v0.20.5
