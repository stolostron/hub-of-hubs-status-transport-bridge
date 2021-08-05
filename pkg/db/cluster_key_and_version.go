package db

// ClusterKeyAndVersion holds cluster name (unique, serves as key) and resource version.
type ClusterKeyAndVersion struct {
	ClusterName     string
	ResourceVersion string
}
