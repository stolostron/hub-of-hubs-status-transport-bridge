package bundle

import (
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

func NewComplianceStatusBundle() *ComplianceStatusBundle {
	return &ComplianceStatusBundle{}
}

type ComplianceStatusBundle struct {
	statusbundle.BaseComplianceStatusBundle
}

func (bundle *ComplianceStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

func (bundle *ComplianceStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}
	return result
}

func (bundle *ComplianceStatusBundle) GetGeneration() uint64 {
	return bundle.Generation
}

func (bundle *ComplianceStatusBundle) GetBaseBundleGeneration() uint64 {
	return bundle.BaseBundleGeneration
}
