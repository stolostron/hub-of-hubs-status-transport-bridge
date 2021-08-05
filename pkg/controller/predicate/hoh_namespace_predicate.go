package predicate

import (
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// HoHNamespacePredicateInstance instance of HohNamespacePredicate
var HoHNamespacePredicateInstance = &HohNamespacePredicate{}

// HohNamespacePredicate a struct that implements controller-runtime Predicate interface
type HohNamespacePredicate struct {
	predicate.Funcs
}

// Create is a function to filter k8s Create events.
func (p *HohNamespacePredicate) Create(e event.CreateEvent) bool {
	return e.Meta.GetNamespace() == datatypes.HohSystemNamespace
}

// Delete is a function to filter k8s Delete events.
func (p *HohNamespacePredicate) Delete(e event.DeleteEvent) bool {
	return e.Meta.GetNamespace() == datatypes.HohSystemNamespace
}

// Update is a function to filter k8s Update events.
func (p *HohNamespacePredicate) Update(e event.UpdateEvent) bool {
	return e.MetaOld.GetNamespace() == datatypes.HohSystemNamespace ||
		e.MetaNew.GetNamespace() == datatypes.HohSystemNamespace
}

// Generic is a function to filter k8s Generic events.
func (p *HohNamespacePredicate) Generic(e event.GenericEvent) bool {
	return e.Meta.GetNamespace() == datatypes.HohSystemNamespace
}
