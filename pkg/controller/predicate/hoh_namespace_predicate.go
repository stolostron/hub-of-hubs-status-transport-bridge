package predicate

import (
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// HoHNamespacePredicate instance of predicate that filters events only in hoh-system namespace
var HoHNamespacePredicate = predicate.NewPredicateFuncs(func(meta metav1.Object, object runtime.Object) bool {
	return meta.GetNamespace() == datatypes.HohSystemNamespace
})
