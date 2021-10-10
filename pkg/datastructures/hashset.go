package datastructures

type (
	void struct{}
	// HashSet implementation of hash set backed by a hash map.
	HashSet map[string]void
)

// NewHashSet returns a new instance of HashSet.
func NewHashSet(capacity ...int) HashSet {
	if len(capacity) == 0 {
		return map[string]void{}
	}

	return make(HashSet, capacity[0])
}

// Add adds an item to the hash set in case it doesn't exist. if it exists add is a no-op.
func (hashset *HashSet) Add(item string) {
	(*hashset)[item] = void{}
}

// Delete removes an item from the hash set. if the item does not exist in the hash set, delete is a no-op.
func (hashset *HashSet) Delete(item string) {
	delete(*hashset, item)
}

// Exists returns true if the item exists in the hash set, otherwise returns false.
func (hashset *HashSet) Exists(item string) bool {
	_, exists := (*hashset)[item]
	return exists
}
