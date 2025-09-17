//go:build !mapof_opt_embeddedhash

package pb

const embeddedHash = false

// Entry is an immutable key-value entry type for [MapOf]
type EntryOf[K comparable, V any] struct {
	Key   K
	Value V
}

//go:nosplit
func (e *EntryOf[K, V]) getHash() uintptr {
	return 0
}

//go:nosplit
func (e *EntryOf[K, V]) setHash(_ uintptr) {
}

// flatEntry is an entry type for [FlatMapOf]
type flatEntry[K comparable, V any] struct {
	value atomicValue[V]
	key   K
}

//go:nosplit
func (e *flatEntry[K, V]) getHash() uintptr {
	return 0
}

//go:nosplit
func (e *flatEntry[K, V]) setHash(_ uintptr) {
}

// seqFlatEntry is an entry type for [SeqFlatMapOf]
type seqFlatEntry[K comparable, V any] struct {
	key   K
	value V
}

//go:nosplit
func (e *seqFlatEntry[K, V]) getHash() uintptr {
	return 0
}

//go:nosplit
func (e *seqFlatEntry[K, V]) setHash(_ uintptr) {
}
