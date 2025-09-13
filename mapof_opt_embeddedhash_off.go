//go:build !mapof_opt_embeddedhash

package pb

const embeddedHash = false

// EntryOf is an immutable map entry.
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

// flatEntry is a flat map entry.
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

// seqFlatEntry is a flat map entry.
type seqFlatEntry[K comparable, V any] struct {
	key   K
	value V
}

// getHash is a no-op for non-embedded-hash build.
func (e *seqFlatEntry[K, V]) getHash() uintptr {
	return 0
}

// setHash is a no-op for non-embedded-hash build.
func (e *seqFlatEntry[K, V]) setHash(_ uintptr) {
}
