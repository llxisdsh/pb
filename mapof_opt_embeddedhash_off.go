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
func (e *EntryOf[K, V]) setHash(h uintptr) {
}

// flatEntry is a flat map entry.
type flatEntry[K comparable, V comparable] struct {
	value atomicValue[V]
	key   K
}

//go:nosplit
func (e *flatEntry[K, V]) getHash() uintptr {
	return 0
}

//go:nosplit
func (e *flatEntry[K, V]) setHash(h uintptr) {
}
