//go:build !mapof_opt_embeddedhash

package pb

const embeddedHash = false

// EntryOf is an immutable key-value entry type for [MapOf]
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
