//go:build mapof_opt_embeddedhash

package pb

const embeddedHash = true

// EntryOf is an immutable key-value entry type for [MapOf]
type EntryOf[K comparable, V any] struct {
	hash  uintptr
	Key   K
	Value V
}

//go:nosplit
func (e *EntryOf[K, V]) getHash() uintptr {
	return e.hash
}

//go:nosplit
func (e *EntryOf[K, V]) setHash(h uintptr) {
	e.hash = h
}
