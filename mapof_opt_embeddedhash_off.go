//go:build !mapof_opt_embeddedhash

package pb

const embeddedHash = false

// EntryOf is an immutable map entry.
type EntryOf[K comparable, V any] struct {
	Key   K
	Value V
}

func (e *EntryOf[K, V]) getHash() uintptr {
	return 0
}

func (e *EntryOf[K, V]) setHash(h uintptr) {
}
