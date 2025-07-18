//go:build mapof_opt_embeddedhash

package pb

const embeddedHash = true

// EntryOf is an immutable map entry.
type EntryOf[K comparable, V any] struct {
	hash  uintptr
	Key   K
	Value V
}

func (e *EntryOf[K, V]) getHash() uintptr {
	return e.hash
}

func (e *EntryOf[K, V]) setHash(h uintptr) {
	e.hash = h
}
