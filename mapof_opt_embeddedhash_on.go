//go:build mapof_opt_embeddedhash

package pb

const embeddedHash = true

// EntryOf is an immutable map entry.
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

// flatEntry is a flat map entry.
type flatEntry[K comparable, V any] struct {
	value atomicValue[V]
	hash  uintptr
	key   K
}

//go:nosplit
func (e *flatEntry[K, V]) getHash() uintptr {
	return e.hash
}

//go:nosplit
func (e *flatEntry[K, V]) setHash(h uintptr) {
	e.hash = h
}

// seqFlatEntry is a flat map entry.
type seqFlatEntry[K comparable, V any] struct {
	hash  uintptr
	key   K
	value V
}

func (e *seqFlatEntry[K, V]) getHash() uintptr {
	return e.hash
}

func (e *seqFlatEntry[K, V]) setHash(h uintptr) {
	e.hash = h
}
