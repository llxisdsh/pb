//go:build mapof_opt_embeddedhash

package pb

const embeddedHash_ = true

type embeddedHash struct {
	Hash uintptr
}

//go:nosplit
func (e *embeddedHash) getHash() uintptr {
	return e.Hash
}

//go:nosplit
func (e *embeddedHash) setHash(h uintptr) {
	e.Hash = h
}
