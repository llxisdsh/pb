//go:build !mapof_opt_embeddedhash

package pb

const embeddedHash_ = false

type embeddedHash struct{}

//go:nosplit
func (e *embeddedHash) getHash() uintptr {
	return 0
}

//go:nosplit
func (e *embeddedHash) setHash(_ uintptr) {
}
