//go:build mapof_opt_enablepadding

package pb

import (
	"unsafe"
)

// counterStripe represents a striped counter to reduce contention.
type counterStripe struct {
	c uintptr // Counter value, accessed atomically
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		c uintptr
	}{})%CacheLineSize) % CacheLineSize]byte
}
