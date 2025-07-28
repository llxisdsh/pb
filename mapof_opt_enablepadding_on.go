//go:build mapof_opt_enablepadding

package pb

import (
	"sync/atomic"
	"unsafe"
)

// enablePadding is true, the counting structure `counterStripe` will be padded
// to align with a cache line,This can mitigate the impact of false sharing on
// certain machine architectures. This constant may also be used for other
// fields in the future. If turned on, the related fields will occupy a bit
// more memory. By default, it is turned off.
const enablePadding = true

// counterStripe represents a striped counter to reduce contention.
type counterStripe struct {
	c atomic.Uintptr // Counter value, accessed atomically
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		c atomic.Uintptr
	}{})%CacheLineSize) % CacheLineSize]byte
}
