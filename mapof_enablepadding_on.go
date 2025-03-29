//go:build mapof_enablepadding

package pb

// enablePadding is true, the counting structure `counterStripe` will be padded to align with a cache line,
// This can mitigate the impact of false sharing on certain machine architectures.
// This constant may also be used for other fields in the future.
// If turned on, the related fields will occupy a bit more memory.
// By default, it is turned off.
const enablePadding = true

// counterStripe represents a striped counter to reduce contention.
type counterStripe struct {
	//lint:ignore U1000 prevents false sharing
	pad [(cacheLineSize - unsafe.Sizeof(struct {
		c uintptr
	}{})%cacheLineSize) % cacheLineSize]byte
	c uintptr // Counter value, accessed atomically
}
