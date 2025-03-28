//go:build !mapof_enablepadding

package pb

// enablePadding is true, the counting structure `counterStripe` will be padded to align with a cache line,
// This can mitigate the impact of false sharing on certain machine architectures.
// This constant may also be used for other fields in the future.
// If turned on, the related fields will occupy a bit more memory.
// By default, it is turned off.
const enablePadding = false

type counterStripe struct {
	c int64
}
