//go:build !mapof_opt_enablepadding

package pb

// counterStripe represents a striped counter to reduce contention.
type counterStripe struct {
	c uintptr // Counter value, accessed atomically
}
