//go:build !mapof_enablepadding

package pb

// enablePadding is true, `counterStripeWithPadding` will replace `counterStripe`
// as the structure used for counting size.
// This can mitigate the impact of false sharing on certain machine architectures.
// This constant may also be used for other fields in the future.
// If turned on, the related fields will occupy a bit more memory.
// By default, it is turned off.
const enablePadding = false
