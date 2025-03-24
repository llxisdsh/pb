//go:build !mapof_atomicreads

package pb

// atomicReads enables atomic reads to provide faster consistency guarantees.
// All entry modifications are done under a lock, making non-atomic reads safe.
// Enabling this option will reduce read performance but offers faster memory consistency guarantees.
// However, regardless of the approach, reads can only satisfy eventual consistency.
const atomicReads = false
