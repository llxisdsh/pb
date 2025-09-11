package pb

import "unsafe"

// WithBuiltInHasher returns a MapConfig option that explicitly sets the
// built-in hash function for the specified type.
//
// This option is useful when you want to explicitly use Go's built-in hasher
// instead of any optimized variants. It ensures that the map uses the same
// hashing strategy as Go's native map implementation.
//
// Performance characteristics:
// - Provides consistent performance across all key sizes
// - Uses Go's optimized internal hash functions
// - Guaranteed compatibility with future Go versions
// - May be slower than specialized hashers for specific use cases
//
// Usage:
//
//	m := NewMapOf[string, int](WithBuiltInHasher[string]())
func WithBuiltInHasher[T comparable]() func(*MapConfig) {
	return func(c *MapConfig) {
		c.KeyHash = GetBuiltInHasher[T]()
	}
}

// GetBuiltInHasher returns Go's built-in hash function for the specified type.
// This function provides direct access to the same hash function that Go's
// built-in map uses internally, ensuring optimal performance and compatibility.
//
// The returned hash function is type-specific and optimized for the given
// comparable type T. It uses Go's internal type representation to access
// the most efficient hashing implementation available.
//
// Usage:
//
//	hashFunc := GetBuiltInHasher[string]()
//	m := NewMapOf[string, int](WithKeyHasherUnsafe(GetBuiltInHasher[string]()))
func GetBuiltInHasher[T comparable]() HashFunc {
	keyHash, _ := defaultHasherUsingBuiltIn[T, struct{}]()
	return keyHash
}

// ProcessEntryOptimistic processes a key-value pair using the provided
// function.
//
// This method is the foundation for all modification operations in MapOf.
// It provides complete control over key-value pairs, allowing atomic reading,
// modification, deletion, or insertion of entries.
//
// EXPERIMENTAL: This is an experimental two-phase implementation that differs
// from the standard ProcessEntry method. It may be subject to changes or
// removal in future versions. Use with caution in production code.
//
// Differences from ProcessEntry:
//   - Two-phase execution: First performs a lock-free read to get the current
//     entry state, then acquires locks only when modifications are needed
//   - May result in fn being called TWICE: once during the initial lock-free
//     phase and again during the locked modification phase if the entry state
//     changes between phases
//   - Potentially better performance for read-heavy workloads due to reduced
//     lock contention, but may have overhead for write-heavy scenarios
//   - The fn function must be idempotent and handle being called multiple times
//     with potentially different loaded values
//
// Parameters:
//
//   - key: The key to process
//
//   - fn: Called regardless of value existence; parameters and return values
//     are described below:
//     loaded *EntryOf[K, V]: current entry (nil if key doesn't exist),
//     return *EntryOf[K, V]:  nil to delete, new entry to store; ==loaded
//     for no modification,
//     return V: value to be returned as ProcessEntry's value;
//     return bool: status to be returned as ProcessEntry's status indicator
//
// Returns:
//   - value V: First return value from fn
//   - status bool: Second return value from fn
//
// Notes:
//   - The input parameter loaded is immutable and should not be modified
//     directly.
//   - This method internally ensures goroutine safety and consistency
//   - If you need to modify a value, return a new EntryOf instance
//   - The fn function is executed while holding an internal lock during the
//     second phase. Keep the execution time short to avoid blocking other
//
// operations.
//   - Avoid calling other map methods inside fn to prevent deadlocks.
//   - Do not perform expensive computations or I/O operations inside fn.
//   - ⚠️ WARNING: fn may be called TWICE - ensure your function is idempotent
//     and can handle multiple invocations safely
//   - Side effects in fn should be avoided as they may occur multiple times
func (m *MapOf[K, V]) ProcessEntryOptimistic(
	key K,
	fn func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (V, bool) {
	table := (*mapOfTable)(loadPointerNoRB(&m.table))
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	// --- Two-phase Fast Path ------------------------------------------------

	bidx := table.bucketsMask & h1v
	rootb := table.buckets.At(bidx)
	var loadedPre *EntryOf[K, V]
findEntry:
	for b := rootb; b != nil; b = (*bucketOf)(loadPointerNoRB(&b.next)) {
		metaw := loadUint64NoRB(&b.meta)
		for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := (*EntryOf[K, V])(loadPointerNoRB(b.At(idx))); e != nil {
				if embeddedHash {
					if e.getHash() == hash && e.Key == key {
						loadedPre = e
						break findEntry
					}
				} else {
					if e.Key == key {
						loadedPre = e
						break findEntry
					}
				}
			}
		}
	}
	newEntry, value, status := fn(loadedPre)
	if loadedPre != nil {
		if newEntry == loadedPre {
			return value, status
		}
	} else {
		if newEntry == nil {
			return value, status
		}
	}

	for {
		rootb.Lock()

		// This is the first check, checking if there is a resize operation in
		// progress before acquiring the bucket lock
		if rs := (*resizeState)(loadPointerNoRB(&m.resizeState)); rs != nil &&
			loadPointerNoRB(&rs.table) != nil /*skip init*/ &&
			loadPointerNoRB(&rs.newTable) != nil /*skip if newTable is nil */ {
			rootb.Unlock()
			// Wait for the current resize operation to complete
			m.helpCopyAndWait(rs)
			table = (*mapOfTable)(loadPointerNoRB(&m.table))
			bidx = table.bucketsMask & h1v
			rootb = table.buckets.At(bidx)
			continue
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := (*mapOfTable)(loadPointerNoRB(&m.table)); table != newTable {
			rootb.Unlock()
			table = newTable
			bidx = table.bucketsMask & h1v
			rootb = table.buckets.At(bidx)
			continue
		}

		var (
			oldEntry    *EntryOf[K, V]
			oldBucket   *bucketOf
			oldIdx      int
			oldMeta     uint64
			emptyBucket *bucketOf
			emptyIdx    int
			lastBucket  *bucketOf
		)

	findLoop:
		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			metaw := b.meta
			for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(*b.At(idx)); e != nil {
					if embeddedHash {
						if e.getHash() == hash && e.Key == key {
							oldEntry = e
							oldBucket = b
							oldIdx = idx
							oldMeta = metaw
							break findLoop
						}
					} else {
						if e.Key == key {
							oldEntry = e
							oldBucket = b
							oldIdx = idx
							oldMeta = metaw
							break findLoop
						}
					}
				}
			}
			if emptyBucket == nil {
				if emptyw := (^metaw) & metaMask; emptyw != 0 {
					emptyBucket = b
					emptyIdx = firstMarkedByteIndex(emptyw)
				}
			}
			lastBucket = b
		}

		// --- Processing Logic ---
		if oldEntry != loadedPre {
			newEntry, value, status = fn(oldEntry)
		}

		if oldEntry != nil {
			if newEntry == oldEntry {
				// No entry to update or delete
				rootb.Unlock()
				return value, status
			}
			if newEntry != nil {
				// Update
				if embeddedHash {
					newEntry.setHash(hash)
				}
				newEntry.Key = key
				storePointerNoWB(
					oldBucket.At(oldIdx),
					unsafe.Pointer(newEntry),
				)
				rootb.Unlock()
				return value, status
			}
			// Delete
			storePointerNoWB(oldBucket.At(oldIdx), nil)
			newmetaw := setByte(oldMeta, emptyMetaSlot, oldIdx)
			if oldBucket == rootb {
				rootb.UnlockWithMeta(newmetaw)
			} else {
				storeUint64NoWB(&oldBucket.meta, newmetaw)
				rootb.Unlock()
			}
			table.AddSize(bidx, -1)

			// Check if table shrinking is needed
			if m.shrinkEnabled && newmetaw&(^opByteMask) == emptyMeta &&
				loadPointerNoRB(&m.resizeState) == nil {
				tableLen := table.bucketsMask + 1
				if m.minTableLen < tableLen {
					size := table.SumSize()
					if size < tableLen*entriesPerMapOfBucket/mapShrinkFraction {
						m.tryResize(mapShrinkHint, size, 0)
					}
				}
			}
			return value, status
		}

		if newEntry == nil {
			// No entry to insert or delete
			rootb.Unlock()
			return value, status
		}

		// Insert
		if embeddedHash {
			newEntry.setHash(hash)
		}
		newEntry.Key = key
		if emptyBucket != nil {
			// publish pointer first, then meta; readers check meta before
			// pointer so they won't observe a partially-initialized entry,
			// and this reduces the window where meta is visible but pointer is
			// still nil
			storePointerNoWB(emptyBucket.At(emptyIdx), unsafe.Pointer(newEntry))
			if emptyBucket == rootb {
				rootb.UnlockWithMeta(setByte(emptyBucket.meta, h2v, emptyIdx))
			} else {
				storeUint64NoWB(
					&emptyBucket.meta,
					setByte(emptyBucket.meta, h2v, emptyIdx),
				)
				rootb.Unlock()
			}
			table.AddSize(bidx, 1)
			return value, status
		}

		// No empty slot, create new bucket and insert
		storePointerNoWB(&lastBucket.next, unsafe.Pointer(&bucketOf{
			meta: setByte(emptyMeta, h2v, 0),
			entries: [entriesPerMapOfBucket]unsafe.Pointer{
				unsafe.Pointer(newEntry),
			},
		}))
		rootb.Unlock()
		table.AddSize(bidx, 1)

		// Check if the table needs to grow
		if loadPointerNoRB(&m.resizeState) == nil {
			tableLen := table.bucketsMask + 1
			size := table.SumSize()
			const sizeHintFactor = float64(entriesPerMapOfBucket) * mapLoadFactor
			if size >= int(float64(tableLen)*sizeHintFactor) {
				m.tryResize(mapGrowHint, size, 0)
			}
		}

		return value, status
	}
}
