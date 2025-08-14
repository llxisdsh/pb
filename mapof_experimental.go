package pb

import "unsafe"

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
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	// --- Two-phase Fast Path ------------------------------------------------

	bidx := uintptr(len(table.buckets)-1) & h1v
	rootb := at(table.buckets, bidx)
	var loadedPre *EntryOf[K, V]
findEntry:
	for b := rootb; b != nil; b = (*bucketOf)(loadPointer(&b.next)) {
		metaw := loadUint64(&b.meta)
		for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := loadEntryOf[K, V](b, idx); e != nil {
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
		rootb.lock()

		// This is the first check, checking if there is a resize operation in
		// progress before acquiring the bucket lock
		if rs := m.resizeState.Load(); rs != nil &&
			rs.table.Load() != nil /*skip init*/ &&
			rs.newTable.Load() != nil /*skip if newTable is nil */ {
			rootb.unlock()
			// Wait for the current resize operation to complete
			m.helpCopyAndWait(rs)
			table = m.table.Load()
			bidx = uintptr(len(table.buckets)-1) & h1v
			rootb = at(table.buckets, bidx)
			continue
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := m.table.Load(); table != newTable {
			rootb.unlock()
			table = newTable
			bidx = uintptr(len(table.buckets)-1) & h1v
			rootb = at(table.buckets, bidx)
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
				if e := getEntryOf[K, V](b, idx); e != nil {
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
				rootb.unlock()
				return value, status
			}
			if newEntry != nil {
				// Update
				if embeddedHash {
					newEntry.setHash(hash)
				}
				newEntry.Key = key
				storeEntryOf[K, V](oldBucket, oldIdx, newEntry)
				rootb.unlock()
				return value, status
			}
			// Delete
			newmetaw := setByte(oldMeta, emptyMetaSlot, oldIdx)
			storeUint64(&oldBucket.meta, newmetaw)
			storeEntryOf[K, V](oldBucket, oldIdx, nil)
			rootb.unlock()
			table.addSize(bidx, -1)

			// Check if table shrinking is needed
			if m.shrinkEnabled && clearOp(newmetaw) == emptyMeta &&
				m.resizeState.Load() == nil {
				tableLen := len(table.buckets)
				if m.minTableLen < tableLen {
					size := table.sumSize()
					if size < tableLen*entriesPerMapOfBucket/mapShrinkFraction {
						m.tryResize(mapShrinkHint, size, 0)
					}
				}
			}
			return value, status
		}

		if newEntry == nil {
			// No entry to insert or delete
			rootb.unlock()
			return value, status
		}

		// Insert
		if embeddedHash {
			newEntry.setHash(hash)
		}
		newEntry.Key = key
		if emptyBucket != nil {
			storeUint64(
				&emptyBucket.meta,
				setByte(emptyBucket.meta, h2v, emptyIdx),
			)
			storeEntryOf[K, V](emptyBucket, emptyIdx, newEntry)
			rootb.unlock()
			table.addSize(bidx, 1)
			return value, status
		}

		// No empty slot, create new bucket and insert
		storePointer(&lastBucket.next, unsafe.Pointer(&bucketOf{
			meta: setByte(emptyMeta, h2v, 0),
			entries: [entriesPerMapOfBucket]unsafe.Pointer{
				unsafe.Pointer(newEntry),
			},
		}))
		rootb.unlock()
		table.addSize(bidx, 1)

		// Check if the table needs to grow
		if m.resizeState.Load() == nil {
			tableLen := len(table.buckets)
			size := table.sumSize()
			const sizeHintFactor = float64(entriesPerMapOfBucket) * mapLoadFactor
			if size >= int(float64(tableLen)*sizeHintFactor) {
				m.tryResize(mapGrowHint, size, 0)
			}
		}

		return value, status
	}
}
