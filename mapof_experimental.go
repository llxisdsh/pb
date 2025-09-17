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
//     operations.
//   - Avoid calling other map methods inside fn to prevent deadlocks.
//   - Do not perform expensive computations or I/O operations inside fn.
//   - ⚠️ WARNING: fn may be called TWICE - ensure your function is idempotent
//     and can handle multiple invocations safely
//   - Side effects in fn should be avoided as they may occur multiple times
func (m *MapOf[K, V]) ProcessEntryOptimistic(
	key K,
	fn func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (V, bool) {
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	// --- Two-phase Fast Path ------------------------------------------------

	idx := table.mask & h1v
	root := table.buckets.At(idx)
	var loadedPre *EntryOf[K, V]
findEntry:
	for b := root; b != nil; b = (*bucketOf)(loadPointerNoMB(&b.next)) {
		meta := loadUint64NoMB(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if e := (*EntryOf[K, V])(loadPointerNoMB(b.At(j))); e != nil {
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
		root.Lock()

		// This is the first check, checking if there is a resize operation in
		// progress before acquiring the bucket lock
		if rs := (*resizeState)(loadPointerNoMB(&m.resize)); rs != nil &&
			loadPointerNoMB(&rs.table) != nil /*skip init*/ &&
			loadPointerNoMB(&rs.newTable) != nil /*skip if newTable is nil */ {
			root.Unlock()
			// Wait for the current resize operation to complete
			m.helpCopyAndWait(rs)
			table = (*mapOfTable)(loadPointerNoMB(&m.table))
			idx = table.mask & h1v
			root = table.buckets.At(idx)
			continue
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := (*mapOfTable)(loadPointerNoMB(&m.table)); table != newTable {
			root.Unlock()
			table = newTable
			idx = table.mask & h1v
			root = table.buckets.At(idx)
			continue
		}

		var (
			oldEntry *EntryOf[K, V]
			oldB     *bucketOf
			oldIdx   int
			oldMeta  uint64
			emptyB   *bucketOf
			emptyIdx int
			lastB    *bucketOf
		)

	findLoop:
		for b := root; b != nil; b = (*bucketOf)(b.next) {
			meta := b.meta
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*EntryOf[K, V])(*b.At(j)); e != nil {
					if embeddedHash {
						if e.getHash() == hash && e.Key == key {
							oldEntry = e
							oldB = b
							oldIdx = j
							oldMeta = meta
							break findLoop
						}
					} else {
						if e.Key == key {
							oldEntry = e
							oldB = b
							oldIdx = j
							oldMeta = meta
							break findLoop
						}
					}
				}
			}
			if emptyB == nil {
				if empty := (^meta) & metaMask; empty != 0 {
					emptyB = b
					emptyIdx = firstMarkedByteIndex(empty)
				}
			}
			lastB = b
		}

		// --- Processing Logic ---
		if oldEntry != loadedPre {
			newEntry, value, status = fn(oldEntry)
		}

		if oldEntry != nil {
			if newEntry == oldEntry {
				// No entry to update or delete
				root.Unlock()
				return value, status
			}
			if newEntry != nil {
				// Update
				if embeddedHash {
					newEntry.setHash(hash)
				}
				newEntry.Key = key
				storePointerNoMB(
					oldB.At(oldIdx),
					unsafe.Pointer(newEntry),
				)
				root.Unlock()
				return value, status
			}
			// Delete
			storePointerNoMB(oldB.At(oldIdx), nil)
			newMeta := setByte(oldMeta, emptySlot, oldIdx)
			if oldB == root {
				root.UnlockWithMeta(newMeta)
			} else {
				storeUint64NoMB(&oldB.meta, newMeta)
				root.Unlock()
			}
			table.AddSize(idx, -1)

			// Check if table shrinking is needed
			if m.shrinkOn && newMeta&(^uint64(opByteMask)) == emptyMeta &&
				loadPointerNoMB(&m.resize) == nil {
				tableLen := table.mask + 1
				if m.minLen < tableLen {
					size := table.SumSize()
					if size < tableLen*entriesPerBucket/shrinkFraction {
						m.tryResize(mapShrinkHint, size, 0)
					}
				}
			}
			return value, status
		}

		if newEntry == nil {
			// No entry to insert or delete
			root.Unlock()
			return value, status
		}

		// Insert
		if embeddedHash {
			newEntry.setHash(hash)
		}
		newEntry.Key = key
		if emptyB != nil {
			// publish pointer first, then meta; readers check meta before
			// pointer so they won't observe a partially-initialized entry,
			// and this reduces the window where meta is visible but pointer is
			// still nil
			storePointerNoMB(emptyB.At(emptyIdx), unsafe.Pointer(newEntry))
			if emptyB == root {
				root.UnlockWithMeta(setByte(emptyB.meta, h2v, emptyIdx))
			} else {
				storeUint64NoMB(
					&emptyB.meta,
					setByte(emptyB.meta, h2v, emptyIdx),
				)
				root.Unlock()
			}
			table.AddSize(idx, 1)
			return value, status
		}

		// No empty slot, create new bucket and insert
		storePointerNoMB(&lastB.next, unsafe.Pointer(&bucketOf{
			meta: setByte(emptyMeta, h2v, 0),
			entries: [entriesPerBucket]unsafe.Pointer{
				unsafe.Pointer(newEntry),
			},
		}))
		root.Unlock()
		table.AddSize(idx, 1)

		// Check if the table needs to grow
		if loadPointerNoMB(&m.resize) == nil {
			tableLen := table.mask + 1
			size := table.SumSize()
			const sizeHintFactor = float64(entriesPerBucket) * loadFactor
			if size >= int(float64(tableLen)*sizeHintFactor) {
				m.tryResize(mapGrowHint, size, 0)
			}
		}

		return value, status
	}
}
