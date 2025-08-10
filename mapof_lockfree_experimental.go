// EXPERIMENTAL: Lock-free processEntry implementation
// WARNING: This is a research prototype and NOT production-ready
// Issues: ABA problem, memory ordering, epoch management complexity

package pb

import (
	"sync/atomic"
	"unsafe"
)

// EpochManager manages memory reclamation for lock-free data structures
type EpochManager struct {
	globalEpoch atomic.Uint64
	localEpochs []atomic.Uint64 // Per-goroutine epochs
}

// EntryVersion wraps EntryOf with version for ABA prevention
type EntryVersion[K comparable, V any] struct {
	entry   *EntryOf[K, V]
	version uint64
}

// VersionedBucketOf adds versioning to bucket operations
type VersionedBucketOf struct {
	bucketOf
	version atomic.Uint64
}

// Lock-free processEntry (EXPERIMENTAL - DO NOT USE IN PRODUCTION)
func (m *MapOf[K, V]) processEntryLockFree(
	table *mapOfTable,
	hash uintptr,
	key *K,
	fn func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (V, bool) {
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	// Enter critical section (epoch-based)
	// TODO: Implement proper epoch management
	
	for {
		bidx := uintptr(len(table.buckets)-1) & h1v
		rootb := &table.buckets[bidx]

		// Check for concurrent resize (lock-free)
		if rs := m.resizeState.Load(); rs != nil {
			if rs.table.Load() != nil && rs.newTable.Load() != nil {
				m.helpCopyAndWait(rs)
				table = m.table.Load()
				continue
			}
		}

		// Snapshot current table
		currentTable := m.table.Load()
		if table != currentTable {
			table = currentTable
			continue
		}

		// Phase 1: Lock-free search with hazard pointers
		var (
			oldEntry    *EntryOf[K, V]
			oldBucket   *bucketOf
			oldIdx      int
			oldMeta     uint64
			emptyBucket *bucketOf
			emptyIdx    int
			lastBucket  *bucketOf
		)

		// Traverse bucket chain lock-free
	searchLoop:
		for b := rootb; b != nil; {
			// Load metadata atomically
			metaw := atomic.LoadUint64(&b.meta)
			
			// Check if bucket is being modified (simplified check)
			if getOp(metaw, opLockMask) {
				// Bucket is locked, retry or help
				continue searchLoop
			}
			
			if metaw != emptyMeta {
				markedw := markZeroBytes(metaw ^ h2w)
				for markedw != 0 {
					idx := firstMarkedByteIndex(markedw)
					
					// Load entry pointer atomically
					entryPtr := atomic.LoadPointer(&b.entries[idx])
					if entryPtr != nil {
						e := (*EntryOf[K, V])(entryPtr)
						
						// Validate entry is still valid (hazard pointer protection needed)
						keyMatch := false
						if embeddedHash {
							keyMatch = e.getHash() == hash && e.Key == *key
						} else {
							keyMatch = e.Key == *key
						}
						
						if keyMatch {
							// Found target entry - record location
							oldEntry = e
							oldBucket = b
							oldIdx = idx
							oldMeta = metaw
							break searchLoop
						}
					}
					markedw &= markedw - 1
				}
			}
			
			// Find empty slot
			if emptyBucket == nil {
				if emptyw := (^metaw) & metaMask; emptyw != 0 {
					emptyBucket = b
					emptyIdx = firstMarkedByteIndex(emptyw)
				}
			}
			
			lastBucket = b
			// Load next bucket atomically
			nextPtr := atomic.LoadPointer(&b.next)
			b = (*bucketOf)(nextPtr)
		}

		// Phase 2: Process entry (user function)
		newEntry, value, status := fn(oldEntry)

		// Phase 3: Apply changes using CAS operations
		if oldEntry != nil {
			if newEntry == oldEntry {
				// No change needed
				return value, status
			}
			
			if newEntry != nil {
				// Update: use CAS to replace entry pointer
				if embeddedHash {
					newEntry.setHash(hash)
				}
				newEntry.Key = *key
				
				// Atomic update
				if atomic.CompareAndSwapPointer(
					&oldBucket.entries[oldIdx],
					unsafe.Pointer(oldEntry),
					unsafe.Pointer(newEntry),
				) {
					return value, status
				}
				// CAS failed - retry entire operation
				continue
			}
			
			// Delete: use CAS for both metadata and entry
			newMetaw := setByte(oldMeta, emptyMetaSlot, oldIdx)
			if atomic.CompareAndSwapUint64(&oldBucket.meta, oldMeta, newMetaw) {
				atomic.StorePointer(&oldBucket.entries[oldIdx], nil)
				table.addSize(bidx, -1)
				
				// Check for shrinking (outside critical path)
				if m.shrinkEnabled && clearOp(newMetaw) == emptyMeta &&
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
			// CAS failed - retry
			continue
		}

		if newEntry == nil {
			// No insertion needed
			return value, status
		}

		// Insert: prepare entry
		if embeddedHash {
			newEntry.setHash(hash)
		}
		newEntry.Key = *key

		if emptyBucket != nil {
			// Try to claim empty slot with CAS
			oldEmptyMeta := atomic.LoadUint64(&emptyBucket.meta)
			newEmptyMeta := setByte(oldEmptyMeta, h2v, emptyIdx)
			
			if atomic.CompareAndSwapUint64(&emptyBucket.meta, oldEmptyMeta, newEmptyMeta) {
				atomic.StorePointer(&emptyBucket.entries[emptyIdx], unsafe.Pointer(newEntry))
				table.addSize(bidx, 1)
				return value, status
			}
			// CAS failed - retry
			continue
		}

		// Allocate new bucket (most complex part for lock-free)
		newBucket := &bucketOf{
			meta: setByte(emptyMeta, h2v, 0),
			entries: [entriesPerMapOfBucket]unsafe.Pointer{
				unsafe.Pointer(newEntry),
			},
		}
		
		// Try to link new bucket atomically
		if atomic.CompareAndSwapPointer(&lastBucket.next, nil, unsafe.Pointer(newBucket)) {
			table.addSize(bidx, 1)
			
			// Check for growth
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
		// CAS failed - retry entire operation
		continue
	}
}

/*
CRITICAL ISSUES WITH THIS LOCK-FREE APPROACH:

1. **ABA Problem**: 
   - Between finding an entry and updating it, another thread might delete and recreate it
   - Need versioning or hazard pointers to detect this

2. **Memory Ordering**:
   - Complex dependencies between meta, entries, and next pointers
   - Need careful memory barriers and atomic ordering

3. **Hazard Pointer Management**:
   - Need to protect entries from being freed while being accessed
   - Requires complex epoch-based or hazard pointer system

4. **Resize Coordination**:
   - Lock-free resize is extremely complex
   - Current resize mechanism relies on global coordination

5. **Performance Trade-offs**:
   - CAS loops can cause cache contention
   - May perform worse than fine-grained locking under contention

RECOMMENDATION: Use the optimized locking version instead
*/