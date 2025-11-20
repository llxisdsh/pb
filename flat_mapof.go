package pb

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// FlatMapOf implements a flat hash map using seqlock.
// Table and key/value pairs are stored inline (flat).
// Value size is not limited by the CPU word size.
// Readers use per-bucket seqlock: even sequence means stable; writers
// flip the bucket sequence to odd during mutation, then even again.
//
// Concurrency model:
//   - Readers: read s1=seq (must be even), then meta/entries, then s2=seq;
//     if s1!=s2 or s1 is odd, retry the bucket.
//   - Writers: take the root-bucket lock (opLock in meta), then on the target
//     bucket: seq++, apply changes, seq++, finally release the root lock.
//   - Resize: copy under the root-bucket lock using the same discipline.
type FlatMapOf[K comparable, V any] struct {
	_        noCopy
	table    seqlockSlot[flatTable[K, V]]
	rs       unsafe.Pointer // *flatRebuildState[K,V]
	seed     uintptr
	keyHash  HashFunc
	tableSeq seqlock[uint32, flatTable[K, V]] // seqlock of table
	shrinkOn bool                             // WithShrinkEnabled
	intKey   bool
}

type flatRebuildState[K comparable, V any] struct {
	hint        mapRebuildHint
	chunks      int32
	newTable    seqlockSlot[flatTable[K, V]]
	newTableSeq seqlock[uint32, flatTable[K, V]] // seqlock of new table
	process     int32                            // atomic
	completed   int32                            // atomic
	wg          sync.WaitGroup
}

type flatTable[K comparable, V any] struct {
	buckets  unsafeSlice[flatBucket[K, V]]
	mask     int
	size     unsafeSlice[counterStripe]
	sizeMask uint32
	// The inline size has minimal effect on reducing cache misses,
	// so we will not use it for now.
	// smallSz  uintptr
}

type flatBucket[K comparable, V any] struct {
	_       [0]atomic.Uint64
	meta    uint64                            // op byte + h2 bytes
	seq     seqlock[uintptr, flatEntry[K, V]] // seqlock of bucket
	next    unsafe.Pointer                    // *flatBucket[K,V]
	entries [entriesPerBucket]seqlockSlot[flatEntry[K, V]]
}

// NewFlatMapOf creates a new seqlock-based flat hash map.
//
// Highlights:
//   - Optimistic reads via per-bucket seqlock; brief spinning under
//     contention.
//   - Writes coordinate via a lightweight root-bucket lock and per-bucket
//     seqlock fencing.
//   - Parallel resize (grow/shrink) with cooperative copying by readers and
//     writers.
//
// Configuration options (aligned with MapOf):
//   - WithPresize(sizeHint): pre-allocate capacity to reduce early resizes.
//   - WithShrinkEnabled(): enable automatic shrinking when load drops.
//   - WithKeyHasher / WithKeyHasherUnsafe / WithBuiltInHasher: custom or
//     built-in hashing.
//   - HashOptimization: control h1 distribution strategy (Linear/Shift/Auto).
//
// Example:
//
//	m := NewFlatMapOf[string,int](WithPresize(1024), WithShrinkEnabled())
//	m.Store("a", 1)
//	v, ok := m.Load("a")
func NewFlatMapOf[K comparable, V any](
	options ...func(*MapConfig),
) *FlatMapOf[K, V] {
	var cfg MapConfig
	for _, opt := range options {
		opt(&cfg)
	}
	m := &FlatMapOf[K, V]{}
	m.init(&cfg)
	return m
}

func (m *FlatMapOf[K, V]) init(
	cfg *MapConfig,
) {
	// parse interface
	if cfg.KeyHash == nil {
		cfg.KeyHash, cfg.HashOpts = parseKeyInterface[K]()
	}
	// perform initialization
	m.keyHash, _, m.intKey = defaultHasher[K, V]()
	if cfg.KeyHash != nil {
		m.keyHash = cfg.KeyHash
		cfg.hashOpts(&m.intKey)
	}

	m.seed = uintptr(rand.Uint64())
	m.shrinkOn = cfg.ShrinkEnabled
	var newTable flatTable[K, V]
	tableLen := calcTableLen(cfg.SizeHint)
	newTable.makeTable(tableLen, runtime.GOMAXPROCS(0))
	m.tableSeq.WriteLocked(&m.table, newTable)
}

//go:noinline
func (m *FlatMapOf[K, V]) initSlow() {
	rs := (*flatRebuildState[K, V])(atomic.LoadPointer(&m.rs))
	if rs != nil {
		rs.wg.Wait()
		return
	}
	rs, ok := m.beginRebuild(mapRebuildBlockWritersHint)
	if !ok {
		rs = (*flatRebuildState[K, V])(atomic.LoadPointer(&m.rs))
		if rs != nil {
			rs.wg.Wait()
		}
		return
	}
	// The table may have been altered prior to our changes.
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr != nil {
		m.endRebuild(rs)
		return
	}
	cfg := &MapConfig{}
	m.init(cfg)
	m.endRebuild(rs)
}

// Load retrieves the value for a key.
//
//   - Fast path: per-bucket seqlock read; an even and stable sequence yields
//     a consistent snapshot.
//   - Contention handling: short spinning on observed writes (odd seq) or
//     instability; no locked fallback path.
//   - Provides stable latency under high concurrency.
func (m *FlatMapOf[K, V]) Load(key K) (value V, ok bool) {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1(hash, m.intKey)
	root := table.buckets.At(idx)
	for b := root; b != nil; b = (*flatBucket[K, V])(atomic.LoadPointer(&b.next)) {
		meta := atomic.LoadUint64(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			e := b.seq.Read(b.At(j))
			if embeddedHash {
				if e.getHash() == hash && e.key == key {
					return e.value, true
				}
			} else {
				if e.key == key {
					return e.value, true
				}
			}
		}
	}
	return
}

// Range iterates all entries using per-bucket seqlock reads.
//
//   - Copies a consistent snapshot from each bucket when the sequence is
//     stable; otherwise briefly spins and retries.
//   - Yields outside of locks to minimize contention.
//   - Returning false from the callback stops iteration early.
func (m *FlatMapOf[K, V]) Range(yield func(K, V) bool) {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return
	}

	var meta uint64
	var cache [entriesPerBucket]flatEntry[K, V]
	var cacheCount int
	for i := 0; i <= table.mask; i++ {
		root := table.buckets.At(i)
		for b := root; b != nil; b = (*flatBucket[K, V])(atomic.LoadPointer(&b.next)) {
			var spins int
			for {
				if s1, ok := b.seq.BeginRead(); ok {
					meta = atomic.LoadUint64(&b.meta)
					cacheCount = 0
					for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
						j := firstMarkedByteIndex(marked)
						cache[cacheCount] = b.At(j).ReadUnfenced()
						cacheCount++
					}
					if b.seq.EndRead(s1) {
						for j := range cacheCount {
							kv := &cache[j]
							if !yield(kv.key, kv.value) {
								return
							}
						}
						break
					}
				}
				delay(&spins)
			}
		}
	}
}

// RangeProcess iterates over all key-value pairs and applies a user function.
//
// For each entry, calls:
//
//	fn(key K, value V) (newV V, op ComputeOp)
//
//	op:
//	 - UpdateOp: update the entry value to newV (protected by bucket seqlock)
//	 - DeleteOp: remove the entry (update meta, clear entry, adjust size)
//	 - CancelOp: no change
//
// Concurrency & consistency:
//   - If a resize/rebuild is detected, it cooperates to completion, then
//     iterates the new table while blocking subsequent resize/rebuild.
//   - Holds the root-bucket lock while processing its bucket chain to
//     coordinate with concurrent writers/resize operations.
//   - Uses per-bucket seqlock to minimize the odd (write) window and preserve
//     read consistency.
//
// Parameters:
//   - fn: user function applied to each key-value pair.
//   - policyOpt: optional WriterPolicy (default = AllowWriters).
//     BlockWriters blocks concurrent writers; AllowWriters permits them.
//     Resize (grow/shrink) is always exclusive.
//
// Recommendation: keep fn lightweight to reduce lock hold time.
func (m *FlatMapOf[K, V]) RangeProcess(
	fn func(key K, value V) (newV V, op ComputeOp),
	policyOpt ...WriterPolicy,
) {
	policy := AllowWriters
	if len(policyOpt) != 0 {
		policy = policyOpt[0]
	}

	m.rebuild(policy.hint(), func() {
		table := m.tableSeq.Read(&m.table)
		if table.buckets.ptr == nil {
			return
		}

		for i := 0; i <= table.mask; i++ {
			root := table.buckets.At(i)
			root.Lock()
			for b := root; b != nil; b = (*flatBucket[K, V])(b.next) {
				meta := b.meta
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j)
					newV, op := fn(e.Ptr().key, e.Ptr().value)
					switch op {
					case UpdateOp:
						newE := flatEntry[K, V]{e.Ptr().key, newV}
						if embeddedHash {
							newE.setHash(e.Ptr().getHash())
						}
						b.seq.WriteLocked(e, newE)
					case DeleteOp:
						b.seq.BeginWriteLocked()
						e.WriteUnfenced(flatEntry[K, V]{})
						meta = setByte(meta, emptySlot, j)
						atomic.StoreUint64(&b.meta, meta)
						b.seq.EndWriteLocked()
						table.AddSize(i, -1)
					default:
						// CancelOp: No-op
					}
				}
			}
			root.Unlock()
		}
	})
}

// Process performs a compute-style, atomic update for the given key.
//
// Concurrency model:
//   - Acquires the root-bucket lock to serialize write/resize cooperation.
//   - Performs per-bucket seqlock writes (odd/even sequence) to minimize the
//     write window and preserve reader consistency.
//   - If a resize is observed, cooperates to finish copying and restarts on
//     the latest table.
//
// Callback signature:
//
//	fn(old V, loaded bool) (newV V, op ComputeOp, ret V, status bool)
//
//	- old/loaded: the existing value and whether it was found.
//	- newV/op: the operation to apply to the map:
//	  • UpdateOp: update the existing value; if not found, insert newV.
//	  • DeleteOp: delete the entry only if it exists.
//	  • CancelOp: do not modify the map.
//	- ret/status: values returned to the caller of Process, allowing the
//	  callback to provide computed results (e.g., final value and hit status).
//
// Parameters:
//
//   - key: The key to process
//   - fn: Callback function (called regardless of value existence)
//
// Returns:
//   - (ret, status) as provided by the callback; typical conventions are
//     ret=final value and status=hit/success.
//
// Typical use cases:
//   - Upsert: overwrite if present, insert otherwise.
//   - CAS-like logic: decide UpdateOp/CancelOp based on old.
//   - Conditional delete: choose DeleteOp or CancelOp by business rule.
func (m *FlatMapOf[K, V]) Process(
	key K,
	fn func(old V, loaded bool) (newV V, op ComputeOp, ret V, status bool),
) (V, bool) {
	for {
		table := m.tableSeq.Read(&m.table)
		if table.buckets.ptr == nil {
			m.initSlow()
			continue
		}
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h1v := h1(hash, m.intKey)
		h2v := h2(hash)
		h2w := broadcast(h2v)
		idx := table.mask & h1v
		root := table.buckets.At(idx)
		root.Lock()

		// Help finishing rebuild if needed
		if rs := (*flatRebuildState[K, V])(atomic.LoadPointer(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if rs.newTableSeq.WriteCompleted() {
					root.Unlock()
					m.helpCopyAndWait(rs)
					continue
				}
			case mapRebuildBlockWritersHint:
				root.Unlock()
				rs.wg.Wait()
				continue
			default:
				// mapRebuildWithWritersHint: allow concurrent writers
			}
		}
		if m.tableSeq.Read(&m.table).buckets.ptr != table.buckets.ptr {
			root.Unlock()
			continue
		}

		var (
			oldB     *flatBucket[K, V]
			oldIdx   int
			oldMeta  uint64
			oldVal   V
			loaded   bool
			emptyB   *flatBucket[K, V]
			emptyIdx int
			lastB    *flatBucket[K, V]
		)

	findLoop:
		for b := root; b != nil; b = (*flatBucket[K, V])(b.next) {
			meta := b.meta
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).Ptr()
				if embeddedHash {
					if e.getHash() == hash && e.key == key {
						oldB, oldIdx, oldMeta, oldVal, loaded = b, j, meta, e.value, true
						break findLoop
					}
				} else {
					if e.key == key {
						oldB, oldIdx, oldMeta, oldVal, loaded = b, j, meta, e.value, true
						break findLoop
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

		newV, op, value, status := fn(oldVal, loaded)
		switch op {
		case UpdateOp:
			if loaded {
				e := oldB.At(oldIdx)
				newE := flatEntry[K, V]{e.Ptr().key, newV}
				if embeddedHash {
					newE.setHash(e.Ptr().getHash())
				}
				oldB.seq.WriteLocked(e, newE)
				root.Unlock()
				return value, status
			}
			newE := flatEntry[K, V]{key, newV}
			if embeddedHash {
				newE.setHash(hash)
			}
			// insert new
			if emptyB != nil {
				emptyB.seq.BeginWriteLocked()
				emptyB.At(emptyIdx).WriteUnfenced(newE)
				newMeta := setByte(emptyB.meta, h2v, emptyIdx)
				atomic.StoreUint64(&emptyB.meta, newMeta)
				emptyB.seq.EndWriteLocked()

				root.Unlock()
				table.AddSize(idx, 1)
				return value, status
			}
			// append new bucket
			bucket := &flatBucket[K, V]{
				meta: setByte(emptyMeta, h2v, 0),
				entries: [entriesPerBucket]seqlockSlot[flatEntry[K, V]]{
					{buf: newE},
				},
			}

			atomic.StorePointer(&lastB.next, unsafe.Pointer(bucket))
			root.Unlock()
			table.AddSize(idx, 1)
			// Auto-grow check (parallel resize)
			if atomic.LoadPointer(&m.rs) == nil {
				tableLen := table.mask + 1
				size := table.SumSize()
				const sizeHintFactor = float64(entriesPerBucket) * loadFactor
				if size >= int(float64(tableLen)*sizeHintFactor) {
					m.tryResize(mapGrowHint, size, 0)
				}
			}
			return value, status
		case DeleteOp:
			if !loaded {
				root.Unlock()
				return value, status
			}
			oldB.seq.BeginWriteLocked()
			oldB.At(oldIdx).WriteUnfenced(flatEntry[K, V]{})
			newMeta := setByte(oldMeta, emptySlot, oldIdx)
			atomic.StoreUint64(&oldB.meta, newMeta)
			oldB.seq.EndWriteLocked()
			root.Unlock()
			table.AddSize(idx, -1)
			// Check if table shrinking is needed
			if m.shrinkOn && newMeta&metaDataMask == emptyMeta &&
				atomic.LoadPointer(&m.rs) == nil {
				tableLen := table.mask + 1
				if minTableLen < tableLen {
					size := table.SumSize()
					if size < tableLen*entriesPerBucket/shrinkFraction {
						m.tryResize(mapShrinkHint, size, 0)
					}
				}
			}
			return value, status
		default:
			// CancelOp: No-op
			root.Unlock()
			return value, status
		}
	}
}

// Store sets the value for a key.
func (m *FlatMapOf[K, V]) Store(key K, value V) {
	m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return value, UpdateOp, old, loaded
	})
}

// Swap stores value for key and returns the previous value if any.
// The loaded result reports whether the key was present.
func (m *FlatMapOf[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	return m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return value, UpdateOp, old, loaded
	})
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMapOf[K, V]) LoadOrStore(
	key K,
	value V,
) (actual V, loaded bool) {
	if v, ok := m.Load(key); ok {
		return v, true
	}
	return m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		if loaded {
			return old, CancelOp, old, loaded
		}
		return value, UpdateOp, value, loaded
	})
}

// LoadOrStoreFn loads the value for a key if present.
// Otherwise, it stores and returns the value returned by valueFn.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMapOf[K, V]) LoadOrStoreFn(
	key K,
	valueFn func() V,
) (actual V, loaded bool) {
	if v, ok := m.Load(key); ok {
		return v, true
	}
	return m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		if loaded {
			return old, CancelOp, old, loaded
		}
		value := valueFn()
		return value, UpdateOp, value, loaded
	})
}

// Delete deletes the value for a key.
func (m *FlatMapOf[K, V]) Delete(key K) {
	m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return old, DeleteOp, old, loaded
	})
}

// LoadAndDelete deletes the value for a key, returning the previous value.
// The loaded result reports whether the key was present.
func (m *FlatMapOf[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return old, DeleteOp, old, loaded
	})
}

// LoadAndUpdate updates the value for key if it exists, returning the previous
// value. The loaded result reports whether the key was present.
func (m *FlatMapOf[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	return m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		if loaded {
			return value, UpdateOp, old, loaded
		}
		return old, CancelOp, old, loaded
	})
}

// Clear clears all key-value pairs from the map.
func (m *FlatMapOf[K, V]) Clear() {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return
	}

	m.rebuild(mapRebuildBlockWritersHint, func() {
		var newTable flatTable[K, V]
		cpus := runtime.GOMAXPROCS(0)
		newTable.makeTable(minTableLen, cpus)
		m.tableSeq.WriteLocked(&m.table, newTable)
	})
}

// All returns an iterator function for use with range-over-func.
// It provides the same functionality as Range but in iterator form.
//
//go:nosplit
func (m *FlatMapOf[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the number of key-value pairs in the map.
// This operation sums counters across all size stripes for an approximate
// count.
//
//go:nosplit
func (m *FlatMapOf[K, V]) Size() int {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return 0
	}

	return table.SumSize()
}

// IsZero checks if the map is empty.
// This is faster than checking Size() == 0 as it can return early.
//
//go:nosplit
func (m *FlatMapOf[K, V]) IsZero() bool {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return true
	}

	return !table.SumSizeExceeds(0)
}

func (m *FlatMapOf[K, V]) beginRebuild(hint mapRebuildHint) (*flatRebuildState[K, V], bool) {
	rs := new(flatRebuildState[K, V])
	rs.hint = hint
	rs.wg.Add(1)
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return nil, false
	}
	return rs, true
}

func (m *FlatMapOf[K, V]) endRebuild(rs *flatRebuildState[K, V]) {
	atomic.StorePointer(&m.rs, nil)
	rs.wg.Done()
}

// rebuild reorganizes the map. Only these hints are supported:
//   - mapRebuildWithWritersHint: allows concurrent reads/writes
//   - mapExclusiveRebuildHint: allows concurrent reads
func (m *FlatMapOf[K, V]) rebuild(
	hint mapRebuildHint,
	fn func(),
) {
	for {
		// Help finishing rebuild if needed
		if rs := (*flatRebuildState[K, V])(atomic.LoadPointer(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if rs.newTableSeq.WriteCompleted() {
					m.helpCopyAndWait(rs)
				} else {
					runtime.Gosched()
					continue
				}
			default:
				rs.wg.Wait()
			}
		}
		if rs, ok := m.beginRebuild(hint); ok {
			fn()
			m.endRebuild(rs)
			return
		}
	}
}

//go:noinline
func (m *FlatMapOf[K, V]) tryResize(hint mapRebuildHint, size, sizeAdd int) {
	rs, ok := m.beginRebuild(hint)
	if !ok {
		return
	}

	table := m.table.Ptr()
	tableLen := table.mask + 1
	var newLen int
	if hint == mapGrowHint {
		if sizeAdd == 0 {
			newLen = max(calcTableLen(size), tableLen<<1)
		} else {
			newLen = calcTableLen(size + sizeAdd)
			if newLen <= tableLen {
				m.endRebuild(rs)
				return
			}
		}
	} else {
		// mapShrinkHint
		if sizeAdd == 0 {
			newLen = tableLen >> 1
		} else {
			newLen = calcTableLen(size)
		}
		if newLen < minTableLen {
			m.endRebuild(rs)
			return
		}
	}

	cpus := runtime.GOMAXPROCS(0)
	if cpus > 1 &&
		newLen*int(unsafe.Sizeof(flatBucket[K, V]{})) >= asyncThreshold {
		go m.finalizeResize(table, newLen, rs, cpus)
	} else {
		m.finalizeResize(table, newLen, rs, cpus)
	}
}

func (m *FlatMapOf[K, V]) finalizeResize(
	table *flatTable[K, V],
	newLen int,
	rs *flatRebuildState[K, V],
	cpus int,
) {
	overCpus := cpus * resizeOverPartition
	_, chunks := calcParallelism(table.mask+1, minBucketsPerCPU, overCpus)
	rs.chunks = int32(chunks)
	var newTable flatTable[K, V]
	newTable.makeTable(newLen, cpus)
	rs.newTableSeq.WriteLocked(&rs.newTable, newTable) // Release rs
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *FlatMapOf[K, V]) helpCopyAndWait(rs *flatRebuildState[K, V]) {
	table := m.tableSeq.Read(&m.table)
	newTable := rs.newTableSeq.Read(&rs.newTable) // Acquire rs
	if newTable.buckets.ptr == table.buckets.ptr {
		rs.wg.Wait()
		return
	}
	tableLen := table.mask + 1
	chunks := rs.chunks
	chunkSz := (tableLen + int(chunks) - 1) / int(chunks)
	isGrowth := (newTable.mask + 1) > tableLen
	for {
		process := atomic.AddInt32(&rs.process, 1)
		if process > chunks {
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSz
		end := min(start+chunkSz, tableLen)
		if isGrowth {
			m.copyBucket(&table, start, end, &newTable)
		} else {
			m.copyBucketLock(&table, start, end, &newTable)
		}
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			m.tableSeq.WriteLocked(&m.table, newTable)
			m.endRebuild(rs)
			return
		}
	}
}

func (m *FlatMapOf[K, V]) copyBucket(
	table *flatTable[K, V],
	start, end int,
	newTable *flatTable[K, V],
) {
	copied := 0
	var hash uintptr
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*flatBucket[K, V])(b.next) {
			meta := b.meta
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).Ptr()
				if embeddedHash {
					hash = e.getHash()
				} else {
					hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
				}
				idx := newTable.mask & h1(hash, m.intKey)
				destBucket := newTable.buckets.At(idx)
				h2v := h2(hash)

				b := destBucket
			appendTo:
				for {
					meta := b.meta
					empty := (^meta) & metaMask
					if empty != 0 {
						emptyIdx := firstMarkedByteIndex(empty)
						b.meta = setByte(meta, h2v, emptyIdx)
						entry := b.At(emptyIdx).Ptr()
						entry.value = e.value
						if embeddedHash {
							entry.setHash(hash)
						}
						entry.key = e.key
						break appendTo
					}
					next := (*flatBucket[K, V])(b.next)
					if next == nil {
						newE := flatEntry[K, V]{key: e.key, value: e.value}
						if embeddedHash {
							newE.setHash(hash)
						}
						bucket := &flatBucket[K, V]{
							meta: setByte(emptyMeta, h2v, 0),
							entries: [entriesPerBucket]seqlockSlot[flatEntry[K, V]]{
								{buf: newE},
							},
						}
						b.next = unsafe.Pointer(bucket)
						break appendTo
					}
					b = next
				}
				copied++
			}
		}
		srcBucket.Unlock()
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

func (m *FlatMapOf[K, V]) copyBucketLock(
	table *flatTable[K, V],
	start, end int,
	newTable *flatTable[K, V],
) {
	copied := 0
	var hash uintptr
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*flatBucket[K, V])(b.next) {
			meta := b.meta
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).Ptr()
				if embeddedHash {
					hash = e.getHash()
				} else {
					hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
				}
				idx := newTable.mask & h1(hash, m.intKey)
				destBucket := newTable.buckets.At(idx)
				h2v := h2(hash)

				destBucket.Lock()
				b := destBucket
			appendTo:
				for {
					meta := b.meta
					empty := (^meta) & metaMask
					if empty != 0 {
						emptyIdx := firstMarkedByteIndex(empty)
						b.meta = setByte(meta, h2v, emptyIdx)
						entry := b.At(emptyIdx).Ptr()
						entry.value = e.value
						if embeddedHash {
							entry.setHash(hash)
						}
						entry.key = e.key
						break appendTo
					}
					next := (*flatBucket[K, V])(b.next)
					if next == nil {
						newE := flatEntry[K, V]{e.key, e.value}
						if embeddedHash {
							newE.setHash(hash)
						}
						bucket := &flatBucket[K, V]{
							meta: setByte(emptyMeta, h2v, 0),
							entries: [entriesPerBucket]seqlockSlot[flatEntry[K, V]]{
								{buf: newE},
							},
						}
						b.next = unsafe.Pointer(bucket)
						break appendTo
					}
					b = next
				}
				destBucket.Unlock()
				copied++
			}
		}
		srcBucket.Unlock()
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

func (t *flatTable[K, V]) makeTable(
	tableLen, cpus int,
) {
	sizeLen := calcSizeLen(tableLen, cpus)
	t.buckets = makeUnsafeSlice(make([]flatBucket[K, V], tableLen))
	t.mask = tableLen - 1
	// if sizeLen <= 1 {
	// 	t.size.ptr = unsafe.Pointer(&t.smallSz)
	// } else {
	t.size = makeUnsafeSlice(make([]counterStripe, sizeLen))
	// }
	t.sizeMask = uint32(sizeLen - 1)
}

//go:nosplit
func (t *flatTable[K, V]) AddSize(idx, delta int) {
	atomic.AddUintptr(&t.size.At(int(t.sizeMask)&idx).c, uintptr(delta))
}

//go:nosplit
func (t *flatTable[K, V]) SumSize() int {
	var sum uintptr
	for i := 0; i <= int(t.sizeMask); i++ {
		sum += atomic.LoadUintptr(&t.size.At(i).c)
	}
	return int(sum)
}

//go:nosplit
func (t *flatTable[K, V]) SumSizeExceeds(limit int) bool {
	var sum uintptr
	for i := 0; i <= int(t.sizeMask); i++ {
		sum += atomic.LoadUintptr(&t.size.At(i).c)
		if int(sum) > limit {
			return true
		}
	}
	return false
}

//go:nosplit
func (b *flatBucket[K, V]) At(i int) *seqlockSlot[flatEntry[K, V]] {
	return (*seqlockSlot[flatEntry[K, V]])(unsafe.Add(
		unsafe.Pointer(&b.entries),
		uintptr(i)*unsafe.Sizeof(seqlockSlot[flatEntry[K, V]]{}),
	))
}

//go:nosplit
func (b *flatBucket[K, V]) Lock() {
	cur := atomic.LoadUint64(&b.meta)
	if atomic.CompareAndSwapUint64(&b.meta, cur&(^opLockMask), cur|opLockMask) {
		return
	}
	b.slowLock()
}

//go:nosplit
func (b *flatBucket[K, V]) slowLock() {
	var spins int
	for !b.tryLock() {
		delay(&spins)
	}
}

//go:nosplit
func (b *flatBucket[K, V]) tryLock() bool {
	for {
		cur := atomic.LoadUint64(&b.meta)
		if cur&opLockMask != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64(&b.meta, cur, cur|opLockMask) {
			return true
		}
	}
}

//go:nosplit
func (b *flatBucket[K, V]) Unlock() {
	atomic.StoreUint64(&b.meta, b.meta&^opLockMask)
}
