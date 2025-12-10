//go:build !race

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
//
// Notes:
//   - Reuses MapOf constants and compile-time bucket sizing (entriesPerBucket).
//   - Buckets are packed without padding for cache-friendly layout.
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
	sizeMask int
}

type flatBucket[K comparable, V any] struct {
	_       [0]atomic.Uint64
	meta    uint64                          // op byte + h2 bytes
	seq     seqlock[uintptr, EntryOf[K, V]] // seqlock of bucket
	next    unsafe.Pointer                  // *flatBucket[K,V]
	entries [entriesPerBucket]seqlockSlot[EntryOf[K, V]]
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
		opt(noEscape(&cfg))
	}
	m := &FlatMapOf[K, V]{}
	m.init(noEscape(&cfg))
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
	tableLen := calcTableLen(cfg.SizeHint)
	m.tableSeq.WriteLocked(&m.table, newFlatTable[K, V](tableLen, runtime.GOMAXPROCS(0)))
}

//go:noinline
func (m *FlatMapOf[K, V]) initSlow() {
	rs := (*flatRebuildState[K, V])(loadPtr(&m.rs))
	if rs != nil {
		rs.wg.Wait()
		return
	}
	rs, ok := m.beginRebuild(mapRebuildBlockWritersHint)
	if !ok {
		rs = (*flatRebuildState[K, V])(loadPtr(&m.rs))
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
	var cfg MapConfig
	m.init(noEscape(&cfg))
	m.endRebuild(rs)
}

// Load retrieves the value for a key.
//
//   - Per-bucket seqlock read; an even and stable sequence yields
//     a consistent snapshot.
//   - Short spinning on observed writes (odd seq) or
//     instability; no locked fallback path.
//   - Provides stable latency under high concurrency.
func (m *FlatMapOf[K, V]) Load(key K) (value V, ok bool) {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return *new(V), false
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1(hash, m.intKey)
	for b := table.buckets.At(idx); b != nil; b = (*flatBucket[K, V])(loadPtr(&b.next)) {
		var spins int
	retry:
		s1, ok := b.seq.BeginRead()
		if !ok {
			delay(&spins)
			goto retry
		}
		meta := b.meta
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			e := b.At(j).ReadUnfenced()
			if !b.seq.EndRead(s1) {
				goto retry
			}
			if embeddedHash_ {
				if e.getHash() == hash && e.Key == key {
					return e.Value, true
				}
			} else {
				if e.Key == key {
					return e.Value, true
				}
			}
		}
	}
	return *new(V), false
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
	var cache [entriesPerBucket]EntryOf[K, V]
	var cacheCount int
	for i := 0; i <= table.mask; i++ {
		for b := table.buckets.At(i); b != nil; b = (*flatBucket[K, V])(loadPtr(&b.next)) {
			var spins int
		retry:
			s1, ok := b.seq.BeginRead()

			if !ok {
				delay(&spins)
				goto retry
			}
			meta = b.meta
			cacheCount = 0
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				cache[cacheCount] = b.At(j).ReadUnfenced()
				cacheCount++
			}
			if !b.seq.EndRead(s1) {
				goto retry
			}
			for j := range cacheCount {
				kv := &cache[j]
				if !yield(kv.Key, kv.Value) {
					return
				}
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
					newV, op := fn(e.Ptr().Key, e.Ptr().Value)
					switch op {
					case UpdateOp:
						newE := EntryOf[K, V]{Key: e.Ptr().Key, Value: newV}
						if embeddedHash_ {
							newE.setHash(e.Ptr().getHash())
						}
						b.seq.BeginWriteLocked()
						e.WriteUnfenced(newE)
						b.seq.EndWriteLocked()
					case DeleteOp:
						meta = setByte(meta, slotEmpty, j)
						storeInt(&b.meta, meta)
						b.seq.BeginWriteLocked()
						e.WriteUnfenced(EntryOf[K, V]{})
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
		if rs := (*flatRebuildState[K, V])(loadPtr(&m.rs)); rs != nil {
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
			oldV      V
			oldB      *flatBucket[K, V]
			oldIdx    int
			oldMeta   uint64
			emptyB    *flatBucket[K, V]
			emptyIdx  int
			emptyMeta uint64
			lastB     *flatBucket[K, V]
		)

	findLoop:
		for b := root; b != nil; b = (*flatBucket[K, V])(b.next) {
			meta := b.meta
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).Ptr()
				if embeddedHash_ {
					if e.getHash() == hash && e.Key == key {
						oldV, oldB, oldIdx, oldMeta = e.Value, b, j, meta
						break findLoop
					}
				} else {
					if e.Key == key {
						oldV, oldB, oldIdx, oldMeta = e.Value, b, j, meta
						break findLoop
					}
				}
			}
			if emptyB == nil {
				if empty := (^meta) & metaMask; empty != 0 {
					emptyB = b
					emptyIdx = firstMarkedByteIndex(empty)
					emptyMeta = meta
				}
			}
			lastB = b
		}

		newV, op, value, status := fn(oldV, oldB != nil)
		switch op {
		case UpdateOp:
			if oldB != nil {
				e := oldB.At(oldIdx)
				newE := EntryOf[K, V]{Key: e.Ptr().Key, Value: newV}
				if embeddedHash_ {
					newE.setHash(e.Ptr().getHash())
				}
				oldB.seq.BeginWriteLocked()
				e.WriteUnfenced(newE)
				oldB.seq.EndWriteLocked()
				root.Unlock()
				return value, status
			}
			newE := EntryOf[K, V]{Key: key, Value: newV}
			if embeddedHash_ {
				newE.setHash(hash)
			}
			// insert new
			if emptyB != nil {
				// insert new: no seqlock window needed since slot was empty.
				// Reader won't access slot until meta is published with valid h2.
				// StoreBarrier ensures Entry is visible before meta update on ARM.
				emptyB.At(emptyIdx).WriteUnfenced(newE)
				// emptyB.seq.WriteBarrier()
				newMeta := setByte(emptyMeta, h2v, emptyIdx)
				storeInt(&emptyB.meta, newMeta)

				root.Unlock()
				table.AddSize(idx, 1)
				return value, status
			}
			// append new bucket
			storePtr(&lastB.next, unsafe.Pointer(&flatBucket[K, V]{
				meta: setByte(metaEmpty, h2v, 0),
				entries: [entriesPerBucket]seqlockSlot[EntryOf[K, V]]{
					{buf: newE},
				},
			}))
			root.Unlock()
			table.AddSize(idx, 1)
			// Auto-grow check (parallel resize)
			if loadPtr(&m.rs) == nil {
				tableLen := table.mask + 1
				size := table.SumSize()
				const sizeHintFactor = float64(entriesPerBucket) * loadFactor
				if size >= int(float64(tableLen)*sizeHintFactor) {
					m.tryResize(mapGrowHint, size, 0)
				}
			}
			return value, status
		case DeleteOp:
			if oldB == nil {
				root.Unlock()
				return value, status
			}
			// Delete: update meta first so new Readers skip this slot immediately.
			// Active Readers will see seq change and retry, then see h2=0.
			newMeta := setByte(oldMeta, slotEmpty, oldIdx)
			storeInt(&oldB.meta, newMeta)
			oldB.seq.BeginWriteLocked()
			oldB.At(oldIdx).WriteUnfenced(EntryOf[K, V]{})
			oldB.seq.EndWriteLocked()

			root.Unlock()
			table.AddSize(idx, -1)
			// Check if table shrinking is needed
			if m.shrinkOn && newMeta&metaDataMask == metaEmpty &&
				loadPtr(&m.rs) == nil {
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
	if enableFastPath {
		if v, ok := m.Load(key); ok {
			return v, true
		}
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
	if enableFastPath {
		if v, ok := m.Load(key); ok {
			return v, true
		}
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
	if enableFastPath {
		if _, ok := m.Load(key); !ok {
			return
		}
	}
	m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return old, DeleteOp, old, loaded
	})
}

// LoadAndDelete deletes the value for a key, returning the previous value.
// The loaded result reports whether the key was present.
func (m *FlatMapOf[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	if enableFastPath {
		if v, ok := m.Load(key); !ok {
			return v, ok
		}
	}
	return m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return old, DeleteOp, old, loaded
	})
}

// LoadAndUpdate updates the value for key if it exists, returning the previous
// value. The loaded result reports whether the key was present.
func (m *FlatMapOf[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	if enableFastPath {
		if v, ok := m.Load(key); !ok {
			return v, ok
		}
	}
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
		m.tableSeq.WriteLocked(&m.table, newFlatTable[K, V](minTableLen, runtime.GOMAXPROCS(0)))
	})
}

// All returns an iterator function for use with range-over-func.
// It provides the same functionality as Range but in iterator form.
func (m *FlatMapOf[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the number of key-value pairs in the map.
// This operation sums counters across all size stripes for an approximate
// count.
func (m *FlatMapOf[K, V]) Size() int {
	table := m.tableSeq.Read(&m.table)
	if table.buckets.ptr == nil {
		return 0
	}

	return table.SumSize()
}

// IsZero checks if the map is empty.
func (m *FlatMapOf[K, V]) IsZero() bool {
	return m.Size() == 0
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
		if rs := (*flatRebuildState[K, V])(loadPtr(&m.rs)); rs != nil {
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
			if newLen < minTableLen {
				m.endRebuild(rs)
				return
			}
		} else {
			newLen = calcTableLen(size)
			if newLen >= tableLen {
				m.endRebuild(rs)
				return
			}
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
	rs.newTableSeq.WriteLocked(&rs.newTable, newFlatTable[K, V](newLen, cpus)) // Release rs
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
	oldLen := table.mask + 1
	newLen := newTable.mask + 1
	// Determines the concurrent task range for destination buckets.
	// We iterate based on the "Destination Constraint" to allow lock-free
	// writes:
	// - Grow (Pow2):   baseLen == oldLen. Source i moves to Dest i, i+baseLen...
	// - Shrink (Pow2): baseLen == newLen. Source i, i+baseLen... move to Dest i.
	// By iterating 0..baseLen and processing all aliasing source buckets
	// (srcIdx += baseLen) in the inner loop, a single goroutine exclusively
	// owns the write operations for its assigned destination buckets.
	baseLen := min(newLen, oldLen)
	chunks := rs.chunks
	chunkSz := (baseLen + int(chunks) - 1) / int(chunks)
	for {
		process := atomic.AddInt32(&rs.process, 1)
		if process > chunks {
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSz
		end := min(start+chunkSz, baseLen)
		m.copyBucket(&table, start, end, oldLen, baseLen, &newTable)
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
	oldLen, baseLen int,
	newTable *flatTable[K, V],
) {
	mask := newTable.mask
	seed := m.seed
	keyHash := m.keyHash
	intKey := m.intKey
	copied := 0
	for i := start; i < end; i++ {
		// Visit all source buckets that map to this destination bucket.
		// In Grow, runs once. In Shrink, runs twice (usually).
		for srcIdx := i; srcIdx < oldLen; srcIdx += baseLen {
			srcB := table.buckets.At(srcIdx)
			srcB.Lock()
			for b := srcB; b != nil; b = (*flatBucket[K, V])(b.next) {
				meta := b.meta
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j).Ptr()
					var hash uintptr
					if embeddedHash_ {
						hash = e.getHash()
					} else {
						hash = keyHash(noescape(unsafe.Pointer(&e.Key)), seed)
					}
					idx := mask & h1(hash, intKey)
					destB := newTable.buckets.At(idx)
					// Append entry to the destination bucket
					h2v := h2(hash)
					for {
						meta := destB.meta
						empty := (^meta) & metaMask
						if empty != 0 {
							emptyIdx := firstMarkedByteIndex(empty)
							destB.meta = setByte(meta, h2v, emptyIdx)
							*destB.At(emptyIdx).Ptr() = *e
							break
						}
						next := (*flatBucket[K, V])(destB.next)
						if next == nil {
							destB.next = unsafe.Pointer(&flatBucket[K, V]{
								meta: setByte(metaEmpty, h2v, 0),
								entries: [entriesPerBucket]seqlockSlot[EntryOf[K, V]]{
									{buf: *e},
								},
							})
							break
						}
						destB = next
					}
					copied++
				}
			}
			srcB.Unlock()
		}
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

func newFlatTable[K comparable, V any](
	tableLen, cpus int,
) flatTable[K, V] {
	sizeLen := calcSizeLen(tableLen, cpus)
	return flatTable[K, V]{
		buckets:  makeUnsafeSlice(make([]flatBucket[K, V], tableLen)),
		mask:     tableLen - 1,
		size:     makeUnsafeSlice(make([]counterStripe, sizeLen)),
		sizeMask: sizeLen - 1,
	}
}

//go:nosplit
func (t *flatTable[K, V]) AddSize(idx, delta int) {
	atomic.AddUintptr(&t.size.At(t.sizeMask&idx).c, uintptr(delta))
}

//go:nosplit
func (t *flatTable[K, V]) SumSize() int {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += loadInt(&t.size.At(i).c)
	}
	return int(sum)
}

//go:nosplit
func (b *flatBucket[K, V]) At(i int) *seqlockSlot[EntryOf[K, V]] {
	return (*seqlockSlot[EntryOf[K, V]])(unsafe.Add(
		unsafe.Pointer(&b.entries),
		uintptr(i)*unsafe.Sizeof(seqlockSlot[EntryOf[K, V]]{}),
	))
}

func (b *flatBucket[K, V]) Lock() {
	cur := loadInt(&b.meta)
	if atomic.CompareAndSwapUint64(&b.meta, cur&(^opLockMask), cur|opLockMask) {
		return
	}
	b.slowLock()
}

func (b *flatBucket[K, V]) slowLock() {
	var spins int
	for !b.tryLock() {
		delay(&spins)
	}
}

//go:nosplit
func (b *flatBucket[K, V]) tryLock() bool {
	for {
		cur := loadInt(&b.meta)
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
