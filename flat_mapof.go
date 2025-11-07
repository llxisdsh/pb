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
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		_        noCopy
		table    flatTable[K, V]
		rs       unsafe.Pointer
		seed     uintptr
		keyHash  HashFunc
		shrinkOn bool
		intKey   bool
	}{})%CacheLineSize) % CacheLineSize]byte

	_        noCopy
	table    flatTable[K, V]
	rs       unsafe.Pointer // *flatRebuildState[K,V]
	seed     uintptr
	keyHash  HashFunc
	shrinkOn bool // WithShrinkEnabled
	intKey   bool
}

type flatRebuildState[K comparable, V any] struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		hint      mapRebuildHint
		chunks    int32
		newTable  flatTable[K, V]
		process   int32
		completed int32
		wg        sync.WaitGroup
	}{})%CacheLineSize) % CacheLineSize]byte
	hint      mapRebuildHint
	chunks    int32
	newTable  flatTable[K, V]
	process   int32 // atomic
	completed int32 // atomic
	wg        sync.WaitGroup
}

type flatTable[K comparable, V any] struct {
	buckets  unsafeSlice[flatBucket[K, V]]
	mask     int
	size     unsafeSlice[counterStripe]
	sizeMask uint32
	seq      uint32 // seqlock of table
	// The inline size has minimal effect on reducing cache misses,
	// so we will not use it for now.
	// smallSz  uintptr
}

type flatBucket[K comparable, V any] struct {
	seq     atomicUint64   // seqlock of bucket (even=stable, odd=write)
	meta    atomicUint64   // op byte + h2 bytes
	next    unsafe.Pointer // *flatBucket[K,V]
	entries [entriesPerBucket]flatEntry[K, V]
}

// NewFlatMapOf creates a new seqlock-based flat hash map.
//
// Highlights:
//   - Lock-free reads via per-bucket seqlock; automatic fallback under
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
	m.table.SeqStore(&newTable)
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
	table := m.table.SeqLoad()
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
//   - Fast path: per-bucket seqlock read; if the bucket sequence is even and
//     stable across the read, the snapshot is consistent.
//   - Contention handling: short spinning when write is observed (odd seq) or
//     a torn read; falls back to locked lookup if needed.
//   - Provides stable latency under high concurrency.
func (m *FlatMapOf[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.SeqLoad()
	if table.buckets.ptr == nil {
		return
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1(hash, m.intKey)
	root := table.buckets.At(idx)
	for b := root; b != nil; b = (*flatBucket[K, V])(atomic.LoadPointer(&b.next)) {
		var spins int
	retry:
		s1 := b.seq.Load()
		if (s1 & 1) != 0 {
			// writer in progress
			if trySpin(&spins) {
				goto retry
			}
			goto fallback
		}
		meta := b.meta.Load()
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			e := *b.At(j) // copy entry after seq check
			s2 := b.seq.Load()
			if s1 != s2 {
				if trySpin(&spins) {
					goto retry
				}
				goto fallback
			}
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

fallback:
	// fallback: find entry under lock
	root.Lock()
	for b := root; b != nil; b = (*flatBucket[K, V])(b.next) {
		meta := *b.meta.Raw()
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			e := b.At(j)
			v := e.value
			if embeddedHash {
				if e.getHash() == hash && e.key == key {
					root.Unlock()
					return v, true
				}
			} else {
				if e.key == key {
					root.Unlock()
					return v, true
				}
			}
		}
	}
	root.Unlock()
	return
}

// Range iterates all entries using per-bucket seqlock reads.
//
//   - Copies a consistent snapshot from each bucket when the seqlock sequence
//     is stable; otherwise briefly spins and falls back to locked collection.
//   - Yields outside of locks to minimize contention.
//   - Returning false from the callback stops iteration early.
func (m *FlatMapOf[K, V]) Range(yield func(K, V) bool) {
	table := m.table.SeqLoad()
	if table.buckets.ptr == nil {
		return
	}

	var s1, s2 uint64
	var meta uint64
	// Reusable cache, to avoid reading entry fields after s2 check
	type kvEntry struct {
		k K
		v V
	}
	var cache [entriesPerBucket]kvEntry
	var cacheCount int
	for i := 0; i <= table.mask; i++ {
		root := table.buckets.At(i)
		for b := root; b != nil; b = (*flatBucket[K, V])(atomic.LoadPointer(&b.next)) {
			spins := 0
			for {
				s1 = b.seq.Load()
				if (s1 & 1) != 0 {
					if trySpin(&spins) {
						continue
					}
					goto fallback
				}
				// copy entries after seq check
				meta = b.meta.Load()
				cacheCount = 0
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j)
					cache[cacheCount] = kvEntry{k: e.key, v: e.value}
					cacheCount++
				}
				s2 = b.seq.Load()
				if s1 == s2 {
					for j := range cacheCount {
						kv := &cache[j]
						if !yield(kv.k, kv.v) {
							return
						}
					}
					break
				}
				if trySpin(&spins) {
					continue
				}

			fallback:
				// fallback: collect entries under lock, yield outside lock
				cacheCount = 0
				root.Lock()
				meta = *b.meta.Raw()
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j)
					cache[cacheCount] = kvEntry{k: e.key, v: e.value}
					cacheCount++
				}
				root.Unlock()
				// yield outside lock
				for j := range cacheCount {
					kv := &cache[j]
					if !yield(kv.k, kv.v) {
						return
					}
				}
				break
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
		table := m.table.SeqLoad()
		if table.buckets.ptr == nil {
			return
		}

		for i := 0; i <= table.mask; i++ {
			root := table.buckets.At(i)
			root.Lock()
			for b := root; b != nil; b = (*flatBucket[K, V])(b.next) {
				meta := *b.meta.Raw()
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j)
					newV, op := fn(e.key, e.value)
					switch op {
					case CancelOp:
						// No-op
					case UpdateOp:
						s := *b.seq.Raw()
						b.seq.Store(s + 1)
						e.value = newV
						b.seq.Store(s + 2)
					case DeleteOp:
						// Keep snapshot fresh to prevent stale meta
						meta = setByte(meta, emptySlot, j)
						s := *b.seq.Raw()
						b.seq.Store(s + 1)
						b.meta.Store(meta)
						b.seq.Store(s + 2)
						*e = flatEntry[K, V]{}
						table.AddSize(i, -1)
					default:
						root.Unlock()
						panic(ErrUnexpectedOp)
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
		table := m.table.SeqLoad()
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
				if rs.newTable.SeqInitDone() {
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
		if atomic.LoadUint32(&m.table.seq) != table.seq {
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
			meta := *b.meta.Raw()
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j)
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
		case CancelOp:
			root.Unlock()
			return value, status
		case UpdateOp:
			if loaded {
				s := *oldB.seq.Raw()
				oldB.seq.Store(s + 1)
				oldB.At(oldIdx).value = newV
				oldB.seq.Store(s + 2)
				root.Unlock()
				return value, status
			}
			// insert new
			if emptyB != nil {
				// Prefill entry data before odd to shorten odd window
				entry := emptyB.At(emptyIdx)
				if embeddedHash {
					entry.setHash(hash)
				}
				entry.key = key
				entry.value = newV
				newMeta := setByte(*emptyB.meta.Raw(), h2v, emptyIdx)
				s := *emptyB.seq.Raw()
				emptyB.seq.Store(s + 1)
				// Publish meta while still holding the root lock to ensure
				// no other writer starts while this bucket is in odd state
				emptyB.meta.Store(newMeta)
				// Complete seqlock write (make it even) before
				// releasing root lock
				emptyB.seq.Store(s + 2)
				root.Unlock()
				table.AddSize(idx, 1)
				return value, status
			}
			// append new bucket
			bucket := &flatBucket[K, V]{
				meta: makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
				entries: [entriesPerBucket]flatEntry[K, V]{
					{key: key, value: newV},
				},
			}
			if embeddedHash {
				bucket.At(0).setHash(hash)
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
			// Precompute new meta and minimize odd window
			newMeta := setByte(oldMeta, emptySlot, oldIdx)
			s := *oldB.seq.Raw()
			oldB.seq.Store(s + 1)
			oldB.meta.Store(newMeta)
			oldB.seq.Store(s + 2)
			// After publishing even, clear entry fields before
			// releasing root lock
			*oldB.At(oldIdx) = flatEntry[K, V]{}
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
			root.Unlock()
			panic(ErrUnexpectedOp)
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
	table := m.table.SeqLoad()
	if table.buckets.ptr == nil {
		return
	}

	m.rebuild(mapRebuildBlockWritersHint, func() {
		var newTable flatTable[K, V]
		cpus := runtime.GOMAXPROCS(0)
		newTable.makeTable(minTableLen, cpus)
		m.table.SeqStore(&newTable)
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
	table := m.table.SeqLoad()
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
	table := m.table.SeqLoad()
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
				if rs.newTable.SeqInitDone() {
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

	table := &m.table
	tableLen := table.mask + 1
	var newLen int
	switch hint {
	case mapGrowHint:
		if sizeAdd == 0 {
			newLen = max(calcTableLen(size), tableLen<<1)
		} else {
			newLen = calcTableLen(size + sizeAdd)
			if newLen <= tableLen {
				m.endRebuild(rs)
				return
			}
		}
	case mapShrinkHint:
		if sizeAdd == 0 {
			newLen = tableLen >> 1
		} else {
			newLen = calcTableLen(size)
		}
		if newLen < minTableLen {
			m.endRebuild(rs)
			return
		}
	default:
		panic(ErrUnexpectedResizeHint)
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
	// Release rs
	rs.newTable.SeqStore(&newTable)
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *FlatMapOf[K, V]) helpCopyAndWait(rs *flatRebuildState[K, V]) {
	table := &m.table
	tableLen := table.mask + 1
	// Acquire rs
	newTable := rs.newTable.SeqLoad()
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
			m.copyBucket(table, start, end, &newTable)
		} else {
			m.copyBucketLock(table, start, end, &newTable)
		}
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			m.table.SeqStore(&newTable)
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
			meta := *b.meta.Raw()
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j)
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
					meta := *b.meta.Raw()
					empty := (^meta) & metaMask
					if empty != 0 {
						emptyIdx := firstMarkedByteIndex(empty)
						*b.meta.Raw() = setByte(meta, h2v, emptyIdx)
						entry := b.At(emptyIdx)
						entry.value = e.value
						if embeddedHash {
							entry.setHash(hash)
						}
						entry.key = e.key
						break appendTo
					}
					next := (*flatBucket[K, V])(b.next)
					if next == nil {
						bucket := &flatBucket[K, V]{
							meta:    makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
							entries: [entriesPerBucket]flatEntry[K, V]{{value: e.value, key: e.key}},
						}
						if embeddedHash {
							bucket.At(0).setHash(hash)
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
			meta := *b.meta.Raw()
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j)
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
					meta := *b.meta.Raw()
					empty := (^meta) & metaMask
					if empty != 0 {
						emptyIdx := firstMarkedByteIndex(empty)
						*b.meta.Raw() = setByte(meta, h2v, emptyIdx)
						entry := b.At(emptyIdx)
						entry.value = e.value
						if embeddedHash {
							entry.setHash(hash)
						}
						entry.key = e.key
						break appendTo
					}
					next := (*flatBucket[K, V])(b.next)
					if next == nil {
						bucket := &flatBucket[K, V]{
							meta:    makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
							entries: [entriesPerBucket]flatEntry[K, V]{{value: e.value, key: e.key}},
						}
						if embeddedHash {
							bucket.At(0).setHash(hash)
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
func (t *flatTable[K, V]) SeqLoad() flatTable[K, V] {
	for {
		s1 := atomic.LoadUint32(&t.seq)
		if s1&1 == 0 {
			v := *t
			s2 := atomic.LoadUint32(&t.seq)
			if s1 == s2 {
				return v
			}
		}
	}
}

//go:nosplit
func (t *flatTable[K, V]) SeqStore(v *flatTable[K, V]) {
	s := atomic.LoadUint32(&t.seq)
	atomic.StoreUint32(&t.seq, s+1)
	t.buckets = v.buckets
	t.mask = v.mask
	t.size = v.size
	t.sizeMask = v.sizeMask
	atomic.StoreUint32(&t.seq, s+2)
}

//go:nosplit
func (t *flatTable[K, V]) SeqInitDone() bool {
	return atomic.LoadUint32(&t.seq) == 2
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
func (b *flatBucket[K, V]) At(i int) *flatEntry[K, V] {
	return (*flatEntry[K, V])(unsafe.Add(
		unsafe.Pointer(&b.entries),
		uintptr(i)*unsafe.Sizeof(flatEntry[K, V]{}),
	))
}

//go:nosplit
func (b *flatBucket[K, V]) Lock() {
	cur := b.meta.Load()
	if b.meta.CompareAndSwap(cur&(^opLockMask), cur|opLockMask) {
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
		cur := b.meta.Load()
		if cur&opLockMask != 0 {
			return false
		}
		if b.meta.CompareAndSwap(cur, cur|opLockMask) {
			return true
		}
	}
}

//go:nosplit
func (b *flatBucket[K, V]) Unlock() {
	b.meta.Store(*b.meta.Raw() &^ opLockMask)
}

// atomicUint64 wraps atomic.Uint64 to leverage its built-in
// alignment capabilities. The primary purpose is to ensure
// 8-byte alignment on 32-bit architectures, where atomic.Uint64
// guarantees proper alignment for atomic operations.
type atomicUint64 struct {
	atomic.Uint64
}

//go:nosplit
func makeAtomicUint64(v uint64) atomicUint64 {
	return *(*atomicUint64)(unsafe.Pointer(&v))
}

//go:nosplit
func (a *atomicUint64) Raw() *uint64 {
	return (*uint64)(unsafe.Pointer(a))
}
