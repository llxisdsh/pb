package pb

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// FlatMapOf implements a flat hash map optimized for small K/V.
//
// EXPERIMENTAL: this implementation is considered experimental; APIs and concurrency semantics may evolve.
//
// Design highlights:
//   - Read path is lock-free: readers locate slots via meta (h2 byte); values are
//     read through atomicValue[V] (<= 8B) to guarantee non-torn loads; keys are
//     read without locks but are safe by the publication/deletion protocol below.
//   - Write path uses the root bucket lock to serialize updates within a bucket chain.
//   - Inline keys and per-slot atomic values minimize pointer chasing and improve GC locality.
//
// Key torn-read guarantees (incl. multi-word types like string):
//   - Insert publication order: (1) store key, (2) store value, (3) publish meta for
//     the slot. Readers load meta first, then key, then value. The publish-after-
//     initialize order prevents observing partially-written key/value.
//   - Delete unpublish order: (1) clear meta, (2) clear value. The key is intentionally
//     left intact and gets overwritten only when the slot is reused; thus readers never
//     observe a torn key even if racing with delete. This can retain key backing memory
//     until the slot is reused, which is a deliberate trade-off for safety and simplicity.
//   - Resize: new table is constructed privately and published via an atomic pointer swap;
//     see copyFlatBucketRange for per-bucket ordering, which is safe because readers
//     cannot see the new table before the swap.
//
// Trade-offs:
// - Extremely fast for small keys/values and read-heavy workloads; the read path is lock-free and cache-friendly.
// - V must be <= 8 bytes; otherwise use MapOf or store pointers/indirection.
// - No shrinking support.
// - Load semantics under delete race: a reader may observe the slot as present and race with a concurrent Delete;
//   in such cases Load may return ok=true with a zero value. If you require "return either the old value or miss",
//   implement an after-read meta revalidation and then retry or return miss. See the Load method comment for details.
// - Optimized for small K/V; large values may be less efficient.
//
// Notes:
//   - This type shares hashing, constants, and low-level helpers with MapOf
//     via the same package.
type FlatMapOf[K comparable, V any] struct {
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		table       unsafe.Pointer
		resizeState unsafe.Pointer
		seed        uintptr
		keyHash     HashFunc
		valEqual    EqualFunc
		minTableLen int
		// shrinkEnabled bool
		intKey bool
	}{})%CacheLineSize) % CacheLineSize]byte

	_           noCopy
	table       unsafe.Pointer // *flatTable[K,V]
	resizeState unsafe.Pointer // *flatResizeState
	seed        uintptr
	keyHash     HashFunc
	valEqual    EqualFunc
	minTableLen int
	// shrinkEnabled bool
	intKey bool
}

type flatTable[K comparable, V any] struct {
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		buckets     unsafeSlice[flatBucketOf[K, V]]
		bucketsMask int
		size        unsafeSlice[counterStripe]
		sizeMask    int
		chunks      int
		chunkSize   int
	}{})%CacheLineSize) % CacheLineSize]byte

	buckets     unsafeSlice[flatBucketOf[K, V]]
	bucketsMask int
	size        unsafeSlice[counterStripe]
	sizeMask    int
	// parallel copying parameters
	chunks    int
	chunkSize int
}

func newFlatTable[K comparable, V any](tableLen, cpus int) *flatTable[K, V] {
	b := make([]flatBucketOf[K, V], tableLen)
	chunkSize, chunks := calcParallelism(tableLen, minBucketsPerGoroutine, cpus)
	sizeLen := calcSizeLen(tableLen, cpus)
	return &flatTable[K, V]{
		buckets:     makeUnsafeSlice(b),
		bucketsMask: tableLen - 1,
		size:        makeUnsafeSlice(make([]counterStripe, sizeLen)),
		sizeMask:    sizeLen - 1,
		chunks:      chunks,
		chunkSize:   chunkSize,
	}
}

//go:nosplit
func (t *flatTable[K, V]) AddSize(bidx, delta int) {
	atomic.AddUintptr(&t.size.At(t.sizeMask&bidx).c, uintptr(delta))
}

//go:nosplit
func (t *flatTable[K, V]) SumSize() int {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += loadUintptr(&t.size.At(i).c)
	}
	return int(sum)
}

//go:nosplit
func (t *flatTable[K, V]) SumSizeExceeds(limit int) bool {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += loadUintptr(&t.size.At(i).c)
		if int(sum) > limit {
			return true
		}
	}
	return false
}

// flatBucketOf stores inline keys and atomic values for each slot.
// Uses atomicValue for lock-free value access instead of double buffering.
type flatBucketOf[K comparable, V any] struct {
	_    [0]int64 // keep same alignment trick
	meta uint64
	// Optional cacheline pad (kept minimal to reduce verbosity in this MVP)
	_keys [entriesPerMapOfBucket]K
	_vals [entriesPerMapOfBucket]atomicValue[V]
	next  unsafe.Pointer // *flatBucketOf[K,V]
}

//go:nosplit
func (b *flatBucketOf[K, V]) Key(i int) *K {
	return (*K)(unsafe.Add(
		unsafe.Pointer(&b._keys),
		uintptr(i)*unsafe.Sizeof(*new(K)),
	))
}

//go:nosplit
func (b *flatBucketOf[K, V]) Val(i int) *atomicValue[V] {
	return (*atomicValue[V])(unsafe.Add(
		unsafe.Pointer(&b._vals),
		uintptr(i)*unsafe.Sizeof(*new(atomicValue[V])),
	))
}

//go:nosplit
func (b *flatBucketOf[K, V]) Lock() {
	for {
		meta := atomic.LoadUint64(&b.meta)
		if meta&opLockMask != 0 {
			runtime.Gosched()
			continue
		}
		if atomic.CompareAndSwapUint64(&b.meta, meta, meta|opLockMask) {
			return
		}
	}
}

//go:nosplit
func (b *flatBucketOf[K, V]) Unlock() {
	atomic.StoreUint64(&b.meta, atomic.LoadUint64(&b.meta)&^opLockMask)
}

// NewFlatMapOf creates a new FlatMapOf instance with optional configuration.
// It supports the same configuration options as NewMapOf (WithPresize, WithKeyHasher, etc.)
// except WithShrinkEnabled which is not supported in FlatMapOf.
//
// Parameters:
//   - options: configuration options (WithPresize, WithKeyHasher, etc.)
//
// Note: WithShrinkEnabled is not supported and will be ignored.
func NewFlatMapOf[K comparable, V any](options ...func(*MapConfig)) *FlatMapOf[K, V] {
	if unsafe.Sizeof(*new(V)) > 8 {
		panic("value size must be 8 bytes or less")
	}

	var cfg MapConfig
	for _, opt := range options {
		opt(&cfg)
	}

	// parse interface
	if cfg.KeyHash == nil {
		var zeroK K
		ak := any(&zeroK)
		if _, ok := ak.(IHashCode); ok {
			cfg.KeyHash = func(ptr unsafe.Pointer, seed uintptr) uintptr {
				return any((*K)(ptr)).(IHashCode).HashCode(seed)
			}
			if i, ok := ak.(IHashOpts); ok {
				cfg.HashOpts = i.HashOpts()
			}
		}
	}
	if cfg.ValEqual == nil {
		var zeroV V
		vk := any(&zeroV)
		if _, ok := vk.(IEqual[V]); ok {
			cfg.ValEqual = func(ptr unsafe.Pointer, other unsafe.Pointer) bool {
				return any((*V)(ptr)).(IEqual[V]).Equal(*(*V)(other))
			}
		}
	}

	m := &FlatMapOf[K, V]{}
	m.seed = uintptr(rand.Uint64())
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	if cfg.KeyHash != nil {
		m.keyHash = cfg.KeyHash
		for _, o := range cfg.HashOpts {
			switch o {
			case LinearDistribution:
				m.intKey = true
			case ShiftDistribution:
				m.intKey = false
			case AutoDistribution:
				// default distribution
			}
		}
	}
	if cfg.ValEqual != nil {
		m.valEqual = cfg.ValEqual
	}
	m.minTableLen = calcTableLen(cfg.SizeHint)
	// m.shrinkEnabled = cfg.ShrinkEnabled

	table := newFlatTable[K, V](m.minTableLen, runtime.GOMAXPROCS(0))
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	return m
}

// Load retrieves the value associated with the given key.
// Returns the value and true if the key exists, or zero value and false otherwise.
// This operation is lock-free and uses atomicValue for consistent, non-torn value reads; key reads are safe per the publication/deletion protocol documented above.
// NOTE: Under a concurrent delete, a reader may observe the slot's meta as marked and then race with the value being cleared.
// In that case, returning a cleared (zero) value with ok=true would be undesirable.
// If stricter semantics are required (return either the old value or ok=false), add an after-read meta check and retry/return miss if the slot is no longer published.
func (m *FlatMapOf[K, V]) Load(key K) (value V, ok bool) {
	table := (*flatTable[K, V])(loadPointer(&m.table))
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	bidx := table.bucketsMask & h1(hash, m.intKey)
	for b := table.buckets.At(bidx); b != nil; b = (*flatBucketOf[K, V])(loadPointer(&b.next)) {
		metaw := loadUint64(&b.meta)
		for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if *b.Key(idx) == key {
				// Lock-free read using atomicValue
				value = b.Val(idx).Load()
				return value, true
			}
		}
	}
	return
}

// Range calls yield sequentially for each key and value present in the map.
// If yield returns false, Range stops the iteration.
// This operation uses atomicValue for lock-free value access.
func (m *FlatMapOf[K, V]) Range(yield func(K, V) bool) {
	table := (*flatTable[K, V])(loadPointer(&m.table))
	for i := 0; i <= table.bucketsMask; i++ {
		for b := table.buckets.At(i); b != nil; b = (*flatBucketOf[K, V])(loadPointer(&b.next)) {
			metaw := loadUint64(&b.meta)
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				key := *b.Key(idx)
				// Lock-free read using atomicValue
				value := b.Val(idx).Load()
				if !yield(key, value) {
					return
				}
			}
		}
	}
}

// Process applies a compute-style update to the map entry for the given key.
// The function fn receives the current value (if any) and whether the key exists,
// and returns the new value, operation type, result value, and status.
//
// Operation types:
//   - CancelOp: no change is made
//   - UpdateOp: upsert the new value
//   - DeleteOp: delete the key if present
//
// This operation is performed under a bucket lock for consistency.
func (m *FlatMapOf[K, V]) Process(
	key K,
	fn func(old V, loaded bool) (V, ComputeOp, V, bool),
) (V, bool) {
	table := (*flatTable[K, V])(loadPointer(&m.table))
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	for {
		bidx := table.bucketsMask & h1v
		rootB := table.buckets.At(bidx)
		rootB.Lock()

		// If a growth/shrink is in progress with both tables set, help finish it
		if rs := (*flatResizeState)(loadPointer(&m.resizeState)); rs != nil &&
			loadPointer(&rs.table) != nil && loadPointer(&rs.newTable) != nil {
			rootB.Unlock()
			m.helpCopyAndWait(rs)
			table = (*flatTable[K, V])(loadPointer(&m.table))
			continue
		}

		// Verify table wasn't swapped after lock acquisition
		if newTable := (*flatTable[K, V])(loadPointer(&m.table)); table != newTable {
			rootB.Unlock()
			table = newTable
			continue
		}

		var (
			oldB     *flatBucketOf[K, V]
			oldIdx   int
			oldMeta  uint64
			oldVal   V
			loaded   bool
			emptyB   *flatBucketOf[K, V]
			emptyIdx int
			lastB    *flatBucketOf[K, V]
		)

	findLoop:
		for b := rootB; b != nil; b = (*flatBucketOf[K, V])(b.next) {
			metaw := b.meta
			for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				if *b.Key(idx) == key {
					oldB = b
					oldIdx = idx
					oldMeta = metaw
					oldVal = b.Val(idx).Val
					loaded = true
					break findLoop
				}
			}
			if emptyB == nil {
				if emptyw := (^metaw) & metaMask; emptyw != 0 {
					emptyB = b
					emptyIdx = firstMarkedByteIndex(emptyw)
				}
			}
			lastB = b
		}

		newV, op, value, status := fn(oldVal, loaded)
		switch op {
		case DeleteOp:
			if !loaded {
				rootB.Unlock()
				return value, status
			}
			// Clear the meta slot to mark as deleted (publish-unpublish barrier):
			// readers that still observe the old meta may only ever see the old key; we never clear the key on delete to avoid torn key reads under concurrency.
			newmetaw := setByte(oldMeta, emptyMetaSlot, oldIdx)
			storeUint64(&oldB.meta, newmetaw)
			// Clear references: only clear the value to aid GC; keep the old key intact until the slot is reused by a future insert.
			// The old key remains invisible to readers after meta is cleared; when reusing the slot, we (1) store key, (2) store value, then (3) set meta.
			// Keeping the key may retain its backing memory until reuse; this is a conscious trade-off to avoid torn key reads.
			oldB.Val(oldIdx).Store(*new(V))
			rootB.Unlock()
			table.AddSize(bidx, -1)
			return value, status
		case UpdateOp:
			if loaded {
				// Atomically store new value
				oldB.Val(oldIdx).Store(newV)
				rootB.Unlock()
				return value, status
			}
			// insert new
			if emptyB != nil {
				*emptyB.Key(emptyIdx) = key
				// Initialize atomicValue with new value (before publishing via meta)
				emptyB.Val(emptyIdx).Store(newV)
				// Publish the slot by setting meta last (release)
				newMeta := setByte(emptyB.meta, h2v, emptyIdx)
				storeUint64(&emptyB.meta, newMeta)
				rootB.Unlock()

				table.AddSize(bidx, 1)
				// Early grow: only consider when the bucket just became full
				// to reduce overhead in single-thread case
				if (bidx&1023) == 0 && loadPointer(&m.resizeState) == nil {
					tableLen := table.bucketsMask + 1
					size := table.SumSize()
					const sizeHintFactor = float64(entriesPerMapOfBucket) * mapLoadFactor
					if size >= int(float64(tableLen)*sizeHintFactor) {
						m.tryResize(mapGrowHint, size, 0)
					}
				}
				return value, status
			}
			// append new bucket
			newBucket := &flatBucketOf[K, V]{
				meta:  setByte(emptyMeta, h2v, 0),
				_keys: [entriesPerMapOfBucket]K{key},
				_vals: [entriesPerMapOfBucket]atomicValue[V]{atomicValue[V]{Val: newV}},
			}
			// Publish the new bucket into the chain with a release store so readers see a fully initialized bucket.
			storePointer(&lastB.next, unsafe.Pointer(newBucket))
			rootB.Unlock()
			table.AddSize(bidx, 1)
			// Auto-grow check (parallel resize)
			if loadPointer(&m.resizeState) == nil {
				tableLen := table.bucketsMask + 1
				size := table.SumSize()
				const sizeHintFactor = float64(entriesPerMapOfBucket) * mapLoadFactor
				if size >= int(float64(tableLen)*sizeHintFactor) {
					m.tryResize(mapGrowHint, size, 0)
				}
			}
			return value, status
		default:
			// CancelOp
			rootB.Unlock()
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

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	if v, ok := m.Load(key); ok {
		return v, true
	}

	return m.Process(key,
		func(old V, loaded bool) (V, ComputeOp, V, bool) {
			if loaded {
				return old, CancelOp, old, loaded
			}
			return value, UpdateOp, old, loaded
		},
	)
}

// Delete deletes the value for a key.
func (m *FlatMapOf[K, V]) Delete(key K) {
	m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return old, DeleteOp, old, loaded
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
// This operation sums counters across all size stripes for an approximate count.
//
//go:nosplit
func (m *FlatMapOf[K, V]) Size() int {
	table := (*flatTable[K, V])(loadPointer(&m.table))
	return table.SumSize()
}

// IsZero checks if the map is empty.
// This is faster than checking Size() == 0 as it can return early.
//
//go:nosplit
func (m *FlatMapOf[K, V]) IsZero() bool {
	table := (*flatTable[K, V])(loadPointer(&m.table))
	return !table.SumSizeExceeds(0)
}

type flatResizeState struct {
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		wg        sync.WaitGroup
		table     unsafe.Pointer
		newTable  unsafe.Pointer
		process   int32
		completed int32
	}{})%CacheLineSize) % CacheLineSize]byte

	wg        sync.WaitGroup
	table     unsafe.Pointer // *flatTable[K,V]
	newTable  unsafe.Pointer // *flatTable[K,V]
	process   int32
	completed int32
}

//go:noinline
func (m *FlatMapOf[K, V]) tryResize(hint mapResizeHint, size, sizeAdd int) {
	rs := new(flatResizeState)
	rs.wg.Add(1)
	if !atomic.CompareAndSwapPointer(&m.resizeState, nil, unsafe.Pointer(rs)) {
		return
	}
	cpus := runtime.GOMAXPROCS(0)
	if hint == mapClearHint {
		newTable := newFlatTable[K, V](m.minTableLen, cpus)
		atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
		atomic.StorePointer(&m.resizeState, nil)
		rs.wg.Done()
		return
	}

	table := (*flatTable[K, V])(loadPointer(&m.table))
	tableLen := table.bucketsMask + 1
	var newTableLen int
	if hint == mapGrowHint {
		if sizeAdd == 0 {
			newTableLen = max(calcTableLen(size), tableLen<<1)
		} else {
			newTableLen = calcTableLen(size + sizeAdd)
			if newTableLen <= tableLen {
				atomic.StorePointer(&m.resizeState, nil)
				rs.wg.Done()
				return
			}
		}
	} else { // mapShrinkHint
		if sizeAdd == 0 {
			newTableLen = tableLen >> 1
		} else {
			newTableLen = calcTableLen(size)
		}
		if newTableLen < m.minTableLen {
			atomic.StorePointer(&m.resizeState, nil)
			rs.wg.Done()
			return
		}
	}

	if newTableLen >= int(asyncResizeThreshold) && cpus > 1 {
		go m.finalizeResize(table, newTableLen, rs, cpus)
	} else {
		m.finalizeResize(table, newTableLen, rs, cpus)
	}
}

func (m *FlatMapOf[K, V]) finalizeResize(
	table *flatTable[K, V],
	newTableLen int,
	rs *flatResizeState,
	cpus int,
) {
	atomic.StorePointer(&rs.table, unsafe.Pointer(table))
	newTable := newFlatTable[K, V](newTableLen, cpus)
	atomic.StorePointer(&rs.newTable, unsafe.Pointer(newTable))
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *FlatMapOf[K, V]) helpCopyAndWait(rs *flatResizeState) {
	table := (*flatTable[K, V])(loadPointer(&rs.table))
	tableLen := table.bucketsMask + 1
	chunks := int32(table.chunks)
	chunkSize := table.chunkSize
	newTable := (*flatTable[K, V])(loadPointer(&rs.newTable))
	for {
		process := atomic.AddInt32(&rs.process, 1)
		if process > chunks {
			// Wait copying completed
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSize
		end := min(start+chunkSize, tableLen)
		m.copyFlatBucketRange(table, start, end, newTable)
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			// Copying completed
			atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
			atomic.StorePointer(&m.resizeState, nil)
			rs.wg.Done()
			return
		}
	}
}

func (m *FlatMapOf[K, V]) copyFlatBucketRange(
	table *flatTable[K, V],
	start, end int,
	newTable *flatTable[K, V],
) {
	copied := 0
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*flatBucketOf[K, V])(b.next) {
			metaw := b.meta
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				k := *b.Key(idx)
				// Read value atomically
				v := b.Val(idx).Val
				hash := m.keyHash(noescape(unsafe.Pointer(&k)), m.seed)
				bidx := newTable.bucketsMask & h1(hash, m.intKey)
				destBucket := newTable.buckets.At(bidx)
				h2v := h2(hash)

				destb := destBucket
			appendToBucket:
				for {
					metaw := destb.meta
					emptyw := (^metaw) & metaMask
					if emptyw != 0 {
						emptyIdx := firstMarkedByteIndex(emptyw)
						// It is safe to set meta first here because newTable is not published to readers
						// until the final atomic swap of m.table in helpCopyAndWait; no reader can observe
						// destb before that point.
						destb.meta = setByte(metaw, h2v, emptyIdx)
						*destb.Key(emptyIdx) = k
						destb.Val(emptyIdx).Val = v
						break appendToBucket
					}
					next := (*flatBucketOf[K, V])(destb.next)
					if next == nil {
						newBucket := &flatBucketOf[K, V]{
							meta:  setByte(emptyMeta, h2v, 0),
							_keys: [entriesPerMapOfBucket]K{k},
							_vals: [entriesPerMapOfBucket]atomicValue[V]{atomicValue[V]{Val: v}},
						}
						// Safe for the same reason as above: newTable is private until published.
						destb.next = unsafe.Pointer(newBucket)
						break appendToBucket
					}
					destb = next
				}
				copied++
			}
		}
		srcBucket.Unlock()
	}
	if copied != 0 {
		// copyFlatBucketRange is used during multithreaded growth; increment size via striped counter.
		newTable.AddSize(start, copied)
	}
}
