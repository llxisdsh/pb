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
// Design highlights:
//   - Read path is lock-free: readers locate slots via meta (h2 byte)
//     and read values through atomicValue[V] (<= 8B) for non-torn loads.
//     Keys are read without locks and remain safe by the publication
//     / deletion protocol described below.
//   - Write path uses the root bucket lock to serialize updates within
//     a bucket chain.
//   - Inline keys and per-slot atomic values minimize pointer chasing
//     and improve GC locality.
//
// Publication and deletion ordering (incl. multi-word types like string):
//   - Insert publication order:
//     (1) store key, (2) store value, (3) publish meta for the slot.
//     Readers load meta first, then key, then value. This publish-after-
//     initialize order prevents observing partially-written key/value.
//   - Delete unpublish order:
//     (1) clear meta, (2) optionally clear value (see WithZeroAsDeleted).
//     The key is intentionally left intact and will be overwritten only
//     when the slot is reused. This prevents torn key reads while keeping
//     the protocol simple. It can retain key backing memory until reuse,
//     which is a deliberate trade-off for safety and simplicity.
//   - Resize: a new table is constructed privately and published via an
//     atomic pointer swap. Per-bucket ordering is handled in
//     copyFlatBucketRange and is safe because readers cannot see the new
//     table before the swap.
//
// WithZeroAsDeleted semantics:
//   - Default: disabled. Zero values are treated as ordinary values and
//     returned by reads.
//   - When enabled via WithZeroAsDeleted, a zero value is treated as
//     logically deleted on the read path and is filtered out. Deletes may
//     clear the stored value to zero to aid GC after meta is cleared.
//     Use with caution if zero is a meaningful value for V.
//
// Trade-offs:
//   - Extremely fast for small keys/values and read-heavy workloads; the
//     read path is lock-free and cache-friendly.
//   - V must be <= 8 bytes; otherwise use MapOf or store pointers / an
//     indirection.
//   - No shrinking support.
//   - Optimized for small K/V; large values may be less efficient.
//
// Notes:
//   - This type shares hashing, constants, and low-level helpers with
//     MapOf via the same package.
//
// EXPERIMENTAL: this implementation is experimental; APIs and
// concurrency semantics may evolve.
type FlatMapOf[K comparable, V comparable] struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		_       noCopy
		table   unsafe.Pointer
		resize  unsafe.Pointer
		seed    uintptr
		keyHash HashFunc
		minLen  int
		intKey  bool
		zeroDel bool
	}{})%CacheLineSize) % CacheLineSize]byte

	_       noCopy
	table   unsafe.Pointer // *flatTable[K,V]
	resize  unsafe.Pointer // *flatResizeState
	seed    uintptr
	keyHash HashFunc // WithKeyHasher
	minLen  int      // WithPresize
	intKey  bool
	zeroDel bool // WithZeroAsDeleted
}

// WithZeroAsDeleted configures the map to treat zero values as logically
// deleted. This means that when a value is set to its zero value, it is
// considered deleted and will not be returned by read operations.
//
// Default behavior: disabled. When disabled, zero values are treated as
// ordinary values and are returned by reads just like non-zero values.
//
// Note: Enable with care if the zero value is a valid value for V.
func WithZeroAsDeleted() func(*MapConfig) {
	return func(c *MapConfig) {
		c.zeroAsDeleted = true
	}
}

// NewFlatMapOf creates a new FlatMapOf instance with optional configuration.
// It supports the same configuration options as NewMapOf
//
// Parameters:
//   - options: configuration options
//     WithZeroAsDeleted, WithPresize, WithKeyHasher, etc.
//     WithShrinkEnabled is not supported and will be ignored.
func NewFlatMapOf[K comparable, V comparable](
	options ...func(*MapConfig),
) *FlatMapOf[K, V] {
	checkAtomicValueSize[V]()

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

	m := &FlatMapOf[K, V]{}
	m.seed = uintptr(rand.Uint64())
	m.keyHash, _, m.intKey = defaultHasher[K, V]()
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

	m.minLen = calcTableLen(cfg.SizeHint)
	m.zeroDel = cfg.zeroAsDeleted

	table := newFlatTable[K, V](m.minLen, runtime.GOMAXPROCS(0))
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	return m
}

type flatTable[K comparable, V comparable] struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		buckets  unsafeSlice[flatBucket[K, V]]
		mask     int
		size     unsafeSlice[counterStripe]
		sizeMask int
		chunks   int
		chunkSz  int
	}{})%CacheLineSize) % CacheLineSize]byte

	buckets  unsafeSlice[flatBucket[K, V]]
	mask     int
	size     unsafeSlice[counterStripe]
	sizeMask int
	// parallel copying parameters
	chunks  int
	chunkSz int
}

func newFlatTable[K comparable, V comparable](
	tableLen, cpus int,
) *flatTable[K, V] {
	b := make([]flatBucket[K, V], tableLen)
	chunkSz, chunks := calcParallelism(tableLen, minBucketsPerCPU, cpus)
	sizeLen := calcSizeLen(tableLen, cpus)
	return &flatTable[K, V]{
		buckets:  makeUnsafeSlice(b),
		mask:     tableLen - 1,
		size:     makeUnsafeSlice(make([]counterStripe, sizeLen)),
		sizeMask: sizeLen - 1,
		chunks:   chunks,
		chunkSz:  chunkSz,
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
		sum += atomic.LoadUintptr(&t.size.At(i).c)
	}
	return int(sum)
}

//go:nosplit
func (t *flatTable[K, V]) SumSizeExceeds(limit int) bool {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += atomic.LoadUintptr(&t.size.At(i).c)
		if int(sum) > limit {
			return true
		}
	}
	return false
}

// flatBucket stores inline keys and atomic values for each slot.
// Uses atomicValue for lock-free value access instead of double buffering.
type flatBucket[K comparable, V comparable] struct {
	meta    atomicUint64
	entries [entriesPerBucket]flatEntry[K, V]
	next    unsafe.Pointer // *flatBucket[K,V]
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
	spins := 0
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

//go:nosplit
func (b *flatBucket[K, V]) UnlockWithMeta(meta uint64) {
	b.meta.Store(meta &^ opLockMask)
}

//go:nosplit
func (m *FlatMapOf[K, V]) valueIsValid(v V) bool {
	return v != *new(V) || !m.zeroDel
}

// Load retrieves the value associated with the given key.
// Returns the value and true if the key exists, or zero value and false
// otherwise. This operation is lock-free and uses atomicValue for consistent,
// non-torn value reads; key reads are safe per the publication/deletion
// protocol documented above.
func (m *FlatMapOf[K, V]) Load(key K) (value V, ok bool) {
	table := (*flatTable[K, V])(atomic.LoadPointer(&m.table))
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2 := broadcast(h2v)
	idx := table.mask & h1(hash, m.intKey)
	for b := table.buckets.At(idx); b != nil; b = (*flatBucket[K, V])(atomic.LoadPointer(&b.next)) {
		meta := b.meta.Load()
		for marked := markZeroBytes(meta ^ h2); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			e := b.At(j)
			if v := e.value.Load(); m.valueIsValid(v) {
				// Pre-read revalidation: ensure the slot is currently published
				// with the same h2 before touching the key to avoid torn key
				// reads when the slot is temporarily unpublished.
				cur := b.meta.Load()
				if uint8(cur>>(uint(j)*8)) != (h2v | slotMask) {
					continue
				}
				if embeddedHash {
					if e.getHash() == hash && e.key == key {
						return v, true
					}
				} else {
					if e.key == key {
						return v, true
					}
				}
			}
		}
	}
	return
}

// Range calls yield sequentially for each key and value present in the map.
// If yield returns false, Range stops the iteration.
// This operation uses atomicValue for lock-free value access.
func (m *FlatMapOf[K, V]) Range(yield func(K, V) bool) {
	table := (*flatTable[K, V])(atomic.LoadPointer(&m.table))
	for i := 0; i <= table.mask; i++ {
		for b := table.buckets.At(i); b != nil; b = (*flatBucket[K, V])(atomic.LoadPointer(&b.next)) {
			meta := b.meta.Load()
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j)
				if v := e.value.Load(); m.valueIsValid(v) {
					// Pre-read revalidation: ensure the slot is currently
					// published (MSB set) before reading the key to avoid torn
					// key reads.
					cur := b.meta.Load()
					if (uint8(cur>>(uint(j)*8)) & slotMask) == 0 {
						continue
					}
					if !yield(e.key, v) {
						return
					}
				}
			}
		}
	}
}

// Process applies a compute-style update to the map entry for the given key.
// The function fn receives the current value (if any) and whether the key
// exists, and returns the new value, operation type, result value, and status.
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
	table := (*flatTable[K, V])(atomic.LoadPointer(&m.table))
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	for {
		idx := table.mask & h1v
		root := table.buckets.At(idx)
		root.Lock()

		// If a growth/shrink is in progress with both tables set,
		// help finish it
		if rs := (*flatResizeState)(atomic.LoadPointer(&m.resize)); rs != nil &&
			atomic.LoadPointer(&rs.table) != nil &&
			atomic.LoadPointer(&rs.newTable) != nil {
			root.Unlock()
			m.helpCopyAndWait(rs)
			table = (*flatTable[K, V])(atomic.LoadPointer(&m.table))
			continue
		}

		// Verify table wasn't swapped after lock acquisition
		if newTable := (*flatTable[K, V])(atomic.LoadPointer(&m.table)); table != newTable {
			root.Unlock()
			table = newTable
			continue
		}

		var (
			oldB    *flatBucket[K, V]
			oldIdx  int
			oldMeta uint64
			oldVal  V
			loaded  bool
			emptyB  *flatBucket[K, V]
			emptyI  int
			lastB   *flatBucket[K, V]
		)

	findLoop:
		for b := root; b != nil; b = (*flatBucket[K, V])(b.next) {
			meta := *b.meta.Raw()
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j)
				if v := e.value.raw; m.valueIsValid(v) {
					if embeddedHash {
						if e.getHash() == hash && e.key == key {
							oldB, oldIdx, oldMeta, oldVal, loaded = b, j, meta, v, true
							break findLoop
						}
					} else {
						if e.key == key {
							oldB, oldIdx, oldMeta, oldVal, loaded = b, j, meta, v, true
							break findLoop
						}
					}
				}
			}
			if emptyB == nil {
				if empty := (^meta) & metaMask; empty != 0 {
					emptyB = b
					emptyI = firstMarkedByteIndex(empty)
				}
			}
			lastB = b
		}

		newV, op, value, status := fn(oldVal, loaded)
		switch op {
		case DeleteOp:
			if !loaded {
				root.Unlock()
				return value, status
			}
			// Clear the meta slot to mark as deleted (publish-unpublish
			// barrier): readers that still observe the old meta may only ever
			// see the old key; we never clear the key on delete to avoid torn
			// key reads under concurrency.
			newMeta := setByte(oldMeta, emptySlot, oldIdx)
			oldB.meta.Store(newMeta)

			// Clear references: only clear the value to aid GC; keep the old
			// key intact until the slot is reused by a future insert. The old
			// key remains invisible to readers after meta is cleared; when
			// reusing the slot, we (1) store key, (2) store value, then (3) set
			// meta. Keeping the key may retain its backing memory until reuse;
			// this is a conscious trade-off to avoid torn key reads.
			if m.zeroDel {
				oldB.At(oldIdx).value.Store(*new(V))
			}
			root.Unlock()
			table.AddSize(idx, -1)
			return value, status
		case UpdateOp:
			if loaded {
				// Atomically store new value
				oldB.At(oldIdx).value.Store(newV)
				root.Unlock()
				return value, status
			}
			// insert new
			if emptyB != nil {
				entry := emptyB.At(emptyI)
				if embeddedHash {
					entry.setHash(hash)
				}
				entry.key = key
				// Initialize atomicValue with new value
				// (before publishing via meta)
				entry.value.Store(newV)
				// Publish the slot by setting meta last (release)
				newMeta := setByte(*emptyB.meta.Raw(), h2v, emptyI)
				if emptyB == root {
					root.UnlockWithMeta(newMeta)
				} else {
					emptyB.meta.Store(newMeta)
					root.Unlock()
				}

				table.AddSize(idx, 1)
				// Early grow: only consider when the bucket just became full
				// to reduce overhead in single-thread case
				if (idx&1023) == 0 &&
					atomic.LoadPointer(&m.resize) == nil {
					tableLen := table.mask + 1
					size := table.SumSize()
					const sizeHintFactor = float64(entriesPerBucket) * loadFactor
					if size >= int(float64(tableLen)*sizeHintFactor) {
						m.tryResize(mapGrowHint, size, 0)
					}
				}
				return value, status
			}

			// append new bucket
			bucket := &flatBucket[K, V]{
				meta: makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
				entries: [entriesPerBucket]flatEntry[K, V]{
					{
						value: atomicValue[V]{raw: newV},
						key:   key,
					},
				},
			}
			if embeddedHash {
				bucket.At(0).setHash(hash)
			}
			// Publish the new bucket into the chain with a release store so
			// readers see a fully initialized bucket.
			atomic.StorePointer(&lastB.next, unsafe.Pointer(bucket))
			root.Unlock()
			table.AddSize(idx, 1)
			// Auto-grow check (parallel resize)
			if atomic.LoadPointer(&m.resize) == nil {
				tableLen := table.mask + 1
				size := table.SumSize()
				const sizeHintFactor = float64(entriesPerBucket) * loadFactor
				if size >= int(float64(tableLen)*sizeHintFactor) {
					m.tryResize(mapGrowHint, size, 0)
				}
			}
			return value, status
		default:
			// CancelOp
			root.Unlock()
			return value, status
		}
	}
}

// Store sets the value for a key.
func (m *FlatMapOf[K, V]) Store(key K, value V) {
	m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return value, UpdateOp, value, loaded
	})
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
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
	table := (*flatTable[K, V])(atomic.LoadPointer(&m.table))
	return table.SumSize()
}

// IsZero checks if the map is empty.
// This is faster than checking Size() == 0 as it can return early.
//
//go:nosplit
func (m *FlatMapOf[K, V]) IsZero() bool {
	table := (*flatTable[K, V])(atomic.LoadPointer(&m.table))
	return !table.SumSizeExceeds(0)
}

type flatResizeState struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
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
	if !atomic.CompareAndSwapPointer(&m.resize, nil, unsafe.Pointer(rs)) {
		return
	}
	cpus := runtime.GOMAXPROCS(0)
	if hint == mapClearHint {
		newTable := newFlatTable[K, V](m.minLen, cpus)
		atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
		atomic.StorePointer(&m.resize, nil)
		rs.wg.Done()
		return
	}

	table := (*flatTable[K, V])(atomic.LoadPointer(&m.table))
	tableLen := table.mask + 1
	var newLen int
	if hint == mapGrowHint {
		if sizeAdd == 0 {
			newLen = max(calcTableLen(size), tableLen<<1)
		} else {
			newLen = calcTableLen(size + sizeAdd)
			if newLen <= tableLen {
				atomic.StorePointer(&m.resize, nil)
				rs.wg.Done()
				return
			}
		}
	} else { // mapShrinkHint
		if sizeAdd == 0 {
			newLen = tableLen >> 1
		} else {
			newLen = calcTableLen(size)
		}
		if newLen < m.minLen {
			atomic.StorePointer(&m.resize, nil)
			rs.wg.Done()
			return
		}
	}

	if newLen*int(unsafe.Sizeof(flatBucket[K, V]{})) >= asyncThreshold && cpus > 1 {
		go m.finalizeResize(table, newLen, rs, cpus)
	} else {
		m.finalizeResize(table, newLen, rs, cpus)
	}
}

func (m *FlatMapOf[K, V]) finalizeResize(
	table *flatTable[K, V],
	newLen int,
	rs *flatResizeState,
	cpus int,
) {
	atomic.StorePointer(&rs.table, unsafe.Pointer(table))
	newTable := newFlatTable[K, V](newLen, cpus)
	atomic.StorePointer(&rs.newTable, unsafe.Pointer(newTable))
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *FlatMapOf[K, V]) helpCopyAndWait(rs *flatResizeState) {
	table := (*flatTable[K, V])(atomic.LoadPointer(&rs.table))
	tableLen := table.mask + 1
	chunks := int32(table.chunks)
	chunkSz := table.chunkSz
	newTable := (*flatTable[K, V])(atomic.LoadPointer(&rs.newTable))
	for {
		process := atomic.AddInt32(&rs.process, 1)
		if process > chunks {
			// Wait copying completed
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSz
		end := min(start+chunkSz, tableLen)
		m.copyFlatBucketRange(table, start, end, newTable)
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			// Copying completed
			atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
			atomic.StorePointer(&m.resize, nil)
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
	var hash uintptr
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*flatBucket[K, V])(b.next) {
			meta := *b.meta.Raw()
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				e := b.At(firstMarkedByteIndex(marked))
				if v := e.value.raw; m.valueIsValid(v) {
					if embeddedHash {
						hash = e.getHash()
					} else {
						hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
					}
					idx := newTable.mask & h1(hash, m.intKey)
					destBucket := newTable.buckets.At(idx)
					h2v := h2(hash)

					b := destBucket
				appendToBucket:
					for {
						meta := *b.meta.Raw()
						empty := (^meta) & metaMask
						if empty != 0 {
							emptyIdx := firstMarkedByteIndex(empty)
							// It is safe to set meta first here because
							// newTable is not published to readers until the
							// final atomic swap of m.table in helpCopyAndWait;
							// no reader can observe dest bucket before that point.
							*b.meta.Raw() = setByte(meta, h2v, emptyIdx)
							entry := b.At(emptyIdx)
							entry.value.raw = v
							if embeddedHash {
								entry.setHash(hash)
							}
							entry.key = e.key
							break appendToBucket
						}
						next := (*flatBucket[K, V])(b.next)
						if next == nil {
							bucket := &flatBucket[K, V]{
								meta: makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
								entries: [entriesPerBucket]flatEntry[K, V]{
									{
										value: atomicValue[V]{raw: v},
										key:   e.key,
									},
								},
							}
							if embeddedHash {
								bucket.At(0).setHash(hash)
							}
							// Safe for the same reason as above: newTable is
							// private until published.
							b.next = unsafe.Pointer(bucket)
							break appendToBucket
						}
						b = next
					}
					copied++
				}
			}
		}
		srcBucket.Unlock()
	}
	if copied != 0 {
		// copyFlatBucketRange is used during multithreaded growth; increment
		// size via striped counter.
		newTable.AddSize(start, copied)
	}
}
