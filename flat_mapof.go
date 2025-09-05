package pb

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// FlatMapOf implements a minimal double-buffered flat-map variant
// optimized for small K/V.
//
// Design goals:
//   - Read path is lock-free: readers use meta (h2 byte) to locate slot,
//     then a per-slot version byte to choose the active buffer (A/B).
//     If the version byte changes while reading, reader retries.
//   - Write path uses the root bucket lock (same granularity as MapOf)
//     to keep implementation simple and safe.
//   - Buckets store keys and two value buffers inline to avoid pointer chasing.
//
// Comparison with MapOf:
// Advantages:
//   - More GC-friendly: inline storage reduces pointer indirection and
//     heap allocations
//   - Better memory locality: contiguous data layout improves cache performance
//
// Trade-offs:
//   - Higher memory usage per bucket due to dual value buffers
//   - No shrinking support (WithShrinkEnabled not available)
//   - Optimized for small K/V pairs; may be less efficient for large values
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

// flatBucketOf stores inline keys and two value buffers for each slot.
// vers packs one byte per slot (aligned with meta byte order). LSB of that byte
// indicates which buffer is active: 0 -> A, 1 -> B. Writers increment that byte
// to toggle buffers. Readers verify the byte before/after to detect changes.
//
// The bucket lock is encoded in meta's op byte (same layout/constants as MapOf).
// Only the root bucket lock is used on write path.
type flatBucketOf[K comparable, V any] struct {
	_    [0]int64 // keep same alignment trick
	meta uint64
	vers uint64 // 1 byte per slot (wrap-around allowed)
	// Optional cacheline pad (kept minimal to reduce verbosity in this MVP)
	_keys  [entriesPerMapOfBucket]K
	_valsA [entriesPerMapOfBucket]V
	_valsB [entriesPerMapOfBucket]V
	next   unsafe.Pointer // *flatBucketOf[K,V]
}

//go:nosplit
func (b *flatBucketOf[K, V]) Key(i int) *K {
	return (*K)(unsafe.Add(
		unsafe.Pointer(&b._keys),
		uintptr(i)*unsafe.Sizeof(*new(K)),
	))
}

//go:nosplit
func (b *flatBucketOf[K, V]) ValA(i int) *V {
	return (*V)(unsafe.Add(
		unsafe.Pointer(&b._valsA),
		uintptr(i)*unsafe.Sizeof(*new(V)),
	))
}

//go:nosplit
func (b *flatBucketOf[K, V]) ValB(i int) *V {
	return (*V)(unsafe.Add(
		unsafe.Pointer(&b._valsB),
		uintptr(i)*unsafe.Sizeof(*new(V)),
	))
}

//go:nosplit
func (b *flatBucketOf[K, V]) Lock() {
	cur := loadUint64(&b.meta)
	if atomic.CompareAndSwapUint64(&b.meta, cur&(^opLockMask), cur|opLockMask) {
		return
	}
	b.slowLock()
}

//go:nosplit
func (b *flatBucketOf[K, V]) slowLock() {
	spins := 0
	for !b.tryLock() {
		delay(&spins)
	}
}

//go:nosplit
func (b *flatBucketOf[K, V]) tryLock() bool {
	for {
		cur := loadUint64(&b.meta)
		if cur&opLockMask != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64(&b.meta, cur, cur|opLockMask) {
			return true
		}
	}
}

//go:nosplit
func (b *flatBucketOf[K, V]) Unlock() {
	atomic.StoreUint64(&b.meta, b.meta&(^opLockMask))
}

//go:nosplit
func (b *flatBucketOf[K, V]) UnlockWithMeta(meta uint64) {
	atomic.StoreUint64(&b.meta, meta&(^opLockMask))
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
// This operation is lock-free and uses versioned double buffering for consistency.
//
//go:nosplit
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
				shift := idx << 3
				for {
					ver1 := loadUint64(&b.vers)
					vb1 := uint8(ver1 >> shift)
					if (vb1 & 1) == 0 {
						value = *b.ValA(idx)
					} else {
						value = *b.ValB(idx)
					}
					ver2 := loadUint64(&b.vers)
					if vb1 == uint8(ver2>>shift) {
						return value, true
					}
				}
			}
		}
	}
	return
}

// Range calls yield sequentially for each key and value present in the map.
// If yield returns false, Range stops the iteration.
// This operation uses versioned double buffering to ensure consistency during iteration.
func (m *FlatMapOf[K, V]) Range(yield func(K, V) bool) {
	table := (*flatTable[K, V])(loadPointer(&m.table))
	for i := 0; i <= table.bucketsMask; i++ {
		for b := table.buckets.At(i); b != nil; b = (*flatBucketOf[K, V])(loadPointer(&b.next)) {
			metaw := loadUint64(&b.meta)
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				key := *b.Key(idx)
				shift := idx << 3
				var value V
				for {
					ver1 := loadUint64(&b.vers)
					vb1 := uint8(ver1 >> shift)
					if (vb1 & 1) == 0 {
						value = *b.ValA(idx)
					} else {
						value = *b.ValB(idx)
					}
					ver2 := loadUint64(&b.vers)
					if vb1 == uint8(ver2>>shift) {
						break
					}
				}
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
					shift := idx << 3
					vb := uint8(b.vers >> shift)
					if (vb & 1) == 0 {
						oldVal = *b.ValA(idx)
					} else {
						oldVal = *b.ValB(idx)
					}
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
			// 1) Write oldVal into the inactive buffer so after version flip
			//    readers still see a consistent value
			verAll := oldB.vers
			shift := oldIdx << 3
			vb := uint8(verAll >> shift)
			if (vb & 1) == 0 {
				// A is active, prepare B
				*oldB.ValB(oldIdx) = oldVal
			} else {
				// B is active, prepare A
				*oldB.ValA(oldIdx) = oldVal
			}
			// 2) Bump version to invalidate in-flight readers
			//    and flip active buffer
			mask := uint64(0xff) << shift
			storeUint64(&oldB.vers, (verAll&^mask)|(uint64(vb+1)<<shift))
			// 3) Publish delete by clearing the meta slot first (under root lock)
			newmetaw := setByte(oldMeta, emptyMetaSlot, oldIdx)
			// 4) Drop references and release lock
			// Publish meta to non-root bucket, then clear refs and unlock root
			storeUint64(&oldB.meta, newmetaw)
			*oldB.Key(oldIdx) = *new(K)
			*oldB.ValA(oldIdx) = *new(V)
			*oldB.ValB(oldIdx) = *new(V)
			rootB.Unlock()
			table.AddSize(bidx, -1)
			return value, status
		case UpdateOp:
			if loaded {
				// write to inactive buffer then toggle version byte
				verAll := oldB.vers
				shift := oldIdx << 3
				vb := uint8(verAll >> shift)
				if (vb & 1) == 0 {
					*oldB.ValB(oldIdx) = newV
				} else {
					*oldB.ValA(oldIdx) = newV
				}
				// increment version byte
				mask := uint64(0xff) << shift
				storeUint64(&oldB.vers, verAll&^mask|(uint64(vb+1)<<shift))
				// clear previously active buffer to release references
				if (vb & 1) == 0 {
					// previously active was A
					*oldB.ValA(oldIdx) = *new(V)
				} else {
					// previously active was B
					*oldB.ValB(oldIdx) = *new(V)
				}
				rootB.Unlock()
				return value, status
			}
			// insert new
			if emptyB != nil {
				*emptyB.Key(emptyIdx) = key
				// initialize A buffer and set version byte to even (A active)
				*emptyB.ValA(emptyIdx) = newV
				verAll := emptyB.vers
				shift := emptyIdx << 3
				mask := uint64(0xff) << shift
				// verAll = verAll &^ mask | (uint64(0) << shift)
				storeUint64(&emptyB.vers, verAll&^mask)
				// publish meta
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
			storePointer(&lastB.next, unsafe.Pointer(&flatBucketOf[K, V]{
				meta:   setByte(emptyMeta, h2v, 0),
				_keys:  [entriesPerMapOfBucket]K{key},
				_valsA: [entriesPerMapOfBucket]V{newV},
			}))
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
				shift := idx << 3
				ver := uint8(b.vers >> shift)
				var v V
				if (ver & 1) == 0 {
					v = *b.ValA(idx)
				} else {
					v = *b.ValB(idx)
				}
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
						destb.meta = setByte(metaw, h2v, emptyIdx)
						*destb.Key(emptyIdx) = k
						*destb.ValA(emptyIdx) = v
						break appendToBucket
					}
					next := (*flatBucketOf[K, V])(destb.next)
					if next == nil {
						destb.next = unsafe.Pointer(&flatBucketOf[K, V]{
							meta:   setByte(emptyMeta, h2v, 0),
							_keys:  [entriesPerMapOfBucket]K{k},
							_valsA: [entriesPerMapOfBucket]V{v},
						})
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
		// copyFlatBucketRange is used during multithreaded growth, requiring a
		// thread-safe AddSize.
		newTable.AddSize(start, copied)
	}
}
