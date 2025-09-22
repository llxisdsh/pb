package pb

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// SeqFlatMapOf implements a flat hash map using bucket-level seqlock.
// Keys and values are stored inline (flat). Values are plain (no atomicValue),
// so V is not limited by CPU word size. Consistency for readers is guaranteed
// by per-bucket seqlock (sequence is even when stable; writers make it odd
// during mutations and then even again).
// API mirrors FlatMapOf as much as practical.
//
// Concurrency model:
//   - Readers: per-bucket seqlock read: s1=seq (must be even), read meta/entries,
//     s2=seq; if s1!=s2 or s1 odd, retry this bucket.
//   - Writers: acquire root bucket lock (opLock in meta), then for the bucket
//     being modified: seq++ (odd), apply changes, seq++ (even), finally release
//     root lock.
//   - Resize: same approach as FlatMapOf; copy under root bucket lock.
//
// EXPERIMENTAL: this implementation is experimental; APIs and
// concurrency semantics may evolve.
type SeqFlatMapOf[K comparable, V any] struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		_       noCopy
		table   seqFlatTable[K, V]
		resize  unsafe.Pointer
		seed    uintptr
		keyHash HashFunc
		intKey  bool
	}{})%CacheLineSize) % CacheLineSize]byte

	_       noCopy
	table   seqFlatTable[K, V]
	resize  unsafe.Pointer // *seqFlatResizeState[K,V]
	seed    uintptr
	keyHash HashFunc
	intKey  bool
}

type seqFlatResizeState[K comparable, V any] struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		newTable  seqFlatTable[K, V]
		chunks    int
		process   int32
		completed int32
		wg        sync.WaitGroup
	}{})%CacheLineSize) % CacheLineSize]byte

	newTable  seqFlatTable[K, V]
	chunks    int
	process   int32 // atomic
	completed int32 // atomic
	wg        sync.WaitGroup
}

type seqFlatTable[K comparable, V any] struct {
	buckets  unsafeSlice[seqFlatBucket[K, V]]
	mask     int
	size     unsafeSlice[counterStripe]
	sizeMask uint32
	seq      uint32 // seqlock of table
	// The inline size has minimal effect on reducing cache misses,
	// so we will not use it for now.
	// smallSz  uintptr
}

type seqFlatBucket[K comparable, V any] struct {
	meta    atomicUint64   // op byte + h2 bytes
	seq     atomicUint64   // seqlock of bucket (even=stable, odd=write)
	next    unsafe.Pointer // *seqFlatBucket[K,V]
	entries [entriesPerBucket]seqFlatEntry[K, V]
}

// NewSeqFlatMapOf creates a new seqlock-based flat map.
func NewSeqFlatMapOf[K comparable, V any](
	options ...func(*MapConfig),
) *SeqFlatMapOf[K, V] {
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

	m := &SeqFlatMapOf[K, V]{}
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
			}
		}
	}
	minLen := calcTableLen(cfg.SizeHint)
	m.table.makeTable(minLen, runtime.GOMAXPROCS(0))
	return m
}

func (t *seqFlatTable[K, V]) makeTable(
	tableLen, cpus int,
) {
	b := make([]seqFlatBucket[K, V], tableLen)
	sizeLen := calcSizeLen(tableLen, cpus)
	t.buckets = makeUnsafeSlice(b)
	t.mask = tableLen - 1
	//if sizeLen <= 1 {
	//	t.size.ptr = unsafe.Pointer(&t.smallSz)
	//} else {
	t.size = makeUnsafeSlice(make([]counterStripe, sizeLen))
	//}
	t.sizeMask = uint32(sizeLen - 1)
}

//go:nosplit
func (t *seqFlatTable[K, V]) SeqLoad() seqFlatTable[K, V] {
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

func (t *seqFlatTable[K, V]) SeqStore(v *seqFlatTable[K, V]) {
	s := atomic.LoadUint32(&t.seq)
	atomic.StoreUint32(&t.seq, s+1)
	t.buckets = v.buckets
	t.mask = v.mask
	t.size = v.size
	t.sizeMask = v.sizeMask
	atomic.StoreUint32(&t.seq, s+2)
}

//go:nosplit
func (t *seqFlatTable[K, V]) SeqInitDone() bool {
	return atomic.LoadUint32(&t.seq) == 2
}

//go:nosplit
func (t *seqFlatTable[K, V]) AddSize(idx, delta int) {
	atomic.AddUintptr(&t.size.At(int(t.sizeMask)&idx).c, uintptr(delta))
}

//go:nosplit
func (t *seqFlatTable[K, V]) SumSize() int {
	var sum uintptr
	for i := 0; i <= int(t.sizeMask); i++ {
		sum += atomic.LoadUintptr(&t.size.At(i).c)
	}
	return int(sum)
}

//go:nosplit
func (t *seqFlatTable[K, V]) SumSizeExceeds(limit int) bool {
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
func (b *seqFlatBucket[K, V]) At(i int) *seqFlatEntry[K, V] {
	return (*seqFlatEntry[K, V])(unsafe.Add(
		unsafe.Pointer(&b.entries),
		uintptr(i)*unsafe.Sizeof(seqFlatEntry[K, V]{}),
	))
}

//go:nosplit
func (b *seqFlatBucket[K, V]) Lock() {
	cur := b.meta.Load()
	if b.meta.CompareAndSwap(cur&(^opLockMask), cur|opLockMask) {
		return
	}
	b.slowLock()
}

//go:nosplit
func (b *seqFlatBucket[K, V]) slowLock() {
	spins := 0
	for !b.tryLock() {
		delay(&spins)
	}
}

//go:nosplit
func (b *seqFlatBucket[K, V]) tryLock() bool {
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
func (b *seqFlatBucket[K, V]) Unlock() {
	b.meta.Store(*b.meta.Raw() &^ opLockMask)
}

//go:nosplit
func (b *seqFlatBucket[K, V]) UnlockWithMeta(
	meta uint64,
) {
	b.meta.Store(meta &^ opLockMask)
}

// Load with per-bucket seqlock read
func (m *SeqFlatMapOf[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.SeqLoad()
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1(hash, m.intKey)
	root := table.buckets.At(idx)
	for b := root; b != nil; b = (*seqFlatBucket[K, V])(atomic.LoadPointer(&b.next)) {
		spins := 0
	retry:
		s1 := b.seq.Load()
		if (s1 & 1) == 0 {
			meta := b.meta.Load()
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := *b.At(j) // copy entry after seq check
				s2 := b.seq.Load()
				if s1 == s2 {
					if embeddedHash {
						if e.getHash() == hash && e.key == key {
							return e.value, true
						}
					} else {
						if e.key == key {
							return e.value, true
						}
					}
				} else {
					if trySpin(&spins) {
						goto retry
					}
					goto fallback
				}
			}
		} else {
			// writer in progress
			if trySpin(&spins) {
				goto retry
			}
			goto fallback
		}
	}
	return

fallback:
	// fallback: find entry under lock
	root.Lock()
	for b := root; b != nil; b = (*seqFlatBucket[K, V])(b.next) {
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
func (m *SeqFlatMapOf[K, V]) Range(yield func(K, V) bool) {
	var s1, s2 uint64
	var meta uint64
	// Reusable cache, to avoid reading entry fields after s2 check
	type kvEntry struct {
		k K
		v V
	}
	var cache [entriesPerBucket]kvEntry
	var cacheCount int
	table := m.table.SeqLoad()
	for i := 0; i <= table.mask; i++ {
		root := table.buckets.At(i)
		for b := root; b != nil; b = (*seqFlatBucket[K, V])(atomic.LoadPointer(&b.next)) {
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
					for j := 0; j < cacheCount; j++ {
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
				for j := 0; j < cacheCount; j++ {
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

// RangeProcess iterates over all key-value pairs and applies a function to
// each. The function can return UpdateOp to modify the value, DeleteOp to
// remove the entry, or CancelOp to leave it unchanged. Uses seqlock for
// consistency during updates.
//
// Parameters:
//   - fn: callback that processes each entry and returns new value and
//     operation
//
// Notes:
//   - Holds bucket lock for entire iteration - avoid long operations
//   - Blocks concurrent map operations during execution
func (m *SeqFlatMapOf[K, V]) RangeProcess(
	fn func(key K, value V) (V, ComputeOp),
) {
restart:
	table := m.table.SeqLoad()
	for i := 0; i <= table.mask; i++ {
		root := table.buckets.At(i)
		root.Lock()

		// Check if resize is in progress and help complete the copy
		if rs := (*seqFlatResizeState[K, V])(atomic.LoadPointer(&m.resize)); rs != nil &&
			rs.newTable.SeqInitDone() {
			root.Unlock()
			m.helpCopyAndWait(rs)
			goto restart
		}
		// Check if table has been swapped during resize
		if atomic.LoadUint32(&m.table.seq) != table.seq {
			root.Unlock()
			goto restart
		}

		for b := root; b != nil; b = (*seqFlatBucket[K, V])(b.next) {
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
					*e = seqFlatEntry[K, V]{}
					table.AddSize(i, -1)
				default:
					root.Unlock()
					panic("unexpected op")
				}
			}
		}
		root.Unlock()
	}
}

// Process applies a compute-style update with root bucket lock + per-bucket
// seq fencing.
func (m *SeqFlatMapOf[K, V]) Process(
	key K,
	fn func(old V, loaded bool) (V, ComputeOp, V, bool),
) (V, bool) {
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	for {
		table := m.table.SeqLoad()
		idx := table.mask & h1v
		root := table.buckets.At(idx)
		root.Lock()

		// help finishing resize if needed
		if rs := (*seqFlatResizeState[K, V])(atomic.LoadPointer(&m.resize)); rs != nil &&
			rs.newTable.SeqInitDone() {
			root.Unlock()
			m.helpCopyAndWait(rs)
			continue
		}
		if atomic.LoadUint32(&m.table.seq) != table.seq {
			root.Unlock()
			continue
		}

		var (
			oldB     *seqFlatBucket[K, V]
			oldIdx   int
			oldMeta  uint64
			oldVal   V
			loaded   bool
			emptyB   *seqFlatBucket[K, V]
			emptyIdx int
			lastB    *seqFlatBucket[K, V]
		)

	findLoop:
		for b := root; b != nil; b = (*seqFlatBucket[K, V])(b.next) {
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
			bucket := &seqFlatBucket[K, V]{
				meta: makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
				entries: [entriesPerBucket]seqFlatEntry[K, V]{
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
			if atomic.LoadPointer(&m.resize) == nil {
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
			*oldB.At(oldIdx) = seqFlatEntry[K, V]{}
			root.Unlock()
			table.AddSize(idx, -1)
			return value, status
		default:
			root.Unlock()
			panic("unexpected op")
		}
	}
}

// Store sets the value for a key.
func (m *SeqFlatMapOf[K, V]) Store(key K, value V) {
	m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return value, UpdateOp, value, loaded
	})
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *SeqFlatMapOf[K, V]) LoadOrStore(
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
func (m *SeqFlatMapOf[K, V]) LoadOrStoreFn(
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
func (m *SeqFlatMapOf[K, V]) Delete(key K) {
	m.Process(key, func(old V, loaded bool) (V, ComputeOp, V, bool) {
		return old, DeleteOp, old, loaded
	})
}

// All returns an iterator function for use with range-over-func.
// It provides the same functionality as Range but in iterator form.
//
//go:nosplit
func (m *SeqFlatMapOf[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the number of key-value pairs in the map.
// This operation sums counters across all size stripes for an approximate
// count.
//
//go:nosplit
func (m *SeqFlatMapOf[K, V]) Size() int {
	table := m.table.SeqLoad()
	return table.SumSize()
}

// IsZero checks if the map is empty.
// This is faster than checking Size() == 0 as it can return early.
//
//go:nosplit
func (m *SeqFlatMapOf[K, V]) IsZero() bool {
	table := m.table.SeqLoad()
	return !table.SumSizeExceeds(0)
}

//go:noinline
func (m *SeqFlatMapOf[K, V]) tryResize(hint mapResizeHint, size, sizeAdd int) {
	rs := new(seqFlatResizeState[K, V])
	rs.wg.Add(1)
	if !atomic.CompareAndSwapPointer(&m.resize, nil, unsafe.Pointer(rs)) {
		return
	}
	cpus := runtime.GOMAXPROCS(0)
	if hint == mapClearHint {
		var newTable seqFlatTable[K, V]
		newTable.makeTable(minTableLen, cpus)
		m.table.SeqStore(&newTable)
		atomic.StorePointer(&m.resize, nil)
		rs.wg.Done()
		return
	}
	table := &m.table
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
	} else { // shrink
		if sizeAdd == 0 {
			newLen = tableLen >> 1
		} else {
			newLen = calcTableLen(size)
		}
		if newLen < minTableLen {
			atomic.StorePointer(&m.resize, nil)
			rs.wg.Done()
			return
		}
	}

	if cpus > 1 &&
		newLen*int(unsafe.Sizeof(seqFlatBucket[K, V]{})) >= asyncThreshold {
		go m.finalizeResize(table, newLen, rs, cpus)
	} else {
		m.finalizeResize(table, newLen, rs, cpus)
	}
}

func (m *SeqFlatMapOf[K, V]) finalizeResize(
	table *seqFlatTable[K, V],
	newLen int,
	rs *seqFlatResizeState[K, V],
	cpus int,
) {
	overCpus := cpus * resizeOverPartition
	_, rs.chunks = calcParallelism(table.mask+1, minBucketsPerCPU, overCpus)
	newTable := new(seqFlatTable[K, V])
	newTable.makeTable(newLen, cpus)
	// Release rs
	rs.newTable.SeqStore(newTable)
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *SeqFlatMapOf[K, V]) helpCopyAndWait(rs *seqFlatResizeState[K, V]) {
	table := &m.table
	tableLen := table.mask + 1
	// Acquire rs
	newTable := rs.newTable.SeqLoad()
	chunks := int32(rs.chunks)
	chunkSz := (tableLen + rs.chunks - 1) / rs.chunks
	for {
		process := atomic.AddInt32(&rs.process, 1)
		if process > chunks {
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSz
		end := min(start+chunkSz, tableLen)
		m.copySeqFlatBucketRange(table, start, end, &newTable)
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			m.table.SeqStore(&newTable)
			atomic.StorePointer(&m.resize, nil)
			rs.wg.Done()
			return
		}
	}
}

func (m *SeqFlatMapOf[K, V]) copySeqFlatBucketRange(
	table *seqFlatTable[K, V],
	start, end int,
	newTable *seqFlatTable[K, V],
) {
	copied := 0
	var hash uintptr
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*seqFlatBucket[K, V])(b.next) {
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
					next := (*seqFlatBucket[K, V])(b.next)
					if next == nil {
						bucket := &seqFlatBucket[K, V]{
							meta:    makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
							entries: [entriesPerBucket]seqFlatEntry[K, V]{{value: e.value, key: e.key}},
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

//go:nosplit
func trySpin(spins *int) bool {
	if runtime_canSpin(*spins) {
		*spins++
		runtime_doSpin()
		return true
	}
	return false
}
