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
// WithZeroAsDeleted is supported like FlatMapOf.
type SeqFlatMapOf[K comparable, V comparable] struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		_             noCopy
		table         unsafe.Pointer
		resizeState   unsafe.Pointer
		seed          uintptr
		keyHash       HashFunc
		minTableLen   int
		intKey        bool
		zeroAsDeleted bool
	}{})%CacheLineSize) % CacheLineSize]byte

	_             noCopy
	table         unsafe.Pointer // *seqFlatTable[K,V]
	resizeState   unsafe.Pointer // *seqFlatResizeState
	seed          uintptr
	keyHash       HashFunc
	minTableLen   int
	intKey        bool
	zeroAsDeleted bool
}

// NewSeqFlatMapOf creates a new seqlock-based flat map.
func NewSeqFlatMapOf[K comparable, V comparable](
	options ...func(*MapConfig),
) *SeqFlatMapOf[K, V] {
	var cfg MapConfig
	for _, opt := range options {
		opt(&cfg)
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
	m.minTableLen = calcTableLen(cfg.SizeHint)
	m.zeroAsDeleted = cfg.zeroAsDeleted

	t := newSeqFlatTable[K, V](m.minTableLen, runtime.GOMAXPROCS(0))
	atomic.StorePointer(&m.table, unsafe.Pointer(t))
	return m
}

type seqFlatTable[K comparable, V comparable] struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		buckets     unsafeSlice[seqFlatBucket[K, V]]
		bucketsMask int
		size        unsafeSlice[counterStripe]
		sizeMask    int
		chunks      int
		chunkSize   int
	}{})%CacheLineSize) % CacheLineSize]byte

	buckets     unsafeSlice[seqFlatBucket[K, V]]
	bucketsMask int
	size        unsafeSlice[counterStripe]
	sizeMask    int
	chunks      int
	chunkSize   int
}

func newSeqFlatTable[K comparable, V comparable](
	tableLen, cpus int,
) *seqFlatTable[K, V] {
	b := make([]seqFlatBucket[K, V], tableLen)
	chunkSize, chunks := calcParallelism(tableLen, minBucketsPerGoroutine, cpus)
	sizeLen := calcSizeLen(tableLen, cpus)
	return &seqFlatTable[K, V]{
		buckets:     makeUnsafeSlice(b),
		bucketsMask: tableLen - 1,
		size:        makeUnsafeSlice(make([]counterStripe, sizeLen)),
		sizeMask:    sizeLen - 1,
		chunks:      chunks,
		chunkSize:   chunkSize,
	}
}

//go:nosplit
func (t *seqFlatTable[K, V]) AddSize(bidx, delta int) {
	atomic.AddUintptr(&t.size.At(t.sizeMask&bidx).c, uintptr(delta))
}

//go:nosplit
func (t *seqFlatTable[K, V]) SumSize() int {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += atomic.LoadUintptr(&t.size.At(i).c)
	}
	return int(sum)
}

//go:nosplit
func (t *seqFlatTable[K, V]) SumSizeExceeds(limit int) bool {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += atomic.LoadUintptr(&t.size.At(i).c)
		if int(sum) > limit {
			return true
		}
	}
	return false
}

type seqFlatBucket[K comparable, V comparable] struct {
	_       [0]int64
	meta    atomicUint64 // occupancy + h2 bytes + op lock bit
	seq     atomicUint64 // seqlock sequence (even=stable, odd=write)
	entries [entriesPerMapOfBucket]seqFlatEntry[K, V]
	next    unsafe.Pointer // *seqFlatBucket[K,V]
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
func (b *seqFlatBucket[K, V]) Unlock() { b.meta.Store(*b.meta.Raw() &^ opLockMask) }

//go:nosplit
func (b *seqFlatBucket[K, V]) UnlockWithMeta(
	meta uint64,
) {
	b.meta.Store(meta & (^opLockMask))
}

//go:nosplit
func (m *SeqFlatMapOf[K, V]) valueIsValid(v V) bool {
	return v != *new(V) || !m.zeroAsDeleted
}

// Load with per-bucket seqlock read
func (m *SeqFlatMapOf[K, V]) Load(key K) (value V, ok bool) {
	table := (*seqFlatTable[K, V])(atomic.LoadPointer(&m.table))
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	bidx := table.bucketsMask & h1(hash, m.intKey)
	rootB := table.buckets.At(bidx)
	for b := rootB; b != nil; b = (*seqFlatBucket[K, V])(atomic.LoadPointer(&b.next)) {
		spins := 0
		for {
			s1 := b.seq.Load()
			if (s1 & 1) != 0 { // writer in progress
				if !trySpin(&spins) {
					goto fallback
				}
			}
			metaw := b.meta.Load()
			for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				e := b.At(idx)
				v := e.value
				if embeddedHash {
					if e.getHash() == hash && e.key == key {
						s2 := b.seq.Load()
						if s1 == s2 && (s2&1) == 0 {
							if m.valueIsValid(v) {
								return v, true
							}
							return value, false
						}
						break // retry bucket
					}
				} else {
					if e.key == key {
						s2 := b.seq.Load()
						if s1 == s2 && (s2&1) == 0 {
							if m.valueIsValid(v) {
								return v, true
							}
							return value, false
						}
						break // retry bucket
					}
				}
			}
			s2 := b.seq.Load()
			if s1 == s2 && (s2&1) == 0 {
				break
			}
			if !trySpin(&spins) {
				goto fallback
			}
		}
	}
	return

fallback:
	rootB.Lock()
	for b := rootB; b != nil; b = (*seqFlatBucket[K, V])(atomic.LoadPointer(&b.next)) {
		metaw := *b.meta.Raw()
		for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			e := b.At(idx)
			v := e.value
			if embeddedHash {
				if e.getHash() == hash && e.key == key {
					rootB.Unlock()
					if m.valueIsValid(v) {
						return v, true
					}
					return
				}
			} else {
				if e.key == key {
					rootB.Unlock()
					if m.valueIsValid(v) {
						return v, true
					}
					return
				}
			}
		}
	}
	rootB.Unlock()
	return
}

// Range iterates all entries using per-bucket seqlock reads.
func (m *SeqFlatMapOf[K, V]) Range(yield func(K, V) bool) {
	table := (*seqFlatTable[K, V])(atomic.LoadPointer(&m.table))
	for i := 0; i <= table.bucketsMask; i++ {
		rootB := table.buckets.At(i)
		for b := rootB; b != nil; b = (*seqFlatBucket[K, V])(atomic.LoadPointer(&b.next)) {
			spins := 0
			for {
				s1 := b.seq.Load()
				if (s1 & 1) != 0 {
					if trySpin(&spins) {
						continue
					}
					// fallback snapshot for this bucket (consistent, no duplicates)
					rootB.Lock()
					spairs := m.snapshotBucketLocked(b)
					rootB.Unlock()
					for _, p := range spairs {
						if !yield(p.k, p.v) {
							return
						}
					}
					break
				}
				metaw := b.meta.Load()
				// Buffer only indices first; yield after s2 verification to avoid duplicates and extra copies
				var tmpIdx [entriesPerMapOfBucket]uint8
				idxs := tmpIdx[:0]
				for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
					idx := firstMarkedByteIndex(markedw)
					e := b.At(idx)
					v := e.value
					if m.valueIsValid(v) {
						idxs = append(idxs, uint8(idx))
					}
				}
				s2 := b.seq.Load()
				if s1 == s2 && (s2&1) == 0 {
					for _, bi := range idxs {
						ei := int(bi)
						e := b.At(ei)
						v := e.value
						if m.valueIsValid(v) {
							if !yield(e.key, v) {
								return
							}
						}
					}
					break
				}
				if trySpin(&spins) {
					continue
				}
				// fallback snapshot for this bucket (consistent, no duplicates)
				rootB.Lock()
				spairs := m.snapshotBucketLocked(b)
				rootB.Unlock()
				for _, p := range spairs {
					if !yield(p.k, p.v) {
						return
					}
				}
				break
			}
		}
	}
}

// Process applies a compute-style update with root bucket lock + per-bucket
// seq fencing.
func (m *SeqFlatMapOf[K, V]) Process(
	key K,
	fn func(old V, loaded bool) (V, ComputeOp, V, bool),
) (V, bool) {
	table := (*seqFlatTable[K, V])(atomic.LoadPointer(&m.table))
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	for {
		bidx := table.bucketsMask & h1v
		rootB := table.buckets.At(bidx)
		rootB.Lock()

		// help finishing resize if needed
		if rs := (*seqFlatResizeState)(atomic.LoadPointer(&m.resizeState)); rs != nil &&
			atomic.LoadPointer(&rs.table) != nil &&
			atomic.LoadPointer(&rs.newTable) != nil {
			rootB.Unlock()
			m.helpCopyAndWait(rs)
			table = (*seqFlatTable[K, V])(atomic.LoadPointer(&m.table))
			continue
		}
		if newTable := (*seqFlatTable[K, V])(atomic.LoadPointer(&m.table)); newTable != table {
			rootB.Unlock()
			table = newTable
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
		for b := rootB; b != nil; b = (*seqFlatBucket[K, V])(b.next) {
			metaw := *b.meta.Raw()
			for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				e := b.At(idx)
				v := e.value
				if m.valueIsValid(v) {
					if embeddedHash {
						if e.getHash() == hash && e.key == key {
							oldB, oldIdx, oldMeta, oldVal, loaded = b, idx, metaw, v, true
							break findLoop
						}
					} else {
						if e.key == key {
							oldB, oldIdx, oldMeta, oldVal, loaded = b, idx, metaw, v, true
							break findLoop
						}
					}
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
			// start seqlock for the bucket we modify
			s := oldB.seq.Load()
			oldB.seq.Store(s + 1)
			newmetaw := setByte(oldMeta, emptyMetaSlot, oldIdx)
			oldB.meta.Store(newmetaw)
			if m.zeroAsDeleted {
				oldB.At(oldIdx).value = *new(V)
			}
			oldB.seq.Store(s + 2)
			rootB.Unlock()
			table.AddSize(bidx, -1)
			return value, status
		case UpdateOp:
			if loaded {
				s := oldB.seq.Load()
				oldB.seq.Store(s + 1)
				oldB.At(oldIdx).value = newV
				oldB.seq.Store(s + 2)
				rootB.Unlock()
				return value, status
			}
			// insert new
			if emptyB != nil {
				s := emptyB.seq.Load()
				emptyB.seq.Store(s + 1)
				emptyEntry := emptyB.At(emptyIdx)
				if embeddedHash {
					emptyEntry.setHash(hash)
				}
				emptyEntry.key = key
				emptyEntry.value = newV
				newMeta := setByte(*emptyB.meta.Raw(), h2v, emptyIdx)
				if emptyB == rootB {
					rootB.UnlockWithMeta(newMeta)
				} else {
					emptyB.meta.Store(newMeta)
					rootB.Unlock()
				}
				emptyB.seq.Store(s + 2)
				table.AddSize(bidx, 1)
				// Early grow: only consider when the bucket just became full
				// to reduce overhead in single-thread case
				if (bidx&1023) == 0 &&
					atomic.LoadPointer(&m.resizeState) == nil {
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
			newBucket := &seqFlatBucket[K, V]{
				meta: makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
				entries: [entriesPerMapOfBucket]seqFlatEntry[K, V]{
					{key: key, value: newV},
				},
			}
			if embeddedHash {
				newBucket.At(0).setHash(hash)
			}
			atomic.StorePointer(&lastB.next, unsafe.Pointer(newBucket))
			rootB.Unlock()
			table.AddSize(bidx, 1)
			// Auto-grow check (parallel resize)
			if atomic.LoadPointer(&m.resizeState) == nil {
				tableLen := table.bucketsMask + 1
				size := table.SumSize()
				const sizeHintFactor = float64(entriesPerMapOfBucket) * mapLoadFactor
				if size >= int(float64(tableLen)*sizeHintFactor) {
					m.tryResize(mapGrowHint, size, 0)
				}
			}
			return value, status
		default:
			rootB.Unlock()
			return value, status
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
func (m *SeqFlatMapOf[K, V]) All() func(yield func(K, V) bool) { return m.Range }

// Size returns the number of key-value pairs in the map.
// This operation sums counters across all size stripes for an approximate
// count.
//
//go:nosplit
func (m *SeqFlatMapOf[K, V]) Size() int {
	return (*seqFlatTable[K, V])(atomic.LoadPointer(&m.table)).SumSize()
}

// IsZero checks if the map is empty.
// This is faster than checking Size() == 0 as it can return early.
//
//go:nosplit
func (m *SeqFlatMapOf[K, V]) IsZero() bool {
	return !(*seqFlatTable[K, V])(
		atomic.LoadPointer(&m.table),
	).SumSizeExceeds(0)
}

type seqFlatResizeState struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		wg        sync.WaitGroup
		table     unsafe.Pointer
		newTable  unsafe.Pointer
		process   int32
		completed int32
	}{})%CacheLineSize) % CacheLineSize]byte

	wg        sync.WaitGroup
	table     unsafe.Pointer // *seqFlatTable[K,V]
	newTable  unsafe.Pointer // *seqFlatTable[K,V]
	process   int32
	completed int32
}

//go:noinline
func (m *SeqFlatMapOf[K, V]) tryResize(hint mapResizeHint, size, sizeAdd int) {
	rs := new(seqFlatResizeState)
	rs.wg.Add(1)
	if !atomic.CompareAndSwapPointer(&m.resizeState, nil, unsafe.Pointer(rs)) {
		return
	}
	cpus := runtime.GOMAXPROCS(0)
	if hint == mapClearHint {
		newTable := newSeqFlatTable[K, V](m.minTableLen, cpus)
		atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
		atomic.StorePointer(&m.resizeState, nil)
		rs.wg.Done()
		return
	}

	table := (*seqFlatTable[K, V])(atomic.LoadPointer(&m.table))
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
	} else { // shrink
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

func (m *SeqFlatMapOf[K, V]) finalizeResize(
	table *seqFlatTable[K, V],
	newTableLen int,
	rs *seqFlatResizeState,
	cpus int,
) {
	atomic.StorePointer(&rs.table, unsafe.Pointer(table))
	newTable := newSeqFlatTable[K, V](newTableLen, cpus)
	atomic.StorePointer(&rs.newTable, unsafe.Pointer(newTable))
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *SeqFlatMapOf[K, V]) helpCopyAndWait(rs *seqFlatResizeState) {
	table := (*seqFlatTable[K, V])(atomic.LoadPointer(&rs.table))
	tableLen := table.bucketsMask + 1
	chunks := int32(table.chunks)
	chunkSize := table.chunkSize
	newTable := (*seqFlatTable[K, V])(atomic.LoadPointer(&rs.newTable))
	for {
		process := atomic.AddInt32(&rs.process, 1)
		if process > chunks {
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSize
		end := min(start+chunkSize, tableLen)
		m.copySeqFlatBucketRange(table, start, end, newTable)
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
			atomic.StorePointer(&m.resizeState, nil)
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
			metaw := *b.meta.Raw()
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				e := b.At(idx)
				v := e.value
				if m.valueIsValid(v) {
					if embeddedHash {
						hash = e.getHash()
					} else {
						hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
					}
					bidx := newTable.bucketsMask & h1(hash, m.intKey)
					destBucket := newTable.buckets.At(bidx)
					h2v := h2(hash)

					destb := destBucket
				appendTo:
					for {
						metaw := *destb.meta.Raw()
						emptyw := (^metaw) & metaMask
						if emptyw != 0 {
							emptyIdx := firstMarkedByteIndex(emptyw)
							*destb.meta.Raw() = setByte(metaw, h2v, emptyIdx)
							emptyEntry := destb.At(emptyIdx)
							emptyEntry.value = v
							if embeddedHash {
								emptyEntry.setHash(hash)
							}
							emptyEntry.key = e.key
							break appendTo
						}
						next := (*seqFlatBucket[K, V])(destb.next)
						if next == nil {
							newBucket := &seqFlatBucket[K, V]{
								meta:    makeAtomicUint64(setByte(emptyMeta, h2v, 0)),
								entries: [entriesPerMapOfBucket]seqFlatEntry[K, V]{{value: v, key: e.key}},
							}
							if embeddedHash {
								newBucket.At(0).setHash(hash)
							}
							destb.next = unsafe.Pointer(newBucket)
							break appendTo
						}
						destb = next
					}
					copied++
				}
			}
		}
		srcBucket.Unlock()
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

type kvPair[K comparable, V comparable] struct {
	k K
	v V
}

// Assumes caller holds the root bucket lock protecting this bucket
func (m *SeqFlatMapOf[K, V]) snapshotBucketLocked(
	b *seqFlatBucket[K, V],
) []kvPair[K, V] {
	pairs := make([]kvPair[K, V], 0, entriesPerMapOfBucket)
	metaw := *b.meta.Raw()
	for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
		idx := firstMarkedByteIndex(markedw)
		e := b.At(idx)
		v := e.value
		if m.valueIsValid(v) {
			pairs = append(pairs, kvPair[K, V]{k: e.key, v: v})
		}
	}
	return pairs
}

//go:nosplit
func trySpin(spins *int) bool {
	if runtime_canSpin(*spins) {
		*spins += 1
		runtime_doSpin()
		return true
	}
	return false
}
