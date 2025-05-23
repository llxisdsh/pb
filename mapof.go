package pb

import (
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// opByteIdx reserve the meta highest byte for extended status flags (OpByte)
	opByteIdx         = 7
	opByteMask uint64 = 0xff00000000000000
	opLockMask uint64 = 1 << (opByteIdx*8 + 7)

	// entriesPerMapOfBucket defines the number of MapOf entries per bucket.
	// This value is automatically calculated to fit within a cache line,
	// but will not exceed 8, which is the upper limit supported by the meta field.
	entriesPerMapOfBucket = min(opByteIdx, (int(CacheLineSize)-int(unsafe.Sizeof(struct {
		meta uint64
		// entries [entriesPerMapOfBucket]unsafe.Pointer
		next unsafe.Pointer
	}{})))/int(unsafe.Sizeof(unsafe.Pointer(nil))))

	emptyMeta     uint64 = 0
	emptyMetaSlot uint8  = 0
	metaMask      uint64 = 0x8080808080808080 >> (64 - min(entriesPerMapOfBucket*8, 64))
	metaSlotMask  uint8  = 0x80
)

const (
	// mapShrinkFraction defines the threshold fraction of table occupation that triggers
	// table shrinking when deleting the last entry in a bucket chain.
	mapShrinkFraction = 128
	// mapLoadFactor defines the threshold that triggers a table resize during insertion.
	// A map can hold up to mapLoadFactor*entriesPerMapOfBucket*tableLen key-value pairs
	// (this is a soft limit).
	mapLoadFactor = 0.75
	// defaultMinMapTableLen defines the minimum table size (number of buckets).
	// The minimum map capacity can be calculated as entriesPerMapOfBucket*defaultMinMapTableLen.
	defaultMinMapTableLen = 32
	// minBucketsPerGoroutine defines the minimum table size required to trigger parallel resizing.
	// Tables smaller than this threshold will use single-threaded resizing for better efficiency.
	minBucketsPerGoroutine       = 4
	minBucketsPerHelperGoroutine = 64
	// minParallelBatchItems defines the minimum number of items required for parallel batch processing.
	// Below this threshold, serial processing is used to avoid the overhead of goroutine creation.
	minParallelBatchItems = 256
)

const (
	// enableFastPath optimizes read-only operations by fast-returning results,
	// avoiding expensive lock contention and write barriers. In extreme cases, reduces latency by up to 100x.
	enableFastPath = true

	// enableHashSpread controls whether to apply hash spreading for non-integer keys.
	// When enabled, it improves hash distribution by XORing the original hash with its high bits
	// before calculating bucket indices (h1) and metadata values (h2).
	// This reduces collisions for complex key types like strings, but adds a small computational overhead.
	// For integer keys, spreading is not applied as their natural distribution is already sufficient.
	enableHashSpread = false

	// enableSpin controls whether spinning is enabled in custom synchronization logic
	// (e.g., bucket locks or other wait mechanisms). When true, waiting operations
	// will call runtime_doSpin() directly, which uses the CPU's PAUSE instruction
	// to reduce contention latency. This improves performance for short waits but
	// may slightly reduce throughput under high contention.
	enableSpin = true

	// enableParallelResize enables concurrent resizing using worker goroutines.
	// Trade-off: Faster but uses more memory. Disable for low-CPU environments.
	enableParallelResize = false
)

// MapOf is a high-performance concurrent map implementation that offers significant
// performance improvements over sync.Map in many common scenarios.
//
// Benchmarks show that MapOf can achieve several times better read performance
// compared to sync.Map in read-heavy workloads, while also providing competitive
// write performance. The actual performance gain varies depending on workload
// characteristics, key types, and concurrency patterns.
//
// MapOf is particularly well-suited for scenarios with:
//   - High read-to-write ratios
//   - Frequent lookups of existing keys
//   - Need for atomic operations on values
//
// Key features of pb.MapOf:
//   - Uses cache-line aligned structures to prevent false sharing
//   - Implements zero-value usability for convenient initialization
//   - Provides lazy initialization for better performance
//   - Defaults to Go's built-in hash function, customizable on creation or initialization
//   - Offers complete sync.Map API compatibility
//   - Specially optimized for read operations
//   - Supports parallel resizing for better scalability
//   - Includes rich functional extensions such as LoadOrStoreFn, ProcessEntry, Size, IsZero,
//     Clone, and batch processing functions
//   - Thoroughly tested with comprehensive test coverage
//   - Delivers exceptional performance (see benchmark results below)
//
// pb.MapOf is built upon xsync.MapOf. We extend our thanks to the authors of
// [xsync](https://github.com/puzpuzpuz/xsync) and reproduce its introduction below:
//
// MapOf is like a Go map[K]V but is safe for concurrent
// use by multiple goroutines without additional locking or
// coordination. It follows the interface of sync.Map with
// a number of valuable extensions like Compute or Size.
//
// A MapOf must not be copied after first use.
//
// MapOf uses a modified version of Cache-Line Hash Table (CLHT)
// data structure: https://github.com/LPD-EPFL/CLHT
//
// CLHT is built around idea to organize the hash table in
// cache-line-sized buckets, so that on all modern CPUs update
// operations complete with at most one cache-line transfer.
// Also, Get operations involve no write to memory, as well as no
// mutexes or any other sort of locks. Due to this design, in all
// considered scenarios MapOf outperforms sync.Map.
//
// MapOf also borrows ideas from Java's j.u.c.ConcurrentHashMap
// (immutable K/V pair structs instead of atomic snapshots)
// and C++'s absl::flat_hash_map (meta memory and SWAR-based lookups).
type MapOf[K comparable, V any] struct {
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		table        atomic.Pointer[mapOfTable]
		resizeState  atomic.Pointer[resizeState]
		totalGrowths atomic.Uint32
		totalShrinks atomic.Uint32
		seed         uintptr
		keyHash      hashFunc
		valEqual     equalFunc
		minTableLen  int
		growOnly     bool
		intKey       bool
	}{})%CacheLineSize) % CacheLineSize]byte

	table        atomic.Pointer[mapOfTable]
	resizeState  atomic.Pointer[resizeState]
	totalGrowths atomic.Uint32
	totalShrinks atomic.Uint32
	seed         uintptr
	keyHash      hashFunc
	valEqual     equalFunc
	minTableLen  int  // WithPresize
	growOnly     bool // WithGrowOnly
	intKey       bool
}

// NewMapOf creates a new MapOf instance. Direct initialization is also supported.
//
// Parameters:
//   - WithPresize option for initial capacity
//   - WithGrowOnly option to disable shrinking
func NewMapOf[K comparable, V any](
	options ...func(*MapConfig),
) *MapOf[K, V] {
	return NewMapOfWithHasher[K, V](nil, nil, options...)
}

// NewMapOfWithHasher creates a MapOf with custom hashing and equality functions.
// Allows custom key hashing (keyHash) and value equality (valEqual) functions for compare-and-swap operations
//
// Parameters:
//   - keyHash: nil uses the built-in hasher
//   - valEqual: nil uses the built-in comparison, but if the value is not of a comparable type,
//     using the Compare series of functions will cause a panic
//   - WithPresize option for initial capacity
//   - WithGrowOnly option to disable shrinking
func NewMapOfWithHasher[K comparable, V any](
	keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig),
) *MapOf[K, V] {
	m := &MapOf[K, V]{}
	m.Init(keyHash, valEqual, options...)
	return m
}

// NewMapOfWithHasherUnsafe provides functionality similar to NewMapOfWithHasher,
// but uses unsafe versions of keyHash and valEqual.
// The following example uses an unbalanced and unsafe version:
//
//	 m := NewMapOfWithHasherUnsafe[int, int](
//		func(ptr unsafe.Pointer, _ uintptr) uintptr {
//			return *(*uintptr)(ptr)
//		}, nil)
func NewMapOfWithHasherUnsafe[K comparable, V any](
	keyHash func(ptr unsafe.Pointer, seed uintptr) uintptr,
	valEqual func(ptr unsafe.Pointer, ptr2 unsafe.Pointer) bool,
	options ...func(*MapConfig),
) *MapOf[K, V] {
	m := &MapOf[K, V]{}
	m.init(keyHash, valEqual, options...)
	return m
}

// MapConfig defines configurable MapOf options.
type MapConfig struct {
	sizeHint int
	growOnly bool
}

// WithPresize configures new MapOf instance with capacity enough
// to hold sizeHint entries. The capacity is treated as the minimal
// capacity meaning that the underlying hash table will never shrink
// to a smaller capacity. If sizeHint is zero or negative, the value
// is ignored.
func WithPresize(sizeHint int) func(*MapConfig) {
	return func(c *MapConfig) {
		c.sizeHint = sizeHint
	}
}

// WithGrowOnly configures new MapOf instance to be grow-only.
// This means that the underlying hash table grows in capacity when
// new keys are added, but does not shrink when keys are deleted.
// The only exception to this rule is the Clear method which
// shrinks the hash table back to the initial capacity.
func WithGrowOnly() func(*MapConfig) {
	return func(c *MapConfig) {
		c.growOnly = true
	}
}

// EntryOf is an immutable map entry.
type EntryOf[K comparable, V any] struct {
	Key   K
	Value V
}

// bucketOf represents a single bucket in the hash table.
type bucketOf struct {
	meta uint64 // meta for fast entry lookups using SWAR, must be 64-bit aligned

	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		meta    uint64
		entries [entriesPerMapOfBucket]unsafe.Pointer
		next    unsafe.Pointer
	}{})%CacheLineSize) % CacheLineSize]byte

	entries [entriesPerMapOfBucket]unsafe.Pointer // entries Pointers to *EntryOf instances
	next    unsafe.Pointer                        // next Pointer to the next bucket (*bucketOf) in the chain
}

// lock acquires the lock for the bucket.
// Replace Mutex with a spinlock to avoid bucket false-sharing overhead.
// Spinlock state is embedded in the Meta field for cache locality.
//
// Partially references:
// [https://github.com/facebook/folly/blob/main/folly/synchronization/PicoSpinLock.h]
func (b *bucketOf) lock() {
	cur := atomic.LoadUint64(&b.meta)
	if atomic.CompareAndSwapUint64(&b.meta, cur&(^opLockMask), cur|opLockMask) {
		return
	}
	b.slowLock()
}

func (b *bucketOf) slowLock() {
	spins := 0
	for !b.tryLock() {
		delay(&spins)
	}
}

func (b *bucketOf) tryLock() bool {
	return casOp(&b.meta, opLockMask, false, true)
}

func (b *bucketOf) unlock() {
	storeOp(&b.meta, opLockMask, false)
}

// resizeState represents the current state of a resizing operation
type resizeState struct {
	table     *mapOfTable
	wg        sync.WaitGroup
	newWg     sync.WaitGroup
	chunkSize int
	works     atomic.Int32
	chunks    int32
	process   atomic.Int32
	completed atomic.Int32
}

// mapOfTable represents the internal hash table structure.
type mapOfTable struct {
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		buckets  []bucketOf
		size     []counterStripe
		newTable atomic.Pointer[mapOfTable]
	}{})%CacheLineSize) % CacheLineSize]byte

	buckets []bucketOf
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	// when the compile option `mapof_opt_enablepadding` is enabled
	size []counterStripe

	// temporarily store the resized new table
	newTable atomic.Pointer[mapOfTable]
}

func newMapOfTable(tableLen int) *mapOfTable {
	return &mapOfTable{
		buckets: make([]bucketOf, tableLen),
		size:    make([]counterStripe, calcSizeLen(tableLen)),
	}
}

// calcTableLen computes the bucket count for the table
// return value must be a power of 2
func calcTableLen(sizeHint int) int {
	tableLen := defaultMinMapTableLen
	if sizeHint > defaultMinMapTableLen*entriesPerMapOfBucket {
		tableLen = nextPowOf2(int((float64(sizeHint) / float64(entriesPerMapOfBucket)) / mapLoadFactor))
	}
	return tableLen
}

// calcSizeLen computes the size count for the table
// return value must be a power of 2
func calcSizeLen(tableLen int) int {
	return nextPowOf2(min(runtime.GOMAXPROCS(0), max(1, tableLen>>10)))
}

// addSize atomically adds delta to the size counter for the given bucket index.
func (table *mapOfTable) addSize(bucketIdx uintptr, delta int) {
	cidx := uintptr(len(table.size)-1) & bucketIdx
	atomic.AddUintptr(&table.size[cidx].c, uintptr(delta))
}

//// addSizePlain adds delta to the size counter without atomic operations.
//// This method should only be used when thread safety is guaranteed by the caller.
//func (table *mapOfTable) addSizePlain(bucketIdx uintptr, delta int) {
//	cidx := uintptr(len(table.size)-1) & bucketIdx
//	table.size[cidx].c += uintptr(delta)
//}

// sumSize calculates the total number of entries in the table by summing all counter stripes.
func (table *mapOfTable) sumSize() int {
	var sum int
	for i := range table.size {
		sum += int(atomic.LoadUintptr(&table.size[i].c))
	}
	return sum
}

// isZero checks if the table is empty by verifying all counter stripes are zero.
func (table *mapOfTable) isZero() bool {
	for i := range table.size {
		if atomic.LoadUintptr(&table.size[i].c) != 0 {
			return false
		}
	}
	return true
}

// Init the MapOf, Allows custom key hasher (keyHash)
// and value equality (valEqual) functions for compare-and-swap operations
//
// Parameters:
//   - keyHash: nil uses the built-in hasher
//   - valEqual: nil uses the built-in comparison, but if the value is not of a comparable type,
//     using the Compare series of functions will cause a panic
//   - WithPresize option for initial capacity
//   - WithGrowOnly option to disable shrinking
//
// Notes:
//   - This function is not thread-safe and can only be used before the MapOf is utilized.
//   - If this function is not called, MapOf will use the default configuration.
func (m *MapOf[K, V]) Init(
	keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig),
) {
	var hs hashFunc
	var eq equalFunc
	if keyHash != nil {
		hs = func(pointer unsafe.Pointer, u uintptr) uintptr {
			return keyHash(*(*K)(pointer), u)
		}
	}
	if valEqual != nil {
		eq = func(val unsafe.Pointer, val2 unsafe.Pointer) bool {
			return valEqual(*(*V)(val), *(*V)(val2))
		}
	}
	m.init(hs, eq, options...)
}

func (m *MapOf[K, V]) init(
	hs hashFunc,
	eq equalFunc,
	options ...func(*MapConfig),
) *mapOfTable {

	c := &MapConfig{
		sizeHint: defaultMinMapTableLen * entriesPerMapOfBucket,
	}
	for _, o := range options {
		o(c)
	}

	m.seed = uintptr(rand.Uint64())
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	if hs != nil {
		m.keyHash = hs
	}
	if eq != nil {
		m.valEqual = eq
	}

	m.minTableLen = calcTableLen(c.sizeHint)
	m.growOnly = c.growOnly

	table := newMapOfTable(m.minTableLen)
	m.table.Store(table)
	return table
}

// initSlow may be called concurrently by multiple goroutines, so it requires
// synchronization with a "lock" mechanism.
func (m *MapOf[K, V]) initSlow() *mapOfTable {
	rs := m.resizeState.Load()
	if rs != nil {
		rs.wg.Wait()
		// Now the table should be initialized
		return m.table.Load()
	}

	// Create a temporary WaitGroup for initialization synchronization
	rs = new(resizeState)
	rs.wg.Add(1)

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !m.resizeState.CompareAndSwap(nil, rs) {
		// Another goroutine is initializing, wait for it to complete
		rs = m.resizeState.Load()
		if rs != nil {
			rs.wg.Wait()
		}
		// Now the table should be initialized
		return m.table.Load()
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := m.table.Load()
	if table != nil {
		m.resizeState.Store(nil)
		rs.wg.Done()
		return table
	}

	// Perform initialization
	table = m.init(nil, nil)
	m.table.Store(table)
	m.resizeState.Store(nil)
	rs.wg.Done()
	return table
}

// Load retrieves a value for a key, compatible with `sync.Map`.
func (m *MapOf[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return
	}

	// inline findEntry
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	bidx := uintptr(len(table.buckets)-1) & h1(hash, m.intKey)
	rootb := &table.buckets[bidx]
	for b := rootb; b != nil; b = (*bucketOf)(loadPointer(&b.next)) {
		metaw := loadUint64(&b.meta)
		for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := (*EntryOf[K, V])(loadPointer(&b.entries[idx])); e != nil {
				if e.Key == key {
					return e.Value, true
				}
			}
		}
	}
	return
}

func (m *MapOf[K, V]) findEntry(table *mapOfTable, hash uintptr, key *K) *EntryOf[K, V] {
	h2v := h2(hash)
	h2w := broadcast(h2v)
	bidx := uintptr(len(table.buckets)-1) & h1(hash, m.intKey)
	rootb := &table.buckets[bidx]
	for b := rootb; b != nil; b = (*bucketOf)(loadPointer(&b.next)) {
		metaw := loadUint64(&b.meta)
		for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := (*EntryOf[K, V])(loadPointer(&b.entries[idx])); e != nil {
				if e.Key == *key {
					return e
				}
			}
		}
	}
	return nil
}

func (m *MapOf[K, V]) processEntry(
	table *mapOfTable,
	hash uintptr,
	key *K,
	fn func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (V, bool) {

	h1v := h1(hash, m.intKey)
	h2v := h2(hash)
	h2w := broadcast(h2v)

	for {
		bidx := uintptr(len(table.buckets)-1) & h1v
		rootb := &table.buckets[bidx]

		rootb.lock()

		// This is the first check, checking if there is a resize operation in progress
		// before acquiring the bucket lock
		if rs := m.resizeState.Load(); rs != nil {
			rootb.unlock()
			// Wait for the current resize operation to complete
			m.helpCopyAndWait(rs)
			table = m.table.Load()
			continue
		}

		// This is the second check, checking if the table has been replaced by another
		// goroutine after acquiring the bucket lock
		// This is necessary because another goroutine may have completed a resize operation
		// between the first check and acquiring the bucket lock
		if newTable := m.table.Load(); table != newTable {
			rootb.unlock()
			table = newTable
			continue
		}

		var (
			oldEntry    *EntryOf[K, V]
			oldBucket   *bucketOf
			oldIdx      int
			oldMeta     uint64
			emptyBucket *bucketOf
			emptyIdx    int
			lastBucket  *bucketOf
		)

		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			lastBucket = b
			metaw := b.meta

			if emptyBucket == nil {
				emptyw := (^metaw) & metaMask
				if emptyw != 0 {
					emptyBucket = b
					emptyIdx = firstMarkedByteIndex(emptyw)
				}
			}

			for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(b.entries[idx]); e != nil {
					if e.Key == *key {
						oldEntry = e
						oldBucket = b
						oldIdx = idx
						oldMeta = metaw
						break
					}
				}
			}
			if oldEntry != nil {
				break
			}
		}

		// --- Processing Logic ---
		newEntry, value, status := fn(oldEntry)

		if oldEntry != nil {
			if newEntry == oldEntry {
				rootb.unlock()
				return value, status
			}
			if newEntry != nil {
				// Update
				newEntry.Key = *key
				storePointer(&oldBucket.entries[oldIdx], unsafe.Pointer(newEntry))
				rootb.unlock()
				return value, status
			}
			// Delete
			newmetaw := setByte(oldMeta, emptyMetaSlot, oldIdx)
			storeUint64(&oldBucket.meta, newmetaw)
			storePointer(&oldBucket.entries[oldIdx], nil)
			rootb.unlock()
			table.addSize(bidx, -1)

			// Check if table shrinking is needed
			if clearOp(newmetaw) == emptyMeta {
				tableLen := len(table.buckets)
				if !m.growOnly &&
					m.minTableLen < tableLen &&
					table.sumSize() <= (tableLen*entriesPerMapOfBucket)/mapShrinkFraction {
					m.resize(table, mapShrinkHint)
				}
			}
			return value, status
		}

		if newEntry == nil {
			rootb.unlock()
			return value, status
		}

		// Insert
		newEntry.Key = *key
		if emptyBucket != nil {
			storeUint64(&emptyBucket.meta, setByte(emptyBucket.meta, h2v, emptyIdx))
			storePointer(&emptyBucket.entries[emptyIdx], unsafe.Pointer(newEntry))
			rootb.unlock()
			table.addSize(bidx, 1)
			return value, status
		}

		// No empty slot, create new bucket and insert
		storePointer(&lastBucket.next, unsafe.Pointer(&bucketOf{
			meta:    setByte(emptyMeta, h2v, 0),
			entries: [entriesPerMapOfBucket]unsafe.Pointer{unsafe.Pointer(newEntry)},
		}))
		rootb.unlock()
		table.addSize(bidx, 1)

		// Check if the table needs to grow
		if table.sumSize() >= int(float64(len(table.buckets)*entriesPerMapOfBucket)*mapLoadFactor) {
			m.resize(table, mapGrowHint)
		}

		return value, status
	}
}

type mapResizeHint int

const (
	mapGrowHint   mapResizeHint = 0
	mapShrinkHint mapResizeHint = 1
	mapClearHint  mapResizeHint = 2
)

// resize returns the current table and indicates whether it was created by the calling goroutine
//
// Parameters:
//   - knownTable: The previously obtained table reference.
//   - hint: The type of resize operation to perform.
//   - sizeHint: If provided, specifies the target size for growth.
//
// Returns:
//   - bool: True if this goroutine performed the resize, false if another goroutine already did it.
func (m *MapOf[K, V]) resize(
	knownTable *mapOfTable,
	hint mapResizeHint,
	sizeHint ...int) bool {

	rs := m.resizeState.Load()
	if rs != nil {
		m.helpCopyAndWait(rs)
		return false
	}

	// Create a new WaitGroup for the current resize operation
	rs = new(resizeState)
	rs.table = knownTable
	rs.wg.Add(1)
	if hint != mapClearHint {
		rs.newWg.Add(1)
	}

	if enableParallelResize {
		rs.works.Add(1)
	}

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !m.resizeState.CompareAndSwap(nil, rs) {
		// Someone else started resize. Wait for it to finish.
		if rs = m.resizeState.Load(); rs != nil {
			m.helpCopyAndWait(rs)
		}
		return false
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := m.table.Load()
	if table != knownTable {
		// If the table has already been changed, return directly
		m.resizeState.Store(nil)
		rs.wg.Done()
		return false
	}
	tableLen := len(table.buckets)

	var newTableLen int
	if hint == mapGrowHint {
		if len(sizeHint) == 0 {
			// Growth the table with factor of 2
			newTableLen = tableLen << 1
		} else {
			// Grow the table to sizeHint
			newTableLen = calcTableLen(sizeHint[0])
		}
		m.totalGrowths.Add(1)
	} else if hint == mapShrinkHint {
		// Shrink the table with factor of 2
		newTableLen = tableLen >> 1
		m.totalShrinks.Add(1)
	} else {
		newTableLen = m.minTableLen
	}

	newTable := newMapOfTable(newTableLen)

	// start copying
	var completed bool
	if hint == mapClearHint {
		completed = true
	} else {
		cpus := runtime.NumCPU()
		chunkSize, chunksInt := calcParallelism(tableLen, minBucketsPerGoroutine, cpus)
		chunks := int32(chunksInt)
		rs.chunkSize = chunkSize
		rs.chunks = chunks
		table.newTable.Store(newTable)
		rs.newWg.Done()

		for {
			process := rs.process.Add(1)
			if process > chunks {
				break
			}
			process--
			start := int(process) * chunkSize
			end := min(start+chunkSize, tableLen)
			if hint == mapGrowHint {
				copyBucketOf[K, V](table, start, end, newTable, m.keyHash, m.seed, m.intKey)
			} else {
				copyBucketOfLock[K, V](table, start, end, newTable, m.keyHash, m.seed, m.intKey)
			}
			if rs.completed.Add(1) == chunks {
				completed = true
				break
			}
			if enableParallelResize {
				process = rs.process.Load()
				if process > chunks {
					break
				}
				remainingLen := tableLen - int(process)*chunkSize
				needHelpers := remainingLen / minBucketsPerHelperGoroutine
				works := int(rs.works.Load())
				addHelpers := needHelpers - works

				if addHelpers > 0 && works < cpus {
					remainingCpus := cpus - works
					spawnHelpers := min(addHelpers, remainingCpus)
					if spawnHelpers > 0 {
						// fmt.Printf("tableLen: %v, spawnHelpers: %v\n", tableLen, spawnHelpers)
						rs.works.Add(int32(spawnHelpers))
						for i := 0; i < spawnHelpers; i++ {
							go m.helpCopy(rs)
						}
					}
				}
			}
		}
	}

	if completed {
		m.table.Store(newTable)
		m.resizeState.Store(nil)
		rs.wg.Done()
	} else {
		rs.wg.Wait()
	}
	return true
}

func (m *MapOf[K, V]) helpCopyAndWait(rs *resizeState) {
	if enableParallelResize {
		rs.works.Add(1)
	}
	rs.newWg.Wait()
	completed := m.helpCopy(rs)
	if !completed {
		rs.wg.Wait()
	}
}

func (m *MapOf[K, V]) helpCopy(rs *resizeState) (completed bool) {
	table := rs.table
	newTable := table.newTable.Load()
	if newTable == nil {
		// hint == mapClearHint
		return false
	}

	tableLen := len(table.buckets)
	chunks := rs.chunks
	chunkSize := rs.chunkSize
	isGrowth := len(newTable.buckets) > tableLen
	for {
		process := rs.process.Add(1)
		if process > chunks {
			break
		}
		process--
		start := int(process) * chunkSize
		end := min(start+chunkSize, tableLen)
		if isGrowth {
			copyBucketOf[K, V](table, start, end, newTable, m.keyHash, m.seed, m.intKey)
		} else {
			copyBucketOfLock[K, V](table, start, end, newTable, m.keyHash, m.seed, m.intKey)
		}
		if rs.completed.Add(1) == chunks {
			completed = true
			break
		}
	}

	if completed {
		m.table.Store(rs.table.newTable.Load())
		m.resizeState.Store(nil)
		rs.wg.Done()
	}
	return completed
}

// calcParallelism calculates the number of goroutines for parallel processing.
//
// Parameters:
//   - items: Number of items to process.
//   - threshold: Minimum threshold to enable parallel processing.
//   - number of available CPU cores
//
// Returns:
//   - chunks: Suggested degree of parallelism (number of goroutines).
//   - chunkSize: Number of items processed per goroutine
func calcParallelism(items, threshold, cpus int) (chunkSize, chunks int) {
	// If the items is too small, use single-threaded processing.
	// Adjusts the parallel process trigger threshold using a scaling factor.
	// example: items < threshold * 2
	if items <= threshold {
		return items, 1
	}

	chunks = items / threshold
	if chunks > cpus {
		chunks = cpus
	}

	chunkSize = (items + chunks - 1) / chunks

	return chunkSize, chunks
}

// copyBucketOfLock unlike copyBucketOf, it locks the destination bucket to ensure concurrency safety.
func copyBucketOfLock[K comparable, V any](
	table *mapOfTable,
	start, end int,
	destTable *mapOfTable,
	hasher hashFunc,
	seed uintptr,
	intKey bool,
) {
	copied := 0
	for i := start; i < end; i++ {
		srcBucket := &table.buckets[i]
		srcBucket.lock()
		for b := srcBucket; b != nil; b = (*bucketOf)(b.next) {
			metaw := b.meta
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				i := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(b.entries[i]); e != nil {
					// We could store the hash value in the Entry during processEntry to avoid
					// recalculating it here, which would speed up the resize process.
					// However, for simple integer keys, this approach would actually slow down
					// the load operation. Therefore, recalculating the hash value is the better approach.
					hash := hasher(noescape(unsafe.Pointer(&e.Key)), seed)
					bidx := uintptr(len(destTable.buckets)-1) & h1(hash, intKey)
					destb := &destTable.buckets[bidx]
					h2v := h2(hash)

					destb.lock()
					b := destb
				appendToBucket:
					for {
						metaw := b.meta
						emptyw := (^metaw) & metaMask
						if emptyw != 0 {
							emptyIdx := firstMarkedByteIndex(emptyw)
							b.meta = setByte(metaw, h2v, emptyIdx)
							b.entries[emptyIdx] = unsafe.Pointer(e)
							break appendToBucket
						}
						next := (*bucketOf)(b.next)
						if next == nil {
							b.next = unsafe.Pointer(&bucketOf{
								meta:    setByte(emptyMeta, h2v, 0),
								entries: [entriesPerMapOfBucket]unsafe.Pointer{unsafe.Pointer(e)},
							})
							break appendToBucket
						}
						b = next
					}
					destb.unlock()

					copied++
				}
			}
		}
		srcBucket.unlock()
	}
	if copied != 0 {
		destTable.addSize(uintptr(start), copied)
	}
}

func copyBucketOf[K comparable, V any](
	table *mapOfTable,
	start, end int,
	destTable *mapOfTable,
	hasher hashFunc,
	seed uintptr,
	intKey bool,
) {
	copied := 0
	for i := start; i < end; i++ {
		srcBucket := &table.buckets[i]
		srcBucket.lock()
		for b := srcBucket; b != nil; b = (*bucketOf)(b.next) {
			metaw := b.meta
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				i := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(b.entries[i]); e != nil {
					// We could store the hash value in the Entry during processEntry to avoid
					// recalculating it here, which would speed up the resize process.
					// However, for simple integer keys, this approach would actually slow down
					// the load operation. Therefore, recalculating the hash value is the better approach.
					hash := hasher(noescape(unsafe.Pointer(&e.Key)), seed)
					bidx := uintptr(len(destTable.buckets)-1) & h1(hash, intKey)
					destb := &destTable.buckets[bidx]
					h2v := h2(hash)

					b := destb
				appendToBucket:
					for {
						metaw := b.meta
						emptyw := (^metaw) & metaMask
						if emptyw != 0 {
							emptyIdx := firstMarkedByteIndex(emptyw)
							b.meta = setByte(metaw, h2v, emptyIdx)
							b.entries[emptyIdx] = unsafe.Pointer(e)
							break appendToBucket
						}
						next := (*bucketOf)(b.next)
						if next == nil {
							b.next = unsafe.Pointer(&bucketOf{
								meta:    setByte(emptyMeta, h2v, 0),
								entries: [entriesPerMapOfBucket]unsafe.Pointer{unsafe.Pointer(e)},
							})
							break appendToBucket
						}
						b = next
					}

					copied++
				}
			}
		}
		srcBucket.unlock()
	}
	if copied != 0 {
		// copyBucketOf is used during multi-threaded growth, requiring a thread-safe addSize.
		destTable.addSize(uintptr(start), copied)
	}
}

// Store inserts or updates a key-value pair, compatible with `sync.Map`.
func (m *MapOf[K, V]) Store(key K, value V) {
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		// deduplicates identical values
		if m.valEqual != nil {
			e := m.findEntry(table, hash, &key)
			if e != nil {
				if m.valEqual(noescape(unsafe.Pointer(&e.Value)), noescape(unsafe.Pointer(&value))) {
					return
				}
			}
		}
	}

	m.mockSyncMap(table, hash, &key, nil, &value, false)
}

// Swap stores a key-value pair and returns the previous value if any, compatible with `sync.Map`.
func (m *MapOf[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		// deduplicates identical values
		if m.valEqual != nil {
			e := m.findEntry(table, hash, &key)
			if e != nil {
				if m.valEqual(noescape(unsafe.Pointer(&e.Value)), noescape(unsafe.Pointer(&value))) {
					return e.Value, true
				}
			}
		}
	}

	return m.mockSyncMap(table, hash, &key, nil, &value, false)
}

// LoadOrStore retrieves an existing value or stores a new one if the key doesn't exist,
// compatible with `sync.Map`.
func (m *MapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		return m.mockSyncMap(table, hash, &key, nil, &value, true)
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.findEntry(table, hash, &key)
		if e != nil {
			return e.Value, true
		}
	}

	return m.mockSyncMap(table, hash, &key, nil, &value, true)
}

// Delete removes a key-value pair, compatible with `sync.Map`.
func (m *MapOf[K, V]) Delete(key K) {
	table := m.table.Load()
	if table == nil {
		return
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.findEntry(table, hash, &key)
		if e == nil {
			return
		}
	}

	m.mockSyncMap(table, hash, &key, nil, nil, false)
}

// LoadAndDelete retrieves the value for a key and deletes it from the map,
// compatible with `sync.Map`.
func (m *MapOf[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.findEntry(table, hash, &key)
		if e == nil {
			return
		}
	}

	return m.mockSyncMap(table, hash, &key, nil, nil, false)
}

// CompareAndSwap atomically replaces an existing value with a new value
// if the existing value matches the expected value, compatible with `sync.Map`.
func (m *MapOf[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	table := m.table.Load()
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndSwap when value is not of comparable type")
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.findEntry(table, hash, &key)
		if e == nil {
			return false
		}

		if m.valEqual != nil {
			if !m.valEqual(noescape(unsafe.Pointer(&e.Value)), noescape(unsafe.Pointer(&old))) {
				return false
			}
			// deduplicates identical values
			if m.valEqual(noescape(unsafe.Pointer(&e.Value)), noescape(unsafe.Pointer(&new))) {
				return true
			}
		}
	}

	_, swapped = m.mockSyncMap(table, hash, &key, &old, &new, false)
	return swapped
}

// CompareAndDelete atomically deletes an existing entry
// if its value matches the expected value, compatible with `sync.Map`.
func (m *MapOf[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	table := m.table.Load()
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndDelete when value is not of comparable type")
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		e := m.findEntry(table, hash, &key)
		if e == nil {
			return false
		}
		if m.valEqual != nil {
			if !m.valEqual(noescape(unsafe.Pointer(&e.Value)), noescape(unsafe.Pointer(&old))) {
				return false
			}
		}
	}

	_, deleted = m.mockSyncMap(table, hash, &key, &old, nil, false)
	return deleted
}

func (m *MapOf[K, V]) mockSyncMap(
	table *mapOfTable,
	hash uintptr,
	key *K,
	cmpValue *V,
	newValue *V,
	loadOrStore bool,
) (value V, status bool) {
	return m.processEntry(table, hash, key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				if loadOrStore {
					return loaded, loaded.Value, true
				}
				if cmpValue != nil &&
					!m.valEqual(noescape(unsafe.Pointer(&loaded.Value)), noescape(unsafe.Pointer(cmpValue))) {
					return loaded, loaded.Value, false
				}
				if newValue == nil {
					// Delete
					return nil, loaded.Value, true
				}

				// Disabled: Skip deduplication here - move to caller side to avoid lock contention
				//if enableFastPath {
				//	if m.valEqual != nil &&
				//		m.valEqual(noescape(unsafe.Pointer(&loaded.Value)), noescape(unsafe.Pointer(newValue))) {
				//		return loaded, loaded.Value, true
				//	}
				//}

				// Update
				newe := &EntryOf[K, V]{Value: *newValue}
				return newe, loaded.Value, true
			}

			if newValue == nil || cmpValue != nil {
				return loaded, *new(V), false
			}
			// Insert
			newe := &EntryOf[K, V]{Value: *newValue}
			if loadOrStore {
				return newe, *newValue, false
			}
			return newe, *new(V), false
		},
	)
}

// LoadOrStoreFn returns the existing value for the key if
// present. Otherwise, it tries to compute the value using the
// provided function and, if successful, stores and returns
// the computed value. The loaded result is true if the value was
// loaded, or false if computed.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *MapOf[K, V]) LoadOrStoreFn(
	key K,
	valueFn func() V,
) (actual V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		return m.processEntry(table, hash, &key,
			func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if loaded != nil {
					return loaded, loaded.Value, true
				}
				newValue := valueFn()
				return &EntryOf[K, V]{Value: newValue}, newValue, false
			},
		)
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		if e := m.findEntry(table, hash, &key); e != nil {
			return e.Value, true
		}
	}

	return m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return loaded, loaded.Value, true
			}
			newValue := valueFn()
			return &EntryOf[K, V]{Value: newValue}, newValue, false
		},
	)
}

// LoadAndStore returns the existing value for the key if present,
// while setting the new value for the key.
// It stores the new value and returns the existing one, if present.
// The loaded result is true if the existing value was loaded,
// false otherwise.
//
// Compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) LoadAndStore(key K, value V) (actual V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		// deduplicates identical values
		if m.valEqual != nil {
			e := m.findEntry(table, hash, &key)
			if e != nil &&
				m.valEqual(noescape(unsafe.Pointer(&e.Value)), noescape(unsafe.Pointer(&value))) {
				return e.Value, true
			}
		}
	}

	return m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: value}, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: value}, value, false
		},
	)
}

// LoadOrCompute returns the existing value for the key if
// present. Otherwise, it tries to compute the value using the
// provided function and, if successful, stores and returns
// the computed value. The loaded result is true if the value was
// loaded, or false if computed. If valueFn returns true as the
// cancel value, the computation is cancelled and the zero value
// for type V is returned.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
//
// Compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) LoadOrCompute(
	key K,
	valueFn func() (newValue V, cancel bool),
) (value V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		return m.processEntry(table, hash, &key,
			func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if loaded != nil {
					return loaded, loaded.Value, true
				}
				newValue, cancel := valueFn()
				if cancel {
					return nil, *new(V), false
				}
				return &EntryOf[K, V]{Value: newValue}, newValue, false
			},
		)
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		if e := m.findEntry(table, hash, &key); e != nil {
			return e.Value, true
		}
	}

	return m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return loaded, loaded.Value, true
			}
			newValue, cancel := valueFn()
			if cancel {
				return nil, *new(V), false
			}
			return &EntryOf[K, V]{Value: newValue}, newValue, false
		},
	)
}

type ComputeOp int

const (
	// CancelOp signals to Compute to not do anything as a result
	// of executing the lambda. If the entry was not present in
	// the map, nothing happens, and if it was present, the
	// returned value is ignored.
	CancelOp ComputeOp = iota
	// UpdateOp signals to Compute to update the entry to the
	// value returned by the lambda, creating it if necessary.
	UpdateOp
	// DeleteOp signals to Compute to always delete the entry
	// from the map.
	DeleteOp
)

// Compute either sets the computed new value for the key,
// deletes the value for the key, or does nothing, based on
// the returned [ComputeOp]. When the op returned by valueFn
// is [UpdateOp], the value is updated to the new value. If
// it is [DeleteOp], the entry is removed from the map
// altogether. And finally, if the op is [CancelOp] then the
// entry is left as-is. In other words, if it did not already
// exist, it is not created, and if it did exist, it is not
// updated. This is useful to synchronously execute some
// operation on the value without incurring the cost of
// updating the map every time. The ok result indicates
// whether the entry is present in the map after the compute
// operation. The actual result contains the value of the map
// if a corresponding entry is present, or the zero value
// otherwise. See the example for a few use cases.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
//
// Compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) Compute(
	key K,
	valueFn func(oldValue V, loaded bool) (newValue V, op ComputeOp),
) (actual V, ok bool) {

	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	return m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				newValue, op := valueFn(loaded.Value, true)
				if op == UpdateOp {
					// Since we're already inside the lock (where overhead is inevitable),
					// it's better to let users handle same-value filtering with CancelOp instead.
					if enableFastPath {
						if m.valEqual != nil &&
							m.valEqual(noescape(unsafe.Pointer(&loaded.Value)), noescape(unsafe.Pointer(&newValue))) {
							return loaded, loaded.Value, true
						}
					}
					return &EntryOf[K, V]{Value: newValue}, newValue, true
				}
				if op == DeleteOp {
					return nil, loaded.Value, false
				}
				return loaded, loaded.Value, true
			}
			newValue, op := valueFn(*new(V), false)
			if op == UpdateOp {
				return &EntryOf[K, V]{Value: newValue}, newValue, true
			}
			return nil, *new(V), false
		},
	)
}

// LoadOrProcessEntry loads an existing value or computes a new one using the provided function.
//
// If the key exists, its value is returned directly.
// If the key doesn't exist, the provided function fn is called to compute a new value.
//
// Parameters:
//   - key: The key to look up or process
//   - valueFn: Only called when the value does not exist, return values are described below:
//     return *EntryOf[K, V]: New entry, nil means don't store any value.
//     return V: value to return to the caller.
//     return bool: Whether the operation succeeded.
//
// Returns:
//   - value V: Existing value if key exists; otherwise the value returned by valueFn
//   - loaded bool: true if key exists; otherwise the bool value returned by valueFn
//
// Notes:
//   - The fn function is executed while holding an internal lock.
//     Keep the execution time short to avoid blocking other operations.
//   - Avoid calling other map methods inside fn to prevent deadlocks.
func (m *MapOf[K, V]) LoadOrProcessEntry(
	key K,
	valueFn func() (*EntryOf[K, V], V, bool),
) (value V, loaded bool) {

	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		return m.processEntry(table, hash, &key,
			func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if loaded != nil {
					return loaded, loaded.Value, true
				}
				return valueFn()
			},
		)
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		if e := m.findEntry(table, hash, &key); e != nil {
			return e.Value, true
		}
	}

	return m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return loaded, loaded.Value, true
			}
			return valueFn()
		},
	)
}

// ProcessEntry processes a key-value pair using the provided function.
//
// This method is the foundation for all modification operations in MapOf. It provides
// complete control over key-value pairs, allowing atomic reading, modification,
// deletion, or insertion of entries.
//
// Parameters:
//   - key: The key to process
//   - fn: Called regardless of value existence; parameters and return values are described below:
//     loaded *EntryOf[K, V]: current entry (nil if key doesn't exist),
//     return *EntryOf[K, V]:  nil to delete, new entry to store; ==loaded for no modification,
//     return V: value to be returned as ProcessEntry's value;
//     return bool: status to be returned as ProcessEntry's status indicator
//
// Returns:
//   - value V: First return value from fn
//   - status bool: Second return value from fn
//
// Notes:
//   - The input parameter loaded is immutable and should not be modified directly
//   - This method internally ensures goroutine safety and consistency
//   - If you need to modify a value, return a new EntryOf instance
//   - The fn function is executed while holding an internal lock.
//     Keep the execution time short to avoid blocking other operations.
//   - Avoid calling other map methods inside fn to prevent deadlocks.
//   - Do not perform expensive computations or I/O operations inside fn.
func (m *MapOf[K, V]) ProcessEntry(
	key K,
	fn func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (value V, status bool) {

	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	return m.processEntry(table, hash, &key, fn)
}

// Clear compatible with `sync.Map`
func (m *MapOf[K, V]) Clear() {
	table := m.table.Load()
	if table == nil {
		return
	}
	for !m.resize(table, mapClearHint) {
		table = m.table.Load()
	}
}

// RangeEntry iterates over all entries in the map.
//
// Notes:
//   - Never modify the Key or Value in an Entry under any circumstances.
//   - Range operates on the current table snapshot, which may not reflect the most up-to-date state.
//     Similar to `sync.Map`, this provides eventual consistency for reads.
//     at a slight performance cost.
func (m *MapOf[K, V]) RangeEntry(yield func(e *EntryOf[K, V]) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}

	// Pre-allocate array big enough to fit entries for most hash tables.
	entries := make([]*EntryOf[K, V], 0, 16*entriesPerMapOfBucket)
	for i := range table.buckets {
		rootb := &table.buckets[i]
		rootb.lock()
		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			metaw := b.meta
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				i := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(b.entries[i]); e != nil {
					entries = append(entries, e)
				}
			}
		}
		rootb.unlock()
		// Call the function for all copied entries.
		for j := range entries {
			if !yield(entries[j]) {
				return
			}
			entries[j] = nil // fast gc
		}
		entries = entries[:0]
	}
}

// All compatible with `sync.Map`.
func (m *MapOf[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Keys is the iterator version for iterating over all keys.
func (m *MapOf[K, V]) Keys() func(yield func(K) bool) {
	return m.RangeKeys
}

// Values is the iterator version for iterating over all values.
func (m *MapOf[K, V]) Values() func(yield func(V) bool) {
	return m.RangeValues
}

// Range compatible with `sync.Map`.
func (m *MapOf[K, V]) Range(yield func(key K, value V) bool) {
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		return yield(e.Key, e.Value)
	})
}

// RangeKeys to iterate over all keys
func (m *MapOf[K, V]) RangeKeys(yield func(key K) bool) {
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		return yield(e.Key)
	})
}

// RangeValues to iterate over all values
func (m *MapOf[K, V]) RangeValues(yield func(value V) bool) {
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		return yield(e.Value)
	})
}

// Size returns the number of key-value pairs in the map.
// This is an O(1) operation.
//
// Compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) Size() int {
	table := m.table.Load()
	if table == nil {
		return 0
	}
	return table.sumSize()
}

// IsZero checks zero values, faster than Size().
func (m *MapOf[K, V]) IsZero() bool {
	table := m.table.Load()
	if table == nil {
		return true
	}
	return table.isZero()
}

// ToMap collect all entries and return a map[K]V
func (m *MapOf[K, V]) ToMap() map[K]V {
	a := make(map[K]V, m.Size())
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		a[e.Key] = e.Value
		return true
	})
	return a
}

// ToMapWithLimit collect up to limit entries into a map[K]V, limit < 0 is no limit
func (m *MapOf[K, V]) ToMapWithLimit(limit int) map[K]V {
	if limit == 0 {
		return map[K]V{}
	}
	if limit < 0 {
		limit = math.MaxInt
	}
	a := make(map[K]V, min(m.Size(), limit))
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		a[e.Key] = e.Value
		limit--
		return limit > 0
	})
	return a
}

// HasKey to check if the key exist
func (m *MapOf[K, V]) HasKey(key K) bool {
	table := m.table.Load()
	if table == nil {
		return false
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	return m.findEntry(table, hash, &key) != nil
}

// String implement the formatting output interface fmt.Stringer
func (m *MapOf[K, V]) String() string {
	const limit = 1024
	return strings.Replace(fmt.Sprint(m.ToMapWithLimit(limit)), "map[", "MapOf[", 1)
}

var (
	jsonMarshal   func(v any) ([]byte, error)
	jsonUnmarshal func(data []byte, v any) error
)

// SetDefaultJSONMarshal sets the default JSON serialization and deserialization functions.
// If not set, the standard library is used by default.
func SetDefaultJSONMarshal(marshal func(v any) ([]byte, error), unmarshal func(data []byte, v any) error) {
	jsonMarshal, jsonUnmarshal = marshal, unmarshal
}

// MarshalJSON JSON serialization
func (m *MapOf[K, V]) MarshalJSON() ([]byte, error) {
	if jsonMarshal != nil {
		return jsonMarshal(m.ToMap())
	}
	return json.Marshal(m.ToMap())
}

// UnmarshalJSON JSON deserialization
func (m *MapOf[K, V]) UnmarshalJSON(data []byte) error {
	var a map[K]V
	if jsonUnmarshal != nil {
		if err := jsonUnmarshal(data, &a); err != nil {
			return err
		}
	} else {
		if err := json.Unmarshal(data, &a); err != nil {
			return err
		}
	}
	m.FromMap(a)
	return nil
}

// batchProcess is the core generic function for all batch operations.
// It processes a group of keys or key-value pairs by applying the specified processing function to each item.
//
// Parameters:
//   - table: The current hash table.
//   - itemCount: The number of items to be processed.
//   - growFactor: Capacity change coefficient:
//     >0: Estimates new items as itemCount * growFactor.
//     <=0: No estimation for new items.
//   - processor: The function to process each item.
//   - canParallel: Whether parallel processing is supported
func (m *MapOf[K, V]) batchProcess(
	table *mapOfTable,
	itemCount int,
	growFactor float64,
	processor func(table *mapOfTable, start, end int),
	canParallel bool,
) {
	if table == nil {
		table = m.initSlow()
	}

	// Calculate estimated new items based on growFactor
	if growFactor > 0 {
		// Estimate new items
		newItemsEstimate := int(float64(itemCount) * growFactor)

		// Pre-growth check
		if newItemsEstimate > 0 {
			// Retry the resize until it succeeds
			for {
				growThreshold := int(float64(len(table.buckets)*entriesPerMapOfBucket) * mapLoadFactor)
				sizeHint := table.sumSize() + newItemsEstimate
				if sizeHint <= growThreshold {
					break
				}
				if m.resize(table, mapGrowHint, sizeHint) {
					break
				}
				table = m.table.Load()
			}
		}
	}

	// Calculate the parallel count
	chunks := 1
	var chunkSize int
	if canParallel {
		chunkSize, chunks = calcParallelism(itemCount, minParallelBatchItems, runtime.GOMAXPROCS(0))
	}

	if chunks > 1 {
		var wg sync.WaitGroup
		wg.Add(chunks)
		for i := 0; i < chunks; i++ {
			go func(start, end int) {
				defer wg.Done()
				processor(table, start, end)
			}(i*chunkSize, min((i+1)*chunkSize, itemCount))
		}
		wg.Wait()
		return
	}

	// Serial processing
	processor(table, 0, itemCount)
}

// BatchProcessImmutableEntries batch processes multiple immutable key-value pairs with a custom function.
//
// Parameters:
//   - immutableEntries: Slice of immutable key-value pairs to process.
//   - growFactor: Capacity change coefficient (see batchProcess).
//   - processFn: Function that receives entry and current value (if exists),
//     returns new value, result value, and status.
//
// Returns:
//   - values []V: Slice of values from processFn.
//   - status []bool: Slice of status values from processFn.
//
// Notes:
//   - immutableEntries will be stored directly; as the name suggests,
//     the key-value pairs of the elements should not be changed.
func (m *MapOf[K, V]) BatchProcessImmutableEntries(
	immutableEntries []*EntryOf[K, V],
	growFactor float64,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	return m.batchProcessImmutableEntries(immutableEntries, growFactor, processFn)
}

func (m *MapOf[K, V]) batchProcessImmutableEntries(
	immutableEntries []*EntryOf[K, V],
	growFactor float64,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	if len(immutableEntries) == 0 {
		return
	}

	values = make([]V, len(immutableEntries))
	status = make([]bool, len(immutableEntries))

	table := m.table.Load()

	m.batchProcess(table, len(immutableEntries), growFactor, func(table *mapOfTable, start, end int) {
		for i := start; i < end; i++ {
			hash := m.keyHash(noescape(unsafe.Pointer(&immutableEntries[i].Key)), m.seed)
			values[i], status[i] = m.processEntry(table, hash, &immutableEntries[i].Key,
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(immutableEntries[i], loaded)
				},
			)
		}
	}, true)

	return
}

// BatchProcessEntries batch processes multiple key-value pairs with a custom function
//
// Parameters:
//   - entries: slice of key-value pairs to process
//   - growFactor: capacity change coefficient (see batchProcess)
//   - processFn: function that receives entry and current value (if exists),
//     returns new value, result value, and status
//
// Returns:
//   - values []V: slice of values from processFn
//   - status []bool: slice of status values from processFn
//
// Notes:
//   - The processFn should not directly return `entry`,
//     as this would prevent the entire `entries` from being garbage collected in a timely manner.
func (m *MapOf[K, V]) BatchProcessEntries(
	entries []EntryOf[K, V],
	growFactor float64,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	return m.batchProcessEntries(entries, growFactor, processFn)
}

func (m *MapOf[K, V]) batchProcessEntries(
	entries []EntryOf[K, V],
	growFactor float64,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	if len(entries) == 0 {
		return
	}

	values = make([]V, len(entries))
	status = make([]bool, len(entries))

	table := m.table.Load()

	m.batchProcess(table, len(entries), growFactor, func(table *mapOfTable, start, end int) {
		for i := start; i < end; i++ {
			hash := m.keyHash(noescape(unsafe.Pointer(&entries[i].Key)), m.seed)
			values[i], status[i] = m.processEntry(table, hash, &entries[i].Key,
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(&entries[i], loaded)
				},
			)
		}
	}, true)

	return
}

// BatchProcessKeys batch processes multiple keys with a custom function
//
// Parameters:
//   - keys: slice of keys to process
//   - growFactor: capacity change coefficient (see batchProcess)
//   - processFn: function that receives key and current value (if exists),
//     returns new value, result value, and status
//
// Returns:
//   - values []V: slice of values from processFn
//   - status []bool: slice of status values from processFn
func (m *MapOf[K, V]) BatchProcessKeys(
	keys []K,
	growFactor float64,
	processFn func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	return m.batchProcessKeys(keys, growFactor, processFn)
}

func (m *MapOf[K, V]) batchProcessKeys(
	keys []K,
	growFactor float64,
	processFn func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	if len(keys) == 0 {
		return
	}

	values = make([]V, len(keys))
	status = make([]bool, len(keys))

	table := m.table.Load()

	m.batchProcess(table, len(keys), growFactor, func(table *mapOfTable, start, end int) {
		for i := start; i < end; i++ {
			hash := m.keyHash(noescape(unsafe.Pointer(&keys[i])), m.seed)
			values[i], status[i] = m.processEntry(table, hash, &keys[i],
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(keys[i], loaded)
				},
			)
		}
	}, true)

	return
}

// BatchUpsert batch updates or inserts multiple key-value pairs, returning previous values
//
// Parameters:
//   - entries: slice of key-value pairs to upsert
//
// Returns:
//   - previous: slice of previous values for each key
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchUpsert(entries []EntryOf[K, V]) (previous []V, loaded []bool) {
	return m.batchProcessEntries(
		entries, 1.0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: entry.Value}, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: entry.Value}, *new(V), false
		},
	)
}

// BatchInsert batch inserts multiple key-value pairs, not modifying existing keys
//
// Parameters:
//   - entries: slice of key-value pairs to insert
//
// Returns:
//   - actual: slice of actual values for each key (either existing or newly inserted)
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchInsert(entries []EntryOf[K, V]) (actual []V, loaded []bool) {
	return m.batchProcessEntries(
		entries, 1.0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return loaded, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: entry.Value}, entry.Value, false
		},
	)
}

// BatchDelete batch deletes multiple keys, returning previous values
//
// Parameters:
//   - keys: slice of keys to delete
//
// Returns:
//   - previous: slice of previous values for each key
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchDelete(keys []K) (previous []V, loaded []bool) {
	return m.batchProcessKeys(
		keys, 0,
		func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return nil, loaded.Value, true
			}
			return nil, *new(V), false
		},
	)
}

// BatchUpdate batch updates multiple existing keys, returning previous values
//
// Parameters:
//   - entries: slice of key-value pairs to update
//
// Returns:
//   - previous: slice of previous values for each key
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchUpdate(entries []EntryOf[K, V]) (previous []V, loaded []bool) {
	return m.batchProcessEntries(
		entries, 0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: entry.Value}, loaded.Value, true
			}
			return nil, *new(V), false
		},
	)
}

// FromMap imports key-value pairs from a standard Go map
//
// Parameters:
//   - source: standard Go map to import from
func (m *MapOf[K, V]) FromMap(source map[K]V) {
	if len(source) == 0 {
		return
	}

	table := m.table.Load()

	m.batchProcess(table, len(source), 1.0,
		func(table *mapOfTable, _, _ int) {
			// Directly iterate and process all key-value pairs
			for k, v := range source {
				hash := m.keyHash(noescape(unsafe.Pointer(&k)), m.seed)
				m.processEntry(table, hash, &k,
					func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
						return &EntryOf[K, V]{Value: v}, v, e != nil
					},
				)
			}
		}, false)
}

// FilterAndTransform filters and transforms elements in the map
//
// Parameters:
//   - filterFn: returns true to keep the element, false to remove it
//   - transformFn: transforms values of kept elements, returns new value and whether store is needed
//     if nil, only filtering is performed
func (m *MapOf[K, V]) FilterAndTransform(
	filterFn func(key K, value V) bool,
	transformFn func(key K, value V) (V, bool),
) {
	table := m.table.Load()
	if table == nil {
		return
	}

	// Process elements directly during iteration to avoid extra memory allocation
	var toUpsert []*EntryOf[K, V]
	var toDelete []K

	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		key, value := e.Key, e.Value
		if !filterFn(key, value) {
			toDelete = append(toDelete, key)
		} else if transformFn != nil {
			newValue, needUpsert := transformFn(key, value)
			if needUpsert {
				toUpsert = append(toUpsert, &EntryOf[K, V]{Key: key, Value: newValue})
			}
		}
		return true
	})

	// Batch delete elements that don't meet the condition
	if len(toDelete) > 0 {
		m.BatchDelete(toDelete)
	}

	// Batch update elements that meet the condition
	if len(toUpsert) > 0 {
		m.batchProcessImmutableEntries(
			toUpsert, 1.0,
			func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				return entry, entry.Value, false
			},
		)
	}
}

// Merge integrates another MapOf into the current one
//
// Parameters:
//   - other: the MapOf to merge from
//   - conflictFn: conflict resolution function called when a key exists in both maps
//     if nil, values from other map override current map values
func (m *MapOf[K, V]) Merge(
	other *MapOf[K, V],
	conflictFn func(this, other *EntryOf[K, V]) *EntryOf[K, V],
) {
	if other == nil || other.IsZero() {
		return
	}

	// Default conflict handler: use value from other map
	if conflictFn == nil {
		conflictFn = func(_, other *EntryOf[K, V]) *EntryOf[K, V] {
			return other
		}
	}

	// Pre-fetch target table to avoid multiple checks in loop
	table := m.table.Load()
	otherSize := other.Size()

	m.batchProcess(table, otherSize, 1.0,
		func(table *mapOfTable, _, _ int) {
			other.RangeEntry(func(other *EntryOf[K, V]) bool {
				hash := m.keyHash(noescape(unsafe.Pointer(&other.Key)), m.seed)
				m.processEntry(table, hash, &other.Key,
					func(this *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
						if this == nil {
							// Key doesn't exist in current Map, add directly
							return other, other.Value, false
						}
						// Key exists in both Maps, use conflict handler
						return conflictFn(this, other), other.Value, true
					},
				)
				return true
			})
		}, false)
}

// Clone creates a deep copy of the map.
//
// Returns:
//   - A new MapOf instance with the same key-value pairs.
//
// Notes:
//   - The clone operation is not atomic with respect to concurrent modifications.
//   - The returned map will have the same configuration as the original.
func (m *MapOf[K, V]) Clone() *MapOf[K, V] {
	table := m.table.Load()
	if table == nil {
		return &MapOf[K, V]{}
	}

	// Create a new MapOf with the same configuration as the original
	clone := &MapOf[K, V]{
		seed:        m.seed,
		keyHash:     m.keyHash,
		valEqual:    m.valEqual,
		minTableLen: m.minTableLen,
		growOnly:    m.growOnly,
		intKey:      m.intKey,
	}

	// Pre-fetch size to optimize initial capacity
	size := m.Size()
	if size > 0 {
		cloneTable := newMapOfTable(clone.minTableLen)
		clone.table.Store(cloneTable)
		clone.batchProcess(table, size, 1.0,
			func(table *mapOfTable, _, _ int) {
				// Directly iterate and process all key-value pairs
				m.RangeEntry(func(e *EntryOf[K, V]) bool {
					hash := clone.keyHash(noescape(unsafe.Pointer(&e.Key)), clone.seed)
					clone.processEntry(table, hash, &e.Key,
						func(_ *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
							return e, e.Value, false
						},
					)
					return true
				})
			}, false)
	}

	return clone
}

// Stats returns statistics for the MapOf. Just like other map
// methods, this one is thread-safe. Yet it's an O(N) operation,
// so it should be used only for diagnostics or debugging purposes.
func (m *MapOf[K, V]) Stats() *MapStats {
	stats := &MapStats{
		TotalGrowths: m.totalGrowths.Load(),
		TotalShrinks: m.totalShrinks.Load(),
		MinEntries:   math.MaxInt,
	}
	table := m.table.Load()
	if table == nil {
		return stats
	}
	stats.RootBuckets = len(table.buckets)
	stats.Counter = table.sumSize()
	stats.CounterLen = len(table.size)
	for i := range table.buckets {
		nentries := 0
		rootb := &table.buckets[i]
		rootb.lock()
		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			stats.TotalBuckets++
			nentriesLocal := 0
			stats.Capacity += entriesPerMapOfBucket
			for i := 0; i < entriesPerMapOfBucket; i++ {
				if b.entries[i] != nil {
					stats.Size++
					nentriesLocal++
				}
			}
			nentries += nentriesLocal
			if nentriesLocal == 0 {
				stats.EmptyBuckets++
			}
		}
		rootb.unlock()

		if nentries < stats.MinEntries {
			stats.MinEntries = nentries
		}
		if nentries > stats.MaxEntries {
			stats.MaxEntries = nentries
		}
	}
	return stats
}

// MapStats is MapOf statistics.
//
// Warning: map statistics are intented to be used for diagnostic
// purposes, not for production code. This means that breaking changes
// may be introduced into this struct even between minor releases.
type MapStats struct {
	// RootBuckets is the number of root buckets in the hash table.
	// Each bucket holds a few entries.
	RootBuckets int
	// TotalBuckets is the total number of buckets in the hash table,
	// including root and their chained buckets. Each bucket holds
	// a few entries.
	TotalBuckets int
	// EmptyBuckets is the number of buckets that hold no entries.
	EmptyBuckets int
	// Capacity is the MapOf capacity, i.e. the total number of
	// entries that all buckets can physically hold. This number
	// does not consider the findEntry factor.
	Capacity int
	// Size is the exact number of entries stored in the map.
	Size int
	// Counter is the number of entries stored in the map according
	// to the internal atomic counter. In case of concurrent map
	// modifications this number may be different from Size.
	Counter int
	// CounterLen is the number of internal atomic counter stripes.
	// This number may grow with the map capacity to improve
	// multithreaded scalability.
	CounterLen int
	// MinEntries is the minimum number of entries per a chain of
	// buckets, i.e. a root bucket and its chained buckets.
	MinEntries int
	// MinEntries is the maximum number of entries per a chain of
	// buckets, i.e. a root bucket and its chained buckets.
	MaxEntries int
	// TotalGrowths is the number of times the hash table grew.
	TotalGrowths uint32
	// TotalGrowths is the number of times the hash table shrinked.
	TotalShrinks uint32
}

// ToString returns string representation of map stats.
func (s *MapStats) ToString() string {
	var sb strings.Builder
	sb.WriteString("MapStats{\n")
	sb.WriteString(fmt.Sprintf("RootBuckets:  %d\n", s.RootBuckets))
	sb.WriteString(fmt.Sprintf("TotalBuckets: %d\n", s.TotalBuckets))
	sb.WriteString(fmt.Sprintf("EmptyBuckets: %d\n", s.EmptyBuckets))
	sb.WriteString(fmt.Sprintf("Capacity:     %d\n", s.Capacity))
	sb.WriteString(fmt.Sprintf("Size:         %d\n", s.Size))
	sb.WriteString(fmt.Sprintf("Counter:      %d\n", s.Counter))
	sb.WriteString(fmt.Sprintf("CounterLen:   %d\n", s.CounterLen))
	sb.WriteString(fmt.Sprintf("MinEntries:   %d\n", s.MinEntries))
	sb.WriteString(fmt.Sprintf("MaxEntries:   %d\n", s.MaxEntries))
	sb.WriteString(fmt.Sprintf("TotalGrowths: %d\n", s.TotalGrowths))
	sb.WriteString(fmt.Sprintf("TotalShrinks: %d\n", s.TotalShrinks))
	sb.WriteString("}\n")
	return sb.String()
}

// spread improves hash distribution by XORing the original hash with its high bits.
// This function increases randomness in the lower bits of the hash value,
// which helps reduce collisions when calculating bucket indices.
// It's particularly effective for hash values where significant bits
// are concentrated in the upper positions.
func spread(h uintptr) uintptr {
	return h ^ (h >> 16)
}

// h1 extracts the bucket index from a hash value.
// It uses different shift values based on the key type to optimize performance:
//   - For integer keys (intKey=true): Uses a right shift of 2 bits (divide by 4),
//     which provides good distribution for sequential integers while matching
//     the average bucket capacity (with load factor 0.75 and bucket size 6,
//     each bucket handles ~4.5 elements).
//   - For non-integer keys (intKey=false): Uses a right shift of 7 bits (divide by 128),
//     which provides better distribution for hash values from complex types like strings,
//     where the high bits contain more entropy and the distribution is already randomized
//     by the hash function.
//
// The different approaches balance memory usage, lookup performance, and collision rates
// for different key types.
func h1(h uintptr, intKey bool) uintptr {
	if intKey {
		return h >> 2
	} else {
		if enableHashSpread {
			return spread(h) >> 7
		}
		return h >> 7
	}
}

// h2 extracts the byte-level hash for in-bucket lookups.
func h2(h uintptr) uint8 {
	if enableHashSpread {
		return uint8(spread(h)) | metaSlotMask
	}
	return uint8(h) | metaSlotMask
}

// broadcast replicates a byte value across all bytes of an uint64.
func broadcast(b uint8) uint64 {
	return 0x101010101010101 * uint64(b)
}

// firstMarkedByteIndex finds the index of the first marked byte in an uint64.
// It uses the trailing zeros count to determine the position of the first set bit,
// then converts that bit position to a byte index (dividing by 8).
//
// Parameters:
//   - w: A uint64 value with bits set to mark specific bytes
//
// Returns:
//   - The index (0-7) of the first marked byte in the uint64
func firstMarkedByteIndex(w uint64) int {
	return bits.TrailingZeros64(w) >> 3
}

// markZeroBytes implements SWAR (SIMD Within A Register) byte search.
// It may produce false positives (e.g., for 0x0100), so results should be verified.
// Returns an uint64 with the most significant bit of each byte set if that byte is zero.
//
// Notes:
//
//   - This SWAR algorithm identifies byte positions containing zero values.
//   - The operation (w - 0x0101010101010101) triggers underflow for zero-value bytes,
//     causing their most significant bit (MSB) to flip to 1.
//   - The subsequent & (^w) operation isolates the MSB markers specifically for bytes
//     that were originally zero.
//   - Finally, & emptyMetaMask filters to only consider relevant data slots,
//     using the mask-defined marker bits (MSB of each byte).
func markZeroBytes(w uint64) uint64 {
	return (w - 0x0101010101010101) & (^w) & metaMask
}

// setByte sets the byte at index idx in the uint64 w to the value b.
// Returns the modified uint64 value.
func setByte(w uint64, b uint8, idx int) uint64 {
	shift := idx << 3
	return (w &^ (0xff << shift)) | (uint64(b) << shift)
}

//func casByte(addr *uint64, idx int, old, new uint8 /*, tryCount int*/) bool {
//	shift := idx << 3
//	clearMask := ^(uint64(0xff) << shift)
//	newVal := uint64(new) << shift
//	for /*tryCount != 0*/ {
//		cur := atomic.LoadUint64(addr)
//		if uint8((cur>>shift)&0xff) != old {
//			return false
//		}
//
//		nv := (cur & clearMask) | newVal
//		if atomic.CompareAndSwapUint64(addr, cur, nv) {
//			return true
//		}
//		/*tryCount--*/
//	}
//	/*return false*/
//}
//
//func getByte(w uint64, idx int) uint8 {
//	shift := idx << 3
//	return uint8((w >> shift) & 0xff)
//}
//
//func storeByte(addr *uint64, idx int, newByte uint8) {
//	shift := idx << 3
//	mask := uint64(0xff) << shift
//
//	for {
//		oldVal := atomic.LoadUint64(addr)
//		newVal := (oldVal &^ mask) | (uint64(newByte) << shift)
//		if atomic.CompareAndSwapUint64(addr, oldVal, newVal) {
//			return
//		}
//	}
//}

// nextPowOf2 calculates the smallest power of 2 that is greater than or equal to n.
// Compatible with both 32-bit and 64-bit systems.
func nextPowOf2(n int) int {
	if n <= 0 {
		return 1
	}

	if bits.UintSize == 32 {
		v := uint32(n)
		v--
		v |= v >> 1
		v |= v >> 2
		v |= v >> 4
		v |= v >> 8
		v |= v >> 16
		v++
		return int(v)
	}

	v := uint64(n)
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return int(v)
}

func delay(spins *int) {
	const yieldSleep = 500 * time.Microsecond
	if //goland:noinspection ALL
	enableSpin && runtime_canSpin(*spins) {
		runtime_doSpin()
		*spins++
	} else {
		// time.Sleep with non-zero duration (≈Millisecond level) works effectively
		// as backoff under high concurrency.
		time.Sleep(yieldSleep)
		*spins = 0
	}
}

func loadPointer(addr *unsafe.Pointer) unsafe.Pointer {
	if //goland:noinspection ALL
	atomicLevel == 0 {
		return atomic.LoadPointer(addr)
	} else {
		return *addr
	}
}

func loadUint64(addr *uint64) uint64 {
	if //goland:noinspection ALL
	atomicLevel == 0 {
		return atomic.LoadUint64(addr)
	} else {
		return *addr
	}
}

func storePointer(addr *unsafe.Pointer, val unsafe.Pointer) {
	if //goland:noinspection ALL
	atomicLevel < 2 {
		atomic.StorePointer(addr, val)
	} else {
		*addr = val
	}
}

func storeUint64(addr *uint64, val uint64) {
	if //goland:noinspection ALL
	atomicLevel < 2 {
		atomic.StoreUint64(addr, val)
	} else {
		*addr = val
	}
}

//func casPointer(addr *unsafe.Pointer, oldPtr, newPtr unsafe.Pointer) bool {
//	return atomic.CompareAndSwapPointer(addr, oldPtr, newPtr)
//}

func setOp(meta uint64, mask uint64, value bool) uint64 {
	if value {
		return meta | mask
	} else {
		return meta & ^mask
	}
}

func getOp(meta uint64, mask uint64) bool {
	return meta&mask != 0
}

func clearOp(meta uint64) uint64 {
	return meta & (^opByteMask)
}

//func loadOp(addr *uint64, mask uint64) bool {
//	cur := atomic.LoadUint64(addr)
//	return getOp(cur, mask)
//}

func casOp(addr *uint64, mask uint64, old, new bool) bool {
	for {
		cur := atomic.LoadUint64(addr)
		op := getOp(cur, mask)
		if op != old {
			return false
		}
		nv := setOp(cur, mask, new)
		if atomic.CompareAndSwapUint64(addr, cur, nv) {
			return true
		}
	}
}

func storeOp(addr *uint64, mask uint64, value bool) {
	if value {
		atomic.OrUint64(addr, mask)
	} else {
		atomic.AndUint64(addr, ^mask)
	}
}

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
// nolint:all
//
//go:nosplit
//goland:noinspection ALL
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

//// noEscapePtr hides a pointer from escape analysis. See noescape.
//// USE CAREFULLY!
////
//// nolint:all
////
////go:nosplit
////goland:noinspection ALL
//func noEscapePtr[T any](p *T) *T {
//	x := uintptr(unsafe.Pointer(p))
//	return (*T)(unsafe.Pointer(x ^ 0))
//}

// nolint:all
//
//go:linkname runtime_canSpin sync.runtime_canSpin
//go:nosplit
func runtime_canSpin(i int) bool

// nolint:all
//
//go:linkname runtime_doSpin sync.runtime_doSpin
//go:nosplit
func runtime_doSpin()

//// nolint:all
////
////go:linkname runtimeNano runtime.nanotime
////go:nosplit
//func runtimeNano() int64

type hashFunc func(unsafe.Pointer, uintptr) uintptr
type equalFunc func(unsafe.Pointer, unsafe.Pointer) bool

func defaultHasher[K comparable, V any]() (keyHash hashFunc, valEqual equalFunc, intKey bool) {
	keyHash, valEqual = defaultHasherUsingBuiltIn[K, V]()

	switch any(*new(K)).(type) {
	case uint, int, uintptr:
		return func(value unsafe.Pointer, seed uintptr) uintptr {
			return *(*uintptr)(value)
		}, valEqual, true

	case uint64, int64:
		if bits.UintSize == 32 {
			return func(value unsafe.Pointer, seed uintptr) uintptr {
				v := *(*uint64)(value)
				return uintptr(v) ^ uintptr(v>>32)
			}, valEqual, true
		}

		return func(value unsafe.Pointer, seed uintptr) uintptr {
			return uintptr(*(*uint64)(value))
		}, valEqual, true

	case uint32, int32:
		return func(value unsafe.Pointer, seed uintptr) uintptr {
			return uintptr(*(*uint32)(value))
		}, valEqual, true

	case uint16, int16:
		return func(value unsafe.Pointer, seed uintptr) uintptr {
			return uintptr(*(*uint16)(value))
		}, valEqual, true

	case uint8, int8:
		return func(value unsafe.Pointer, seed uintptr) uintptr {
			return uintptr(*(*uint8)(value))
		}, valEqual, true

	default:
		return keyHash, valEqual, false
	}
}

// defaultHasherUsingBuiltIn obtains Go's built-in hash and equality functions
// for the specified types using reflection.
//
// This approach provides direct access to the type-specific functions without
// the overhead of switch statements, resulting in better performance.
//
// Notes:
//   - This implementation relies on Go's internal type representation
//   - It should be verified for compatibility with each Go version upgrade
func defaultHasherUsingBuiltIn[K comparable, V any]() (keyHash hashFunc, valEqual equalFunc) {
	var m map[K]V
	mapType := iTypeOf(m).MapType()
	return mapType.Hasher, mapType.Elem.Equal
}

type iTFlag uint8
type iKind uint8
type iNameOff int32

// TypeOff is the offset to a type from moduledata.types.  See resolveTypeOff in runtime.
type iTypeOff int32

type iType struct {
	Size_       uintptr
	PtrBytes    uintptr // number of (prefix) bytes in the type that can contain pointers
	Hash        uint32  // hash of type; avoids computation in hash tables
	TFlag       iTFlag  // extra type information flags
	Align_      uint8   // alignment of variable with this type
	FieldAlign_ uint8   // alignment of struct field with this type
	Kind_       iKind   // enumeration for C
	// function for comparing objects of this type
	// (ptr to object A, ptr to object B) -> ==?
	Equal func(unsafe.Pointer, unsafe.Pointer) bool
	// GCData stores the GC type data for the garbage collector.
	// Normally, GCData points to a bitmask that describes the
	// ptr/nonptr fields of the type. The bitmask will have at
	// least PtrBytes/ptrSize bits.
	// If the TFlagGCMaskOnDemand bit is set, GCData is instead a
	// **byte and the pointer to the bitmask is one dereference away.
	// The runtime will build the bitmask if needed.
	// (See runtime/type.go:getGCMask.)
	// Note: multiple types may have the same value of GCData,
	// including when TFlagGCMaskOnDemand is set. The types will, of course,
	// have the same pointer layout (but not necessarily the same size).
	GCData    *byte
	Str       iNameOff // string form
	PtrToThis iTypeOff // type for pointer to this type, may be zero
}

func (t *iType) MapType() *iMapType {
	return (*iMapType)(unsafe.Pointer(t))
}

type iMapType struct {
	iType
	Key   *iType
	Elem  *iType
	Group *iType // internal type representing a slot group
	// function for hashing keys (ptr to key, seed) -> hash
	Hasher func(unsafe.Pointer, uintptr) uintptr
}

func iTypeOf(a any) *iType {
	eface := *(*iEmptyInterface)(unsafe.Pointer(&a))
	// Types are either static (for compiler-created types) or
	// heap-allocated but always reachable (for reflection-created
	// types, held in the central map). So there is no need to
	// escape types. noescape here help avoid unnecessary escape
	// of v.
	return (*iType)(noescape(unsafe.Pointer(eface.Type)))
}

type iEmptyInterface struct {
	Type *iType
	Data unsafe.Pointer
}

// Concurrency variable access rules:
//
// 1. If variable has atomic writes outside locks:
//    - Must use atomic loads AND stores inside locks
//    - Example:
//      var value int32
//      func update() {
//          atomic.StoreInt32(&value, 1) // external atomic write
//      }
//      func lockedOp() {
//          mu.Lock()
//          defer mu.Unlock()
//          v := atomic.LoadInt32(&value) // internal atomic load
//          atomic.StoreInt32(&value, v+1) // internal atomic store
//      }
//
// 2. If variable only has atomic reads outside locks:
//    - Only need atomic stores inside locks (atomic loads not required)
//    - Example:
//      func read() int32 {
//          return atomic.LoadInt32(&value) // external atomic read
//      }
//      func lockedOp() {
//          mu.Lock()
//          defer mu.Unlock()
//          // Normal read sufficient (lock guarantees visibility)
//          v := value
//          // But writes need atomic store:
//          atomic.StoreInt32(&value, 42)
//      }
//
// 3. If variable has no external access:
//    - No atomic operations needed inside locks
//    - Normal reads/writes sufficient (lock provides full protection)
//    - Example:
//      func lockedOp() {
//          mu.Lock()
//          defer mu.Unlock()
//          value = 42 // normal write
//          v := value // normal read
//      }
