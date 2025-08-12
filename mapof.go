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
	// opByteIdx reserves the highest byte of meta for extended status flags
	opByteIdx         = 7
	opByteMask uint64 = 0xff00000000000000
	opLockMask uint64 = 1 << (opByteIdx*8 + 7)

	// entriesPerMapOfBucket defines the number of entries per bucket.
	// Calculated to fit within a cache line, with a maximum of 8 entries
	// (upper limit supported by the meta-field).
	entriesPerMapOfBucket = min(
		opByteIdx,
		(int(CacheLineSize)-int(unsafe.Sizeof(struct {
			meta uint64
			// entries [entriesPerMapOfBucket]unsafe.Pointer
			next unsafe.Pointer
		}{})))/int(unsafe.Sizeof(unsafe.Pointer(nil))),
	)

	// Metadata constants for bucket entry management
	emptyMeta     uint64 = 0
	emptyMetaSlot uint8  = 0
	metaMask      uint64 = 0x8080808080808080 >>
		(64 - min(entriesPerMapOfBucket*8, 64))
	metaSlotMask uint8 = 0x80
)

// Performance and resizing configuration
const (
	// mapShrinkFraction: shrink table when occupancy < 1/mapShrinkFraction
	mapShrinkFraction = 8
	// mapLoadFactor: resize table when occupancy > mapLoadFactor
	mapLoadFactor = 0.75
	// defaultMinMapTableLen: minimum number of buckets
	defaultMinMapTableLen = 32
	// minBucketsPerGoroutine: threshold for parallel resizing
	minBucketsPerGoroutine = 4
	// minParallelBatchItems: threshold for parallel batch processing
	minParallelBatchItems = 256
	// asyncResizeThreshold: threshold for asynchronous resize
	asyncResizeThreshold = 128 * 1024 / CacheLineSize
)

// Feature flags for performance optimization
const (
	// enableFastPath: optimize read operations by avoiding locks when possible
	// Can reduce latency by up to 100x in read-heavy scenarios
	enableFastPath = true

	// enableHashSpread: improve hash distribution for non-integer keys
	// Reduces collisions for complex types but adds computational overhead
	enableHashSpread = false

	// enableSpin: use CPU PAUSE instruction for short waits
	// Improves performance for brief contention but may reduce throughput
	// under a high load
	enableSpin = true
)

// MapOf is a high-performance concurrent map implementation that is fully
// compatible with sync.Map API and significantly outperforms sync.Map in
// most scenarios.
//
// Core advantages:
//   - Lock-free reads, fine-grained locking for writes
//   - Zero-value ready with lazy initialization
//   - Custom hash and value comparison function support
//   - Rich batch operations and functional extensions
//
// Usage recommendations:
//   - Direct declaration: var m MapOf[string, int]
//   - Pre-allocate capacity: NewMapOf(WithPresize(1000))
//
// Notes:
//   - MapOf must not be copied after first use.
type MapOf[K comparable, V any] struct {
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		table         atomic.Pointer[mapOfTable]
		resizeState   atomic.Pointer[resizeState]
		totalGrowths  atomic.Uint32
		totalShrinks  atomic.Uint32
		seed          uintptr
		keyHash       hashFunc
		valEqual      equalFunc
		minTableLen   int
		shrinkEnabled bool
		intKey        bool
	}{})%CacheLineSize) % CacheLineSize]byte

	table         atomic.Pointer[mapOfTable]
	resizeState   atomic.Pointer[resizeState]
	totalGrowths  atomic.Uint32
	totalShrinks  atomic.Uint32
	seed          uintptr
	keyHash       hashFunc  // WithKeyHasher
	valEqual      equalFunc // WithValueEqual
	minTableLen   int       // WithPresize
	shrinkEnabled bool      // WithShrinkEnabled
	intKey        bool
}

// NewMapOf creates a new MapOf instance. Direct initialization is also
// supported.
//
// Parameters:
//   - options: configuration options (WithPresize, WithKeyHasher, etc.)
func NewMapOf[K comparable, V any](
	options ...func(*MapConfig),
) *MapOf[K, V] {
	m := &MapOf[K, V]{}
	m.InitWithOptions(options...)
	return m
}

// MapConfig defines configurable options for MapOf initialization.
// This structure contains all the configuration parameters that can be used
// to customize the behavior and performance characteristics of a MapOf
// instance.
type MapConfig struct {
	// KeyHash specifies a custom hash function for keys.
	// If nil, the built-in hash function will be used.
	// Custom hash functions can improve performance for specific key types
	// or provide better hash distribution.
	KeyHash hashFunc

	// ValEqual specifies a custom equality function for values.
	// If nil, the built-in equality comparison will be used.
	// This is primarily used for compare-and-swap operations.
	// Note: Using Compare* methods with non-comparable value types
	// will panic if ValEqual is nil.
	ValEqual equalFunc

	// SizeHint provides an estimate of the expected number of entries.
	// This is used to pre-allocate the underlying hash table with appropriate
	// capacity, reducing the need for resizing during initial population.
	// If zero or negative, the default minimum capacity will be used.
	// The actual capacity will be rounded up to the next power of 2.
	SizeHint int

	// ShrinkEnabled controls whether the map can automatically shrink
	// when the load factor falls below the shrink threshold.
	// When false (default), the map will only grow and never shrink,
	// which provides better performance but may use more memory.
	// When true, the map will shrink when occupancy < 1/mapShrinkFraction.
	ShrinkEnabled bool
}

// WithPresize configuring new MapOf instance with capacity enough
// to hold sizeHint entries. The capacity is treated as the minimal
// capacity, meaning that the underlying hash table will never shrink
// to a smaller capacity. If sizeHint is zero or negative, the value
// is ignored.
func WithPresize(sizeHint int) func(*MapConfig) {
	return func(c *MapConfig) {
		c.SizeHint = sizeHint
	}
}

// WithShrinkEnabled configures automatic map shrinking when the load factor
// falls below the threshold (default: 1/mapShrinkFraction).
// Disabled by default to prioritize performance.
func WithShrinkEnabled() func(*MapConfig) {
	return func(c *MapConfig) {
		c.ShrinkEnabled = true
	}
}

// WithKeyHasher sets a custom key hashing function for the map.
// This allows you to optimize hash distribution for specific key types
// or implement custom hashing strategies.
//
// Parameters:
//   - keyHash: custom hash function that takes a key and seed,
//     returns hash value Pass nil to use the default built-in hasher
//
// Usage:
//
//	m := NewMapOf[string, int](WithKeyHasher(myCustomHashFunc))
//
// Use cases:
//   - Optimize hash distribution for specific data patterns
//   - Implement case-insensitive string hashing
//   - Custom hashing for complex key types
//   - Performance tuning for known key distributions
func WithKeyHasher[K comparable](
	keyHash func(key K, seed uintptr) uintptr,
) func(*MapConfig) {
	return func(c *MapConfig) {
		if keyHash != nil {
			c.KeyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
				return keyHash(*(*K)(pointer), u)
			}
		}
	}
}

// WithKeyHasherUnsafe sets a low-level unsafe key hashing function.
// This is the high-performance version that operates directly on memory
// pointers. Use this when you need maximum performance and are comfortable with
// unsafe operations.
//
// Parameters:
//   - hs: unsafe hash function that operates on raw unsafe.Pointer
//     The pointer points to the key data in memory
//     Pass nil to use the default built-in hasher
//
// Usage:
//
//	unsafeHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
//		// Cast ptr to your key type and implement hashing
//		key := *(*string)(ptr)
//		return uintptr(len(key)) // example hash
//	}
//	m := NewMapOf[string, int](WithKeyHasherUnsafe(unsafeHasher))
//
// ⚠️  SAFETY WARNING:
//   - You must correctly cast unsafe.Pointer to the actual key type
//   - Incorrect pointer operations will cause crashes or memory corruption
//   - Only use if you understand Go's unsafe package
func WithKeyHasherUnsafe(hs hashFunc) func(*MapConfig) {
	return func(c *MapConfig) {
		c.KeyHash = hs
	}
}

// WithValueEqual sets a custom value equality function for the map.
// This is essential for CompareAndSwap and CompareAndDelete operations
// when working with non-comparable value types or custom equality logic.
//
// Parameters:
//   - valEqual: custom equality function that compares two values
//     Pass nil to use the default built-in comparison (for comparable types)
//
// Usage:
//
//	// For custom structs with specific equality logic
//	equalFunc := func(a, b MyStruct) bool {
//		return a.ID == b.ID && a.Name == b.Name
//	}
//	m := NewMapOf[string, MyStruct](WithValueEqual(equalFunc))
//
// Use cases:
//   - Custom equality for structs (compare specific fields)
//   - Case-insensitive string comparison
//   - Floating-point comparison with tolerance
//   - Deep equality for slices/maps
//   - Required for non-comparable types (slices, maps, functions)
func WithValueEqual[V any](
	valEqual func(val, val2 V) bool,
) func(*MapConfig) {
	return func(c *MapConfig) {
		if valEqual != nil {
			c.ValEqual = func(val unsafe.Pointer, val2 unsafe.Pointer) bool {
				return valEqual(*(*V)(val), *(*V)(val2))
			}
		}
	}
}

// WithValueEqualUnsafe sets a low-level unsafe value equality function.
// This is the high-performance version that operates directly on memory
// pointers. Use this when you need maximum performance and are comfortable with
// unsafe operations.
//
// Parameters:
//   - eq: unsafe equality function that operates on raw unsafe.Pointer
//     Both pointers point to value data in memory
//     Pass nil to use the default built-in comparison
//
// Usage:
//
//	unsafeEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
//		// Cast pointers to your value type and implement comparison
//		val1 := *(*MyStruct)(ptr1)
//		val2 := *(*MyStruct)(ptr2)
//		return val1.ID == val2.ID // example comparison
//	}
//	m := NewMapOf[string, MyStruct](WithValueEqualUnsafe(unsafeEqual))
//
// ⚠️  SAFETY WARNING:
//   - You must correctly cast unsafe.Pointer to the actual value type
//   - Both pointers must point to valid memory of the same type
//   - Incorrect pointer operations will cause crashes or memory corruption
//   - Only use if you understand Go's unsafe package
func WithValueEqualUnsafe(eq equalFunc) func(*MapConfig) {
	return func(c *MapConfig) {
		c.ValEqual = eq
	}
}

// bucketOf represents a hash table bucket with cache-line alignment.
type bucketOf struct {
	// meta: SWAR-optimized metadata for fast entry lookups
	// (must be 64-bit aligned)
	meta uint64

	// Cache line padding to prevent false sharing
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		meta    uint64
		entries [entriesPerMapOfBucket]unsafe.Pointer
		next    unsafe.Pointer
	}{})%CacheLineSize) % CacheLineSize]byte

	entries [entriesPerMapOfBucket]unsafe.Pointer // *EntryOf
	next    unsafe.Pointer                        // *bucketOf
}

// lock acquires a spinlock for the bucket using embedded metadata.
// Uses atomic operations on the meta field to avoid false sharing overhead.
// Implements optimistic locking with fallback to spinning.
//
//go:nosplit
func (b *bucketOf) lock() {
	cur := atomic.LoadUint64(&b.meta)
	if atomic.CompareAndSwapUint64(&b.meta, cur&(^opLockMask), cur|opLockMask) {
		return
	}
	b.slowLock()
}

//go:nosplit
func (b *bucketOf) slowLock() {
	spins := 0
	for !b.tryLock() {
		delay(&spins)
	}
}

//go:nosplit
func (b *bucketOf) tryLock() bool {
	return casOp(&b.meta, opLockMask, false, true)
}

//go:nosplit
func (b *bucketOf) unlock() {
	atomic.StoreUint64(&b.meta, b.meta&(^opLockMask))
}

// resizeState represents the current state of a resizing operation
type resizeState struct {
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		wg        sync.WaitGroup
		table     atomic.Pointer[mapOfTable]
		newTable  atomic.Pointer[mapOfTable]
		process   atomic.Int32
		completed atomic.Int32
	}{})%CacheLineSize) % CacheLineSize]byte

	wg        sync.WaitGroup
	table     atomic.Pointer[mapOfTable]
	newTable  atomic.Pointer[mapOfTable]
	process   atomic.Int32
	completed atomic.Int32
}

// mapOfTable represents the internal hash table structure.
type mapOfTable struct {
	//lint:ignore U1000 prevents false sharing
	pad [(CacheLineSize - unsafe.Sizeof(struct {
		buckets   []bucketOf
		size      []counterStripe
		chunks    int
		chunkSize int
	}{})%CacheLineSize) % CacheLineSize]byte

	buckets []bucketOf
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	// when the compile option `mapof_opt_enablepadding` is enabled
	size []counterStripe
	// number of chunks and chunks size for resizing
	chunks    int
	chunkSize int
}

func newMapOfTable(tableLen, cpus int) *mapOfTable {
	chunkSize, chunks := calcParallelism(tableLen, minBucketsPerGoroutine, cpus)
	return &mapOfTable{
		buckets:   make([]bucketOf, tableLen),
		size:      make([]counterStripe, calcSizeLen(tableLen, cpus)),
		chunks:    chunks,
		chunkSize: chunkSize,
	}
}

// addSize atomically adds delta to the size counter for the given bucket index.
//
//go:nosplit
func (table *mapOfTable) addSize(bucketIdx uintptr, delta int) {
	cidx := uintptr(len(table.size)-1) & bucketIdx
	table.size[cidx].c.Add(uintptr(delta))
}

// // addSizePlain adds delta to the size counter without atomic operations.
// // This method should only be used when thread safety is guaranteed by the
// // caller.
//func (table *mapOfTable) addSizePlain(bucketIdx uintptr, delta int) {
//	cidx := uintptr(len(table.size)-1) & bucketIdx
//	table.size[cidx].c += uintptr(delta)
//}

// sumSize calculates the total number of entries in the table
// by summing all counter-stripes.
//
//go:nosplit
func (table *mapOfTable) sumSize() int {
	var sum int
	for i := range table.size {
		sum += int(table.size[i].c.Load())
	}
	return sum
}

// isZero checks if the table is empty by verifying all counter-stripes are
// zero.
//
//go:nosplit
func (table *mapOfTable) isZero() bool {
	for i := range table.size {
		if table.size[i].c.Load() != 0 {
			return false
		}
	}
	return true
}

// InitWithOptions initializes the MapOf instance using variadic option
// parameters. This is a convenience method that allows configuring MapOf
// through the functional options pattern.
//
// Parameters:
//   - options: configuration option functions such as WithPresize,
//     WithShrinkEnabled, WithKeyHasher, WithValueEqual, etc.
//
// Usage example:
//
//	m.InitWithOptions(WithPresize(1000), WithShrinkEnabled())
//
// Notes:
//   - This function is not thread-safe and should only be called before MapOf
//     is used
//   - If this function is not called, MapOf will use default configuration
//   - The behavior of calling this function multiple times is undefined
func (m *MapOf[K, V]) InitWithOptions(
	options ...func(*MapConfig),
) {
	c := &MapConfig{}
	for _, o := range options {
		o(c)
	}
	m.init(c)
}

// initWithConfig initializes the MapOf instance using a pre-built MapConfig.
// This is a more direct initialization method, suitable for scenarios where the
// same configuration needs to be reused.
//
// Parameters:
//   - config: a pre-configured MapConfig instance containing all necessary
//     configuration parameters
//
// Usage example:
//
//	config := &MapConfig{
//	    SizeHint: 1000,
//	    ShrinkEnabled: true,
//	}
//	m.initWithConfig(config)
//
// Notes:
//   - This function is not thread-safe and should only be called before MapOf
//     is used
//   - The passed MapConfig will be used directly without copying
//   - The behavior of calling this function multiple times is undefined
func (m *MapOf[K, V]) initWithConfig(
	config *MapConfig,
) {
	m.init(config)
}

func (m *MapOf[K, V]) init(
	c *MapConfig,
) *mapOfTable {
	m.seed = uintptr(rand.Uint64())
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	if c.KeyHash != nil {
		m.keyHash = c.KeyHash
	}
	if c.ValEqual != nil {
		m.valEqual = c.ValEqual
	}

	m.minTableLen = calcTableLen(c.SizeHint)
	m.shrinkEnabled = c.ShrinkEnabled

	table := newMapOfTable(m.minTableLen, runtime.GOMAXPROCS(0))
	m.table.Store(table)
	return table
}

// initSlow may be called concurrently by multiple goroutines, so it requires
// synchronization with a "lock" mechanism.
//
//go:noinline
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
	c := &MapConfig{}
	table = m.init(c)
	m.table.Store(table)
	m.resizeState.Store(nil)
	rs.wg.Done()
	return table
}

// Load retrieves a value for the given key, compatible with `sync.Map`.
//
//go:nosplit
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
	for b := &table.buckets[bidx]; b != nil; b = (*bucketOf)(loadPointer(&b.next)) {
		metaw := loadUint64(&b.meta)
		for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := (*EntryOf[K, V])(loadPointer(&b.entries[idx])); e != nil {
				if embeddedHash {
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
	}
	return
}

// LoadEntry finds and returns the entry pointer for the given key.
// Returns nil if the key is not found or the map is uninitialized.
//
// Notes:
//   - Never modify the Key or Value in an Entry under any circumstances.
//
//go:nosplit
func (m *MapOf[K, V]) LoadEntry(key K) *EntryOf[K, V] {
	table := m.table.Load()
	if table == nil {
		return nil
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	return m.findEntry(table, hash, &key)
}

//go:nosplit
func (m *MapOf[K, V]) findEntry(
	table *mapOfTable,
	hash uintptr,
	key *K,
) *EntryOf[K, V] {
	h2v := h2(hash)
	h2w := broadcast(h2v)
	bidx := uintptr(len(table.buckets)-1) & h1(hash, m.intKey)
	for b := &table.buckets[bidx]; b != nil; b = (*bucketOf)(loadPointer(&b.next)) {
		metaw := loadUint64(&b.meta)
		for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := (*EntryOf[K, V])(loadPointer(&b.entries[idx])); e != nil {
				if embeddedHash {
					if e.getHash() == hash && e.Key == *key {
						return e
					}
				} else {
					if e.Key == *key {
						return e
					}
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

		// This is the first check, checking if there is a resize operation in
		// progress before acquiring the bucket lock
		if rs := m.resizeState.Load(); rs != nil &&
			rs.table.Load() != nil /*skip init*/ &&
			rs.newTable.Load() != nil /*skip if newTable is nil */ {
			rootb.unlock()
			// Wait for the current resize operation to complete
			m.helpCopyAndWait(rs)
			table = m.table.Load()
			continue
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
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

	findLoop:
		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			metaw := b.meta
			for markedw := markZeroBytes(metaw ^ h2w); markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(b.entries[idx]); e != nil {
					if embeddedHash {
						if e.getHash() == hash && e.Key == *key {
							oldEntry = e
							oldBucket = b
							oldIdx = idx
							oldMeta = metaw
							break findLoop
						}
					} else {
						if e.Key == *key {
							oldEntry = e
							oldBucket = b
							oldIdx = idx
							oldMeta = metaw
							break findLoop
						}
					}
				}
			}
			if emptyBucket == nil {
				if emptyw := (^metaw) & metaMask; emptyw != 0 {
					emptyBucket = b
					emptyIdx = firstMarkedByteIndex(emptyw)
				}
			}
			lastBucket = b
		}

		// --- Processing Logic ---
		newEntry, value, status := fn(oldEntry)

		if oldEntry != nil {
			if newEntry == oldEntry {
				// No entry to update or delete
				rootb.unlock()
				return value, status
			}
			if newEntry != nil {
				// Update
				if embeddedHash {
					newEntry.setHash(hash)
				}
				newEntry.Key = *key
				storePointer(
					&oldBucket.entries[oldIdx],
					unsafe.Pointer(newEntry),
				)
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
			if m.shrinkEnabled && clearOp(newmetaw) == emptyMeta &&
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

		if newEntry == nil {
			// No entry to insert or delete
			rootb.unlock()
			return value, status
		}

		// Insert
		if embeddedHash {
			newEntry.setHash(hash)
		}
		newEntry.Key = *key
		if emptyBucket != nil {
			storeUint64(
				&emptyBucket.meta,
				setByte(emptyBucket.meta, h2v, emptyIdx),
			)
			storePointer(
				&emptyBucket.entries[emptyIdx],
				unsafe.Pointer(newEntry),
			)
			rootb.unlock()
			table.addSize(bidx, 1)
			return value, status
		}

		// No empty slot, create new bucket and insert
		storePointer(&lastBucket.next, unsafe.Pointer(&bucketOf{
			meta: setByte(emptyMeta, h2v, 0),
			entries: [entriesPerMapOfBucket]unsafe.Pointer{
				unsafe.Pointer(newEntry),
			},
		}))
		rootb.unlock()
		table.addSize(bidx, 1)

		// Check if the table needs to grow
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
}

type mapResizeHint int

const (
	mapGrowHint   mapResizeHint = 0
	mapShrinkHint mapResizeHint = 1
	mapClearHint  mapResizeHint = 2
)

func (m *MapOf[K, V]) resize(
	hint mapResizeHint,
	sizeAdd int,
) {
	var size int
	for {
		// Resize check
		table := m.table.Load()
		tableLen := len(table.buckets)
		switch hint {
		case mapGrowHint:
			if sizeAdd <= 0 {
				return
			}
			size = table.sumSize()
			newTableLen := calcTableLen(size + sizeAdd)
			if tableLen >= newTableLen {
				return
			}
		case mapShrinkHint:
			if tableLen <= m.minTableLen {
				return
			}
			// Recalculate the shrink size to avoid over-shrinking
			size = table.sumSize()
			newTableLen := calcTableLen(size)
			if tableLen <= newTableLen {
				return
			}
		default:
			if tableLen == m.minTableLen && table.isZero() {
				return
			}
		}
		// Wait resize finish
		if rs := m.resizeState.Load(); rs != nil {
			if rs.table.Load() != nil /*skip init*/ &&
				rs.newTable.Load() != nil /*skip if newTable is nil */ {
				m.helpCopyAndWait(rs)
			} else {
				rs.wg.Wait()
			}
			continue
		}

		m.tryResize(hint, size, sizeAdd)
	}
}

//go:noinline
func (m *MapOf[K, V]) tryResize(
	hint mapResizeHint,
	size, sizeAdd int,
) {
	// Create a new WaitGroup for the current resize operation
	rs := new(resizeState)
	rs.wg.Add(1)

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !m.resizeState.CompareAndSwap(nil, rs) {
		return
	}

	// Resize start
	cpus := runtime.GOMAXPROCS(0)
	if hint == mapClearHint {
		newTable := newMapOfTable(m.minTableLen, cpus)
		m.table.Store(newTable)
		m.resizeState.Store(nil)
		rs.wg.Done()
		return
	}

	table := m.table.Load()
	tableLen := len(table.buckets)
	var newTableLen int
	if hint == mapGrowHint {
		if sizeAdd == 0 {
			newTableLen = max(calcTableLen(size), tableLen<<1)
		} else {
			newTableLen = calcTableLen(size + sizeAdd)
			if newTableLen <= tableLen {
				m.resizeState.Store(nil)
				rs.wg.Done()
				return
			}
		}
		m.totalGrowths.Add(1)
	} else {
		if sizeAdd == 0 {
			newTableLen = tableLen >> 1
		} else {
			newTableLen = calcTableLen(size)
		}
		if newTableLen < m.minTableLen {
			m.resizeState.Store(nil)
			rs.wg.Done()
			return
		}
		m.totalShrinks.Add(1)
	}

	if newTableLen >= int(asyncResizeThreshold) && cpus > 1 {
		// The big table, use goroutines to create new table and copy entries
		go m.finalizeResize(table, newTableLen, rs, cpus)
	} else {
		m.finalizeResize(table, newTableLen, rs, cpus)
	}
}

func (m *MapOf[K, V]) finalizeResize(
	table *mapOfTable,
	newTableLen int,
	rs *resizeState,
	cpus int,
) {
	rs.table.Store(table)
	newTable := newMapOfTable(newTableLen, cpus)
	rs.newTable.Store(newTable)
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *MapOf[K, V]) helpCopyAndWait(rs *resizeState) {
	table := rs.table.Load()
	tableLen := len(table.buckets)
	chunks := int32(table.chunks)
	chunkSize := table.chunkSize
	newTable := rs.newTable.Load()
	isGrowth := len(newTable.buckets) > tableLen
	for {
		process := rs.process.Add(1)
		if process > chunks {
			// Wait copying completed
			rs.wg.Wait()
			return
		}
		process--
		start := int(process) * chunkSize
		end := min(start+chunkSize, tableLen)
		if isGrowth {
			copyBucketOf[K, V](
				table,
				start,
				end,
				newTable,
				m.keyHash,
				m.seed,
				m.intKey,
			)
		} else {
			copyBucketOfLock[K, V](table, start, end, newTable, m.keyHash, m.seed, m.intKey)
		}
		if rs.completed.Add(1) == chunks {
			// Copying completed
			m.table.Store(newTable)
			m.resizeState.Store(nil)
			rs.wg.Done()
			return
		}
	}
}

// copyBucketOfLock, unlike copyBucketOf, it locks the destination bucket to
// ensure concurrency safety.
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
				if e := (*EntryOf[K, V])(b.entries[firstMarkedByteIndex(markedw)]); e != nil {
					var hash uintptr
					if embeddedHash {
						hash = e.getHash()
					} else {
						hash = hasher(noescape(unsafe.Pointer(&e.Key)), seed)
					}
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
				if e := (*EntryOf[K, V])(b.entries[firstMarkedByteIndex(markedw)]); e != nil {
					var hash uintptr
					if embeddedHash {
						hash = e.getHash()
					} else {
						hash = hasher(noescape(unsafe.Pointer(&e.Key)), seed)
					}
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
		// copyBucketOf is used during multithreaded growth, requiring a
		// thread-safe addSize.
		destTable.addSize(uintptr(start), copied)
	}
}

// RangeProcessEntry iterates through all map entries while holding the
// bucket lock, applying fn to each entry. The iteration is thread-safe
// due to bucket-level locking.
//
// The fn callback (with the same signature as ProcessEntry) controls entry
// modification:
//   - Return modified entry: updates the value
//   - Return nil: deletes the entry
//   - Return original entry: no change
//
// Ideal for batch operations requiring atomic read-modify-write semantics.
//
// Parameters:
//   - fn: callback that processes each entry and returns an operation indicator
//
// Notes:
//
//   - The input parameter loaded is immutable and should not be modified
//     directly
//
//   - Holds bucket lock for entire iteration - avoid long operations/deadlock
//     risks
//
//   - Blocks concurrent map operations during execution
func (m *MapOf[K, V]) RangeProcessEntry(
	fn func(loaded *EntryOf[K, V]) *EntryOf[K, V],
) {
	table := m.table.Load()
	if table == nil {
		return
	}

	for bidx := range table.buckets {
		rootb := &table.buckets[bidx]
		rootb.lock()
		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			metaw := b.meta
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				i := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(b.entries[i]); e != nil {

					newEntry := fn(e)

					if newEntry == e {
						// No entry to update or delete
					} else if newEntry != nil {
						// Update
						newEntry.Key = e.Key
						storePointer(&b.entries[i], unsafe.Pointer(newEntry))
					} else {
						// Delete
						newmetaw := setByte(metaw, emptyMetaSlot, i)
						storeUint64(&b.meta, newmetaw)
						storePointer(&b.entries[i], nil)
						table.addSize(uintptr(bidx), -1)
					}
				}
			}
		}
		rootb.unlock()
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
			if e := m.findEntry(table, hash, &key); e != nil {
				if m.valEqual(
					noescape(unsafe.Pointer(&e.Value)),
					noescape(unsafe.Pointer(&value)),
				) {
					return
				}
			}
		}
	}

	m.processEntry(table, hash, &key,
		func(*EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			return &EntryOf[K, V]{Value: value}, *new(V), false
		},
	)
}

// Swap stores a key-value pair and returns the previous value if any,
// compatible with `sync.Map`.
func (m *MapOf[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)

	if enableFastPath {
		// deduplicates identical values
		if m.valEqual != nil {
			if e := m.findEntry(table, hash, &key); e != nil {
				if m.valEqual(
					noescape(unsafe.Pointer(&e.Value)),
					noescape(unsafe.Pointer(&value)),
				) {
					return value, true
				}
			}
		}
	}

	return m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: value}, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: value}, *new(V), false
		},
	)
}

// LoadOrStore retrieves an existing value or stores a new one if the key
// doesn't exist, compatible with `sync.Map`.
func (m *MapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		table = m.initSlow()
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
			return &EntryOf[K, V]{Value: value}, value, false
		},
	)
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

	m.processEntry(table, hash, &key,
		func(*EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			return nil, *new(V), false
		},
	)
}

// LoadAndDelete retrieves the value for a key and deletes it from the map.
// compatible with `sync.Map`.
func (m *MapOf[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
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

	return m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return nil, loaded.Value, true
			}
			return nil, *new(V), false
		},
	)
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

		if !m.valEqual(
			noescape(unsafe.Pointer(&e.Value)),
			noescape(unsafe.Pointer(&old)),
		) {
			return false
		}
		// deduplicates identical values
		if m.valEqual(
			noescape(unsafe.Pointer(&e.Value)),
			noescape(unsafe.Pointer(&new)),
		) {
			return true
		}
	}

	_, swapped = m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			var zero V
			if loaded != nil &&
				m.valEqual(
					noescape(unsafe.Pointer(&loaded.Value)),
					noescape(unsafe.Pointer(&old)),
				) {
				return &EntryOf[K, V]{Value: new}, zero, true
			}
			return loaded, zero, false
		},
	)
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
		if !m.valEqual(
			noescape(unsafe.Pointer(&e.Value)),
			noescape(unsafe.Pointer(&old)),
		) {
			return false
		}
	}

	_, deleted = m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			var zero V
			if loaded != nil &&
				m.valEqual(
					noescape(unsafe.Pointer(&loaded.Value)),
					noescape(unsafe.Pointer(&old)),
				) {
				return nil, zero, true
			}
			return loaded, zero, false
		},
	)
	return deleted
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

// LoadAndUpdate retrieves the value associated with the given key and updates
// it if the key exists.
//
// Parameters:
//   - key: The key to look up in the map.
//   - value: The new value to set if the key exists.
//
// Returns:
//
//   - previous: The old value associated with the key (if it existed),
//     otherwise a zero-value of V.
//
//   - loaded: True if the key existed and the value was updated,
//     false otherwise.
func (m *MapOf[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
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

		// deduplicates identical values
		if m.valEqual != nil {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.Value)),
				noescape(unsafe.Pointer(&value)),
			) {
				return value, true
			}
		}
	}

	return m.processEntry(table, hash, &key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: value}, loaded.Value, true
			}
			return nil, *(new(V)), false
		},
	)
}

// LoadAndStore returns the existing value for the key if present, while setting
// the new value for the key.
// It stores the new value and returns the existing one, if present.
// The loaded result is true if the existing value was loaded, false otherwise.
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
			if e := m.findEntry(table, hash, &key); e != nil &&
				m.valEqual(
					noescape(unsafe.Pointer(&e.Value)),
					noescape(unsafe.Pointer(&value)),
				) {
				return value, true
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
// altogether. And finally, if the op is [CancelOp], then the
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
					// Since we're already inside the lock
					// (where overhead is inevitable),
					// it's better to let users handle same-value filtering
					// with CancelOp instead.
					if enableFastPath {
						if m.valEqual != nil &&
							m.valEqual(
								noescape(unsafe.Pointer(&loaded.Value)),
								noescape(unsafe.Pointer(&newValue)),
							) {
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
			var zeroV V
			newValue, op := valueFn(zeroV, false)
			if op == UpdateOp {
				return &EntryOf[K, V]{Value: newValue}, newValue, true
			}
			return nil, zeroV, false
		},
	)
}

// LoadOrProcessEntry loads an existing value or computes a new one using
// the provided function.
//
// If the key exists, its value is returned directly.
// If the key doesn't exist, the provided function fn is called to compute a new
// value.
//
// Parameters:
//   - key: The key to look up or process
//   - valueFn: Only called when the value does not exist, return values are
//     described below:
//     Return *EntryOf[K, V]: New entry, nil means don't store any value.
//     Return V: value it to return to the caller.
//     Return bool: Whether the operation succeeded.
//
// Returns:
//   - value V: Existing value if key exists;
//     otherwise the value returned by valueFn
//   - loaded bool: true if key exists;
//     otherwise the bool value returned by valueFn
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
// This method is the foundation for all modification operations in MapOf.
// It provides. Complete control over key-value pairs, allowing atomic reading,
// modification, deletion, or insertion of entries.
//
// Parameters:
//
//   - key: The key to process
//
//   - fn: Called regardless of value existence; parameters and return values
//     are described below:
//     loaded *EntryOf[K, V]: current entry (nil if key doesn't exist),
//     return *EntryOf[K, V]:  nil to delete, new entry to store; ==loaded
//     for no modification,
//     return V: value to be returned as ProcessEntry's value;
//     return bool: status to be returned as ProcessEntry's status indicator
//
// Returns:
//   - value V: First return value from fn
//   - status bool: Second return value from fn
//
// Notes:
//   - The input parameter loaded is immutable and should not be modified
//     directly.
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
	m.resize(mapClearHint, 0)
}

// Grow increases the map's capacity by sizeAdd entries to accommodate future
// growth. This pre-allocation avoids rehashing when adding new entries up to
// the new capacity.
//
// Parameters:
//   - sizeAdd specifies the number of additional entries the map should be able
//     to hold.
//
// Notes:
//   - If the current remaining capacity already exceeds sizeAdd, no growth will
//     be triggered.
func (m *MapOf[K, V]) Grow(sizeAdd int) {
	if sizeAdd <= 0 {
		return
	}
	if m.table.Load() == nil {
		m.initSlow()
	}
	m.resize(mapGrowHint, sizeAdd)
}

// Shrink reduces the capacity to fit the current size,
// always executes regardless of WithShrinkEnabled.
func (m *MapOf[K, V]) Shrink() {
	table := m.table.Load()
	if table == nil {
		return
	}
	m.resize(mapShrinkHint, -1)
}

// RangeEntry iterates over all entries in the map.
//
// Notes:
//
//   - Never modify the Key or Value in an Entry under any circumstances.
//
//   - The iteration directly traverses bucket data. The data is not guaranteed
//     to be real-time but provides eventual consistency.
//     In extreme cases, the same value may be traversed twice
//     (if it gets deleted and re-added later during iteration).
func (m *MapOf[K, V]) RangeEntry(yield func(e *EntryOf[K, V]) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}

	for i := range table.buckets {
		for b := &table.buckets[i]; b != nil; b = (*bucketOf)(loadPointer(&b.next)) {
			metaw := loadUint64(&b.meta)
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				if e := (*EntryOf[K, V])(loadPointer(&b.entries[firstMarkedByteIndex(markedw)])); e != nil {
					if !yield(e) {
						return
					}
				}
			}
		}
	}
}

// All compatible with `sync.Map`.
//
//go:nosplit
func (m *MapOf[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Keys is the iterator version for iterating over all keys.
//
//go:nosplit
func (m *MapOf[K, V]) Keys() func(yield func(K) bool) {
	return m.RangeKeys
}

// Values is the iterator version for iterating over all values.
//
//go:nosplit
func (m *MapOf[K, V]) Values() func(yield func(V) bool) {
	return m.RangeValues
}

// Range compatible with `sync.Map`.
//
//go:nosplit
func (m *MapOf[K, V]) Range(yield func(key K, value V) bool) {
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		return yield(e.Key, e.Value)
	})
}

// RangeKeys to iterate over all keys
//
//go:nosplit
func (m *MapOf[K, V]) RangeKeys(yield func(key K) bool) {
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		return yield(e.Key)
	})
}

// RangeValues to iterate over all values
//
//go:nosplit
func (m *MapOf[K, V]) RangeValues(yield func(value V) bool) {
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		return yield(e.Value)
	})
}

// Size returns the number of key-value pairs in the map.
// This is an O(1) operation.
//
// Compatible with `xsync.MapOf`.
//
//go:nosplit
func (m *MapOf[K, V]) Size() int {
	table := m.table.Load()
	if table == nil {
		return 0
	}
	return table.sumSize()
}

// IsZero checks zero values, faster than Size().
//
//go:nosplit
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

// ToMapWithLimit collect up to limit entries into a map[K]V, limit < 0 is no
// limit
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
//
//go:nosplit
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
	return strings.Replace(
		fmt.Sprint(m.ToMapWithLimit(limit)),
		"map[",
		"MapOf[",
		1,
	)
}

var (
	jsonMarshal   func(v any) ([]byte, error)
	jsonUnmarshal func(data []byte, v any) error
)

// SetDefaultJSONMarshal sets the default JSON serialization and deserialization
// functions. If not set, the standard library is used by default.
func SetDefaultJSONMarshal(
	marshal func(v any) ([]byte, error),
	unmarshal func(data []byte, v any) error,
) {
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

// BatchProcess processes multiple key-value pairs from an iterator with a
// custom function.
//
// This function provides a convenient way to a batch process key-value pairs
// using Go's iterator pattern (seq2 function). It's particularly useful for
// processing data from
// external sources like databases, files, or other iterables.
//
// Parameters:
//
//   - seq2: Iterator function that yields key-value pairs to process.
//     The iterator should call yield(key, value) for each pair and
//     return true to continue. - processFn: Function that receives key, value,
//     and current entry (if exists), returns new entry,
//     result value, and status.
//
//   - growSize: Optional capacity pre-allocation hint. If provided, the map
//     will grow by this amount before processing to reduce resize overhead.
//
// Notes:
//
//   - The function processes items sequentially as yielded by the iterator.
//
//   - Unlike batch functions that return slices, this function processes items
//     immediately without collecting results.
//
//   - Pre-growing the map with growSize can improve performance for large
//     datasets by avoiding multiple resize operations during processing.
//
//   - The processFn follows the same signature as other ProcessEntry functions,
//     allowing for insert, update, delete, or conditional operations.
func (m *MapOf[K, V]) BatchProcess(
	seq2 func(yield func(K, V) bool),
	processFn func(key K, value V, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
	growSize ...int,
) {
	if len(growSize) != 0 {
		m.Grow(growSize[0])
	}
	seq2(func(key K, value V) bool {
		m.ProcessEntry(key,
			func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				return processFn(key, value, loaded)
			},
		)
		return true
	})
}

// BatchProcessImmutableEntries batch processes multiple immutable key-value
// pairs with a custom function.
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
	return m.batchProcessImmutableEntries(
		immutableEntries,
		int(float64(len(immutableEntries))*growFactor),
		processFn,
	)
}

func (m *MapOf[K, V]) batchProcessImmutableEntries(
	immutableEntries []*EntryOf[K, V],
	growSize int,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	if len(immutableEntries) == 0 {
		return
	}

	values = make([]V, len(immutableEntries))
	status = make([]bool, len(immutableEntries))

	m.Grow(growSize)
	parallelProcess(len(immutableEntries), func(start, end int) {
		for i := start; i < end; i++ {
			values[i], status[i] = m.ProcessEntry(immutableEntries[i].Key,
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(immutableEntries[i], loaded)
				},
			)
		}
	})

	return
}

// BatchProcessEntries batch processes multiple key-value pairs with a custom
// function
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
//     as this would prevent the entire `entries` from being garbage collected
//     in a timely manner.
func (m *MapOf[K, V]) BatchProcessEntries(
	entries []EntryOf[K, V],
	growFactor float64,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	return m.batchProcessEntries(
		entries,
		int(float64(len(entries))*growFactor),
		processFn,
	)
}

func (m *MapOf[K, V]) batchProcessEntries(
	entries []EntryOf[K, V],
	growSize int,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	if len(entries) == 0 {
		return
	}

	values = make([]V, len(entries))
	status = make([]bool, len(entries))

	m.Grow(growSize)
	parallelProcess(len(entries), func(start, end int) {
		for i := start; i < end; i++ {
			values[i], status[i] = m.ProcessEntry(entries[i].Key,
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(&entries[i], loaded)
				},
			)
		}
	})

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
	return m.batchProcessKeys(
		keys,
		int(float64(len(keys))*growFactor),
		processFn,
	)
}

func (m *MapOf[K, V]) batchProcessKeys(
	keys []K,
	growSize int,
	processFn func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (values []V, status []bool) {
	if len(keys) == 0 {
		return
	}

	values = make([]V, len(keys))
	status = make([]bool, len(keys))

	m.Grow(growSize)
	parallelProcess(len(keys), func(start, end int) {
		for i := start; i < end; i++ {
			values[i], status[i] = m.ProcessEntry(keys[i],
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(keys[i], loaded)
				},
			)
		}
	})

	return
}

// BatchUpsert batch updates or inserts multiple key-value pairs,
// returning previous values
//
// Parameters:
//   - entries: slice of key-value pairs to upsert
//
// Returns:
//   - previous: slice of previous values for each key
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchUpsert(
	entries []EntryOf[K, V],
) (previous []V, loaded []bool) {
	return m.batchProcessEntries(
		entries,
		len(entries),
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: entry.Value}, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: entry.Value}, *new(V), false
		},
	)
}

// BatchInsert batch inserts multiple key-value pairs,
// not modifying existing keys
//
// Parameters:
//   - entries: slice of key-value pairs to insert
//
// Returns:
//   - actual: slice of actual values for each key
//     (either existing or newly inserted)
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchInsert(
	entries []EntryOf[K, V],
) (actual []V, loaded []bool) {
	return m.batchProcessEntries(
		entries,
		len(entries),
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
		func(_ K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
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
func (m *MapOf[K, V]) BatchUpdate(
	entries []EntryOf[K, V],
) (previous []V, loaded []bool) {
	return m.batchProcessEntries(
		entries,
		0,
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
	for k, v := range source {
		m.Store(k, v)
	}
}

// FilterAndTransform filters and transforms elements in the map
//
// Parameters:
//   - filterFn: returns true to keep the element, false to remove it
//   - transformFn: transforms values of kept elements,
//     returns new value and whether store is needed,
//     if nil, only filtering is performed
func (m *MapOf[K, V]) FilterAndTransform(
	filterFn func(key K, value V) bool,
	transformFn func(key K, value V) (V, bool),
) {
	table := m.table.Load()
	if table == nil {
		return
	}

	// Process elements directly during iteration to avoid extra memory
	// allocation
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
			toUpsert,
			len(toUpsert),
			func(entry *EntryOf[K, V], _ *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				return entry, entry.Value, false
			},
		)
	}
}

// Merge integrates another MapOf into the current one
//
// Parameters:
//   - other: the MapOf to merge from
//   - conflictFn: conflict resolution function called when a key
//     exists in both maps
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

	other.RangeEntry(func(other *EntryOf[K, V]) bool {
		m.ProcessEntry(other.Key,
			func(this *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if this == nil {
					// Key doesn't exist in the current Map, add directly
					return other, other.Value, false
				}
				// Key exists in both Maps, use conflict handler
				return conflictFn(this, other), other.Value, true
			},
		)
		return true
	})
}

// Clone creates a deep copy of the map.
//
// Returns:
//   - A new MapOf instance with the same key-value pairs.
//
// Notes:
//
//   - This operation is not atomic with respect to concurrent modifications.
//
//   - The returned map will have the same configuration as the source.
func (m *MapOf[K, V]) Clone() *MapOf[K, V] {
	clone := &MapOf[K, V]{}
	m.CloneTo(clone)
	return clone
}

// CloneTo copies all key-value pairs from this map to the destination map.
// The destination map is cleared before copying.
//
// Parameters:
//   - clone: The destination map to copy into. Must not be nil.
//
// Notes:
//
//   - This operation is not atomic with respect to concurrent modifications.
//
//   - The destination map will have the same configuration as the source.
//
//   - The destination map is cleared before copying to ensure a clean state.
func (m *MapOf[K, V]) CloneTo(clone *MapOf[K, V]) {
	clone.Clear()
	table := m.table.Load()
	if table == nil {
		return
	}

	clone.seed = m.seed
	clone.keyHash = m.keyHash
	clone.valEqual = m.valEqual
	clone.minTableLen = m.minTableLen
	clone.shrinkEnabled = m.shrinkEnabled
	clone.intKey = m.intKey
	clone.table.Store(newMapOfTable(clone.minTableLen, runtime.GOMAXPROCS(0)))

	// Pre-fetch size to optimize initial capacity
	clone.Grow(m.Size())
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		clone.ProcessEntry(e.Key,
			func(*EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				return e, e.Value, false
			},
		)
		return true
	})
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
		for b := &table.buckets[i]; b != nil; b = (*bucketOf)(loadPointer(&b.next)) {
			stats.TotalBuckets++
			nentriesLocal := 0
			stats.Capacity += entriesPerMapOfBucket

			metaw := loadUint64(&b.meta)
			for markedw := metaw & metaMask; markedw != 0; markedw &= markedw - 1 {
				i := firstMarkedByteIndex(markedw)
				if loadPointer(&b.entries[i]) != nil {
					stats.Size++
					nentriesLocal++
				}
			}
			nentries += nentriesLocal
			if nentriesLocal == 0 {
				stats.EmptyBuckets++
			}
		}

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
// Notes:
//   - map statistics are intended to be used for diagnostic
//     purposes, not for production code. This means that breaking changes
//     may be introduced into this struct even between minor releases.
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
	// Capacity is the MapOf capacity, i.e., the total number of
	// entries that all buckets can physically hold. This number
	// does not consider the findEntry factor.
	Capacity int
	// Size is the exact number of entries stored in the map.
	Size int
	// Counter is the number of entries stored in the map according
	// to the internal atomic counter. In the case of concurrent map
	// modifications, this number may be different from Size.
	Counter int
	// CounterLen is the number of internal atomic counter stripes.
	// This number may grow with the map capacity to improve
	// multithreaded scalability.
	CounterLen int
	// MinEntries is the minimum number of entries per a chain of
	// buckets, i.e., a root bucket and its chained buckets.
	MinEntries int
	// MinEntries is the maximum number of entries per a chain of
	// buckets, i.e., a root bucket and its chained buckets.
	MaxEntries int
	// TotalGrowths is the number of times the hash table grew.
	TotalGrowths uint32
	// TotalGrowths is the number of times the hash table shrunk.
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

func (s *MapStats) String() string {
	return s.ToString()
}

// parallelProcess executes the given processor function in parallel batches.
// It automatically splits the work across available CPU cores and falls back
// to serial processing if not beneficial.
//
// Parameters:
//   - itemCount: Total number of items to process
//   - processor: Worker function that processes a range [start, end) of items
func parallelProcess(
	itemCount int,
	processor func(start, end int),
) {
	chunkSize, chunks := calcParallelism(
		itemCount,
		minParallelBatchItems,
		runtime.GOMAXPROCS(0),
	)
	if chunks > 1 {
		var wg sync.WaitGroup
		wg.Add(chunks)
		for i := 0; i < chunks; i++ {
			go func(start, end int) {
				defer wg.Done()
				processor(start, end)
			}(i*chunkSize, min((i+1)*chunkSize, itemCount))
		}
		wg.Wait()
		return
	}

	// Serial processing
	processor(0, itemCount)
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
//
//go:nosplit
func calcParallelism(items, threshold, cpus int) (chunkSize, chunks int) {
	// If the items are too small, use single-threaded processing.
	// Adjusts the parallel process trigger threshold using a scaling factor.
	// example: items < threshold * 2
	if items <= threshold {
		return items, 1
	}

	chunks = min(items/threshold, cpus)

	chunkSize = (items + chunks - 1) / chunks

	return chunkSize, chunks
}

// calcTableLen computes the bucket count for the table
// return value must be a power of 2
//
//go:nosplit
func calcTableLen(sizeHint int) int {
	tableLen := defaultMinMapTableLen
	const minSizeHintThreshold = int(float64(defaultMinMapTableLen*entriesPerMapOfBucket) * mapLoadFactor)
	if sizeHint >= minSizeHintThreshold {
		const invSizeHintFactor = 1.0 / (float64(entriesPerMapOfBucket) * mapLoadFactor)
		// +entriesPerMapOfBucket-1 is used to compensate for calculation
		// inaccuracies
		tableLen = nextPowOf2(
			int(float64(sizeHint+entriesPerMapOfBucket-1) * invSizeHintFactor),
		)
	}
	return tableLen
}

// calcSizeLen computes the size count for the table
// return value must be a power of 2
//
//go:nosplit
func calcSizeLen(tableLen, cpus int) int {
	return nextPowOf2(min(cpus, tableLen>>10))
}

// nextPowOf2 calculates the smallest power of 2 that is greater than or equal
// to n.
// Compatible with both 32-bit and 64-bit systems.
//
//go:nosplit
func nextPowOf2(n int) int {
	if n <= 0 {
		return 1
	}
	v := uint(n - 1)
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	if bits.UintSize == 64 {
		v |= v >> 32
	}
	return int(v + 1)
}

// spread improves hash distribution by XORing the original hash with its high
// bits.
// This function increases randomness in the lower bits of the hash value,
// which helps reduce collisions when calculating bucket indices.
// It's particularly effective for hash values where significant bits
// are concentrated in the upper positions.
//
//go:nosplit
func spread(h uintptr) uintptr {
	return h ^ (h >> 16)
}

// h1 extracts the bucket index from a hash value.
//
//go:nosplit
func h1(h uintptr, intKey bool) uintptr {
	if intKey {
		// Possible values: [1,2,3,4,...entriesPerMapOfBucket].
		return h / uintptr(entriesPerMapOfBucket)
	} else {
		if enableHashSpread {
			return spread(h) >> 7
		}
		return h >> 7
	}
}

// h2 extracts the byte-level hash for in-bucket lookups.
//
//go:nosplit
func h2(h uintptr) uint8 {
	if enableHashSpread {
		return uint8(spread(h)) | metaSlotMask
	}
	return uint8(h) | metaSlotMask
}

// broadcast replicates a byte value across all bytes of an uint64.
//
//go:nosplit
func broadcast(b uint8) uint64 {
	return 0x101010101010101 * uint64(b)
}

// firstMarkedByteIndex finds the index of the first marked byte in an uint64.
// It uses the trailing zeros count to determine the position of the first set
// bit, then converts that bit position to a byte index (dividing by 8).
//
// Parameters:
//   - w: A uint64 value with bits set to mark specific bytes
//
// Returns:
//   - The index (0-7) of the first marked byte in the uint64
//
//go:nosplit
func firstMarkedByteIndex(w uint64) int {
	return bits.TrailingZeros64(w) >> 3
}

// markZeroBytes implements SWAR (SIMD Within A Register) byte search.
// It may produce false positives (e.g., for 0x0100), so results should be
// verified. Returns a uint64 with the most significant bit of each byte set if
// that byte is zero.
//
// Notes:
//
//   - This SWAR algorithm identifies byte positions containing zero values.
//
//   - The operation (w - 0x0101010101010101) triggers underflow for zero-value
//     bytes, causing their most significant bit (MSB) to flip to 1.
//
//   - The subsequent & (^w) operation isolates the MSB markers specifically for
//     bytes, that were originally zero.
//
//   - Finally, & emptyMetaMask filters to only consider relevant data slots,
//     using the mask-defined marker bits (MSB of each byte).
//
//go:nosplit
func markZeroBytes(w uint64) uint64 {
	return (w - 0x0101010101010101) & (^w) & metaMask
}

// setByte sets the byte at index idx in the uint64 w to the value b.
// Returns the modified uint64 value.
//
//go:nosplit
func setByte(w uint64, b uint8, idx int) uint64 {
	shift := idx << 3
	return (w &^ (0xff << shift)) | (uint64(b) << shift)
}

////go:nosplit
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
////go:nosplit
//func getByte(w uint64, idx int) uint8 {
//	shift := idx << 3
//	return uint8((w >> shift) & 0xff)
//}
//
////go:nosplit
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

//go:nosplit
func loadPointer(addr *unsafe.Pointer) unsafe.Pointer {
	if //goland:noinspection ALL
	atomicLevel == 0 {
		return atomic.LoadPointer(addr)
	} else {
		return *addr
	}
}

//go:nosplit
func storePointer(addr *unsafe.Pointer, val unsafe.Pointer) {
	if //goland:noinspection ALL
	atomicLevel < 2 {
		atomic.StorePointer(addr, val)
	} else {
		*addr = val
	}
}

//go:nosplit
func loadUint64(addr *uint64) uint64 {
	if //goland:noinspection ALL
	atomicLevel == 0 {
		return atomic.LoadUint64(addr)
	} else {
		return *addr
	}
}

//go:nosplit
func storeUint64(addr *uint64, val uint64) {
	if //goland:noinspection ALL
	atomicLevel < 2 {
		atomic.StoreUint64(addr, val)
	} else {
		*addr = val
	}
}

//go:nosplit
func setOp(meta uint64, mask uint64, value bool) uint64 {
	if value {
		return meta | mask
	} else {
		return meta & ^mask
	}
}

//go:nosplit
func getOp(meta uint64, mask uint64) bool {
	return meta&mask != 0
}

//go:nosplit
func clearOp(meta uint64) uint64 {
	return meta & (^opByteMask)
}

////go:nosplit
//func loadOp(addr *uint64, mask uint64) bool {
//	cur := atomic.LoadUint64(addr)
//	return getOp(cur, mask)
//}
//
////go:nosplit
//func storeOp(addr *uint64, mask uint64, value bool) {
//	if value {
//		atomic.OrUint64(addr, mask)
//	} else {
//		atomic.AndUint64(addr, ^mask)
//	}
//}

//go:nosplit
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

// noescape hides a pointer from escape analysis. noescape is
// the identity function, but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	//nolint:all
	//goland:noinspection ALL
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

//go:nosplit
func delay(spins *int) {
	if //goland:noinspection ALL
	enableSpin && runtime_canSpin(*spins) {
		*spins++
		runtime_doSpin()
	} else {
		*spins = 0
		// time.Sleep with non-zero duration (≈Millisecond level) works
		// effectively as backoff under high concurrency.
		// The 500µs duration is derived from Facebook/folly's implementation:
		// https://github.com/facebook/folly/blob/main/folly/synchronization/detail/Sleeper.h
		const yieldSleep = 500 * time.Microsecond
		time.Sleep(yieldSleep)
	}
}

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
////go:linkname runtime_nanotime runtime.nanotime
////go:nosplit
//func runtime_nanotime() int64

type (
	hashFunc  func(unsafe.Pointer, uintptr) uintptr
	equalFunc func(unsafe.Pointer, unsafe.Pointer) bool
)

func defaultHasher[K comparable, V any]() (
	keyHash hashFunc,
	valEqual equalFunc,
	intKey bool,
) {
	keyHash, valEqual = defaultHasherUsingBuiltIn[K, V]()

	switch any(*new(K)).(type) {
	case uint, int, uintptr:
		return func(value unsafe.Pointer, _ uintptr) uintptr {
			return *(*uintptr)(value)
		}, valEqual, true

	case uint64, int64:
		if bits.UintSize == 32 {
			return func(value unsafe.Pointer, _ uintptr) uintptr {
				v := *(*uint64)(value)
				return uintptr(v) ^ uintptr(v>>32)
			}, valEqual, true
		}

		return func(value unsafe.Pointer, _ uintptr) uintptr {
			return uintptr(*(*uint64)(value))
		}, valEqual, true

	case uint32, int32:
		return func(value unsafe.Pointer, _ uintptr) uintptr {
			return uintptr(*(*uint32)(value))
		}, valEqual, true

	case uint16, int16:
		return func(value unsafe.Pointer, _ uintptr) uintptr {
			return uintptr(*(*uint16)(value))
		}, valEqual, true

	case uint8, int8:
		return func(value unsafe.Pointer, _ uintptr) uintptr {
			return uintptr(*(*uint8)(value))
		}, valEqual, true

	default:
		return keyHash, valEqual, false
	}
}

// defaultHasherUsingBuiltIn gets Go's built-in hash and equality functions
// for the specified types using reflection.
//
// This approach provides direct access to the type-specific functions without
// the overhead of switch statements, resulting in better performance.
//
// Notes:
//   - This implementation relies on Go's internal type representation
//   - It should be verified for compatibility with each Go version upgrade
func defaultHasherUsingBuiltIn[K comparable, V any]() (
	keyHash hashFunc,
	valEqual equalFunc,
) {
	var m map[K]V
	mapType := iTypeOf(m).MapType()
	return mapType.Hasher, mapType.Elem.Equal
}

type (
	iTFlag   uint8
	iKind    uint8
	iNameOff int32
)

// TypeOff is the offset to a type from moduledata.types.  See resolveTypeOff in
// runtime.
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
// 1. If a variable has atomic writes outside locks:
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
// 2. If a variable only has atomic reads outside locks:
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
// 3. If a variable has no external access:
//    - No atomic operations needed inside locks
//    - Normal reads/writes sufficient (lock provides full protection)
//    - Example:
//      func lockedOp() {
//          mu.Lock()
//          defer mu.Unlock()
//          value = 42 // normal write
//          v := value // normal read
//      }
