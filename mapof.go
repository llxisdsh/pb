package pb

import (
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// opByteIdx reserves the highest byte of meta for extended status flags
	opByteIdx  = 7
	opByteMask = 0xff00000000000000
	opLockMask = uint64(1) << (opByteIdx*8 + 7)
	ptrSize    = unsafe.Sizeof(unsafe.Pointer(nil))

	// entriesPerBucket defines the number of entries per bucket.
	// Calculated to fit within a cache line, with a maximum of 8 entries
	// (upper limit supported by the meta-field).
	entriesPerBucket = min(
		opByteIdx,
		(int(CacheLineSize)-int(unsafe.Sizeof(struct {
			meta uint64
			// entries [entriesPerBucket]unsafe.Pointer
			next unsafe.Pointer
		}{})))/int(ptrSize),
	)

	// Metadata constants for bucket entry management
	emptyMeta uint64 = 0
	emptySlot uint8  = 0
	metaMask  uint64 = 0x8080808080808080 >>
		(64 - min(entriesPerBucket*8, 64))
	slotMask uint8 = 0x80
)

// Performance and resizing configuration
const (
	// shrinkFraction: shrink table when occupancy < 1/shrinkFraction
	shrinkFraction = 8
	// loadFactor: resize table when occupancy > loadFactor
	loadFactor = 0.75
	// minTableLen: minimum number of buckets
	minTableLen = 32
	// minBucketsPerCPU: threshold for parallel resizing
	minBucketsPerCPU = 4
	// asyncThreshold: threshold for asynchronous resize
	asyncThreshold = 128 * 1024
	// resizeOverPartition: over-partition factor to reduce resize tail latency
	resizeOverPartition = 8
	// minBatchItems: threshold for parallel batch processing
	minBatchItems = 256
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
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		_        noCopy
		table    unsafe.Pointer
		resize   unsafe.Pointer
		growths  uint32
		shrinks  uint32
		seed     uintptr
		keyHash  HashFunc
		valEqual EqualFunc
		minLen   int
		shrinkOn bool
		intKey   bool
	}{})%CacheLineSize) % CacheLineSize]byte

	_        noCopy
	table    unsafe.Pointer // *mapOfTable
	resize   unsafe.Pointer // *resizeState
	growths  uint32
	shrinks  uint32
	seed     uintptr
	keyHash  HashFunc  // WithKeyHasher
	valEqual EqualFunc // WithValueEqual
	minLen   int       // WithPresize
	shrinkOn bool      // WithShrinkEnabled
	intKey   bool
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
	KeyHash HashFunc

	// HashOpts specifies the hash distribution optimization strategies to use.
	// These options control how hash values are converted to bucket indices
	// in the h1 function. Different strategies work better for different
	// key patterns and distributions.
	// If empty, AutoDistribution will be used (recommended for most cases).
	HashOpts []HashOptimization

	// ValEqual specifies a custom equality function for values.
	// If nil, the built-in equality comparison will be used.
	// This is primarily used for compare-and-swap operations.
	// Note: Using Compare* methods with non-comparable value types
	// will panic if ValEqual is nil.
	ValEqual EqualFunc

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
	// When true, the map will shrink when occupancy < 1/shrinkFraction.
	ShrinkEnabled bool

	// Treat zero value as logically deleted.
	// Read paths filter it, write paths treat zero as delete.
	// (Only applicable to FlatMapOf)
	zeroAsDeleted bool
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
// falls below the threshold (default: 1/shrinkFraction).
// Disabled by default to prioritize performance.
func WithShrinkEnabled() func(*MapConfig) {
	return func(c *MapConfig) {
		c.ShrinkEnabled = true
	}
}

// HashOptimization defines hash distribution optimization strategies.
// These strategies control how hash values are converted to bucket indices
// in the h1 function, affecting performance for different key patterns.
type HashOptimization int

const (
	// AutoDistribution automatically selects the most suitable distribution
	// strategy (default).
	// Based on key type analysis: integer types use linear distribution,
	// other types use shift distribution. This provides optimal performance
	// for most use cases without manual tuning.
	AutoDistribution HashOptimization = iota

	// LinearDistribution uses division-based bucket index calculation.
	// Formula: h / entriesPerBucket
	// Optimal for: sequential integer keys (1,2,3,4...), ordered data,
	// auto-incrementing IDs, or any pattern with continuous key values.
	// Provides better cache locality and reduces hash collisions for such
	// patterns.
	LinearDistribution

	// ShiftDistribution uses bit-shifting for bucket index calculation.
	// Formula: h >> 7
	// Optimal for: randomly distributed keys, string keys, complex types,
	// or any pattern with pseudo-random hash distribution.
	// Faster computation but may have suboptimal distribution for sequential
	// keys.
	ShiftDistribution
)

// WithKeyHasher sets a custom key hashing function for the map.
// This allows you to optimize hash distribution for specific key types
// or implement custom hashing strategies.
//
// Parameters:
//   - keyHash: custom hash function that takes a key and seed,
//     returns hash value. Pass nil to use the default built-in hasher
//   - opts: optional hash distribution optimization strategies.
//     Controls how hash values are converted to bucket indices.
//     If not specified, AutoDistribution will be used.
//
// Usage:
//
//	// Basic custom hasher
//	m := NewMapOf[string, int](WithKeyHasher(myCustomHashFunc))
//
//	// Custom hasher with linear distribution for sequential keys
//	m := NewMapOf[int, string](WithKeyHasher(myIntHasher, LinearDistribution))
//
//	// Custom hasher with shift distribution for random keys
//	m := NewMapOf[string, int](WithKeyHasher(myStringHasher, ShiftDistribution))
//
// Use cases:
//   - Optimize hash distribution for specific data patterns
//   - Implement case-insensitive string hashing
//   - Custom hashing for complex key types
//   - Performance tuning for known key distributions
//   - Combine with distribution strategies for optimal performance
func WithKeyHasher[K comparable](
	keyHash func(key K, seed uintptr) uintptr,
	opts ...HashOptimization,
) func(*MapConfig) {
	return func(c *MapConfig) {
		if keyHash != nil {
			c.KeyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
				return keyHash(*(*K)(pointer), u)
			}
			c.HashOpts = opts
		}
	}
}

// WithKeyHasherUnsafe sets a low-level unsafe key hashing function.
// This is the high-performance version that operates directly on memory
// pointers. Use this when you need maximum performance and are comfortable with
// unsafe operations.
//
// Parameters:
//   - hs: unsafe hash function that operates on raw unsafe.Pointer.
//     The pointer points to the key data in memory.
//     Pass nil to use the default built-in hasher
//   - opts: optional hash distribution optimization strategies.
//     Controls how hash values are converted to bucket indices.
//     If not specified, AutoDistribution will be used.
//
// Usage:
//
//	// Basic unsafe hasher
//	unsafeHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
//		// Cast ptr to your key type and implement hashing
//		key := *(*string)(ptr)
//		return uintptr(len(key)) // example hash
//	}
//	m := NewMapOf[string, int](WithKeyHasherUnsafe(unsafeHasher))
//
//	// Unsafe hasher with specific distribution strategy
//	m := NewMapOf[int, string](WithKeyHasherUnsafe(fastIntHasher,
//
// LinearDistribution))
//
// ⚠️  SAFETY WARNING:
//   - You must correctly cast unsafe.Pointer to the actual key type
//   - Incorrect pointer operations will cause crashes or memory corruption
//   - Only use if you understand Go's unsafe package
//   - Distribution strategies still apply to the hash output
func WithKeyHasherUnsafe(
	hs HashFunc,
	opts ...HashOptimization,
) func(*MapConfig) {
	return func(c *MapConfig) {
		c.KeyHash = hs
		c.HashOpts = opts
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
//	EqualFunc := func(a, b MyStruct) bool {
//		return a.ID == b.ID && a.Name == b.Name
//	}
//	m := NewMapOf[string, MyStruct](WithValueEqual(EqualFunc))
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
//	unsafeEqual := func(ptr, other unsafe.Pointer) bool {
//		// Cast pointers to your value type and implement comparison
//		val1 := *(*MyStruct)(ptr)
//		val2 := *(*MyStruct)(other)
//		return val1.ID == val2.ID // example comparison
//	}
//	m := NewMapOf[string, MyStruct](WithValueEqualUnsafe(unsafeEqual))
//
// ⚠️  SAFETY WARNING:
//   - You must correctly cast unsafe.Pointer to the actual value type
//   - Both pointers must point to valid memory of the same type
//   - Incorrect pointer operations will cause crashes or memory corruption
//   - Only use if you understand Go's unsafe package
func WithValueEqualUnsafe(eq EqualFunc) func(*MapConfig) {
	return func(c *MapConfig) {
		c.ValEqual = eq
	}
}

// bucketOf represents a hash table bucket with cache-line alignment.
type bucketOf struct {
	// meta: SWAR-optimized metadata for fast entry lookups
	// (must be 64-bit aligned)
	_    [0]int64
	meta uint64

	// Cache line padding to prevent false sharing
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		_       [0]int64
		meta    uint64
		entries [entriesPerBucket]unsafe.Pointer
		next    unsafe.Pointer
	}{})%CacheLineSize) % CacheLineSize]byte

	entries [entriesPerBucket]unsafe.Pointer // *EntryOf
	next    unsafe.Pointer                   // *bucketOf
}

// Lock acquires a spinlock for the bucket using embedded metadata.
// Uses atomic operations on the meta field to avoid false sharing overhead.
// Implements optimistic locking with fallback to spinning.
//
//go:nosplit
func (b *bucketOf) Lock() {
	cur := loadUint64NoMB(&b.meta)
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
	for {
		cur := loadUint64NoMB(&b.meta)
		if cur&opLockMask != 0 {
			return false
		}
		if atomic.CompareAndSwapUint64(&b.meta, cur, cur|opLockMask) {
			return true
		}
	}
}

//go:nosplit
func (b *bucketOf) Unlock() {
	atomic.StoreUint64(&b.meta, b.meta&^opLockMask)
}

//go:nosplit
func (b *bucketOf) UnlockWithMeta(meta uint64) {
	atomic.StoreUint64(&b.meta, meta&^opLockMask)
}

//go:nosplit
func (b *bucketOf) At(i int) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(
		unsafe.Pointer(&b.entries),
		uintptr(i)*ptrSize),
	)
}

// resizeState represents the current state of a resizing operation
type resizeState struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		wg        sync.WaitGroup
		table     unsafe.Pointer
		newTable  unsafe.Pointer
		process   int32
		completed int32
	}{})%CacheLineSize) % CacheLineSize]byte

	wg        sync.WaitGroup
	table     unsafe.Pointer // *mapOfTable
	newTable  unsafe.Pointer // *mapOfTable
	process   int32
	completed int32
}

// mapOfTable represents the internal hash table structure.
type mapOfTable struct {
	_ [(CacheLineSize - unsafe.Sizeof(struct {
		buckets  unsafeSlice[bucketOf]
		mask     uintptr
		size     unsafeSlice[counterStripe]
		sizeMask uintptr
		chunks   int
		chunkSz  int
	}{})%CacheLineSize) % CacheLineSize]byte

	buckets unsafeSlice[bucketOf]
	mask    int
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	// when the compile option `mapof_opt_enablepadding` is enabled
	size     unsafeSlice[counterStripe]
	sizeMask int
	// number of chunks and chunks size for resizing
	chunks  int
	chunkSz int
}

func newMapOfTable(tableLen, cpus int) *mapOfTable {
	overCpus := cpus * resizeOverPartition
	chunkSz, chunks := calcParallelism(tableLen, minBucketsPerCPU, overCpus)
	sizeLen := calcSizeLen(tableLen, cpus)
	return &mapOfTable{
		buckets:  makeUnsafeSlice(make([]bucketOf, tableLen)),
		mask:     tableLen - 1,
		size:     makeUnsafeSlice(make([]counterStripe, sizeLen)),
		sizeMask: sizeLen - 1,
		chunks:   chunks,
		chunkSz:  chunkSz,
	}
}

// AddSize atomically adds delta to the size counter for the given bucket index.
//
//go:nosplit
func (table *mapOfTable) AddSize(idx, delta int) {
	atomic.AddUintptr(&table.size.At(table.sizeMask&idx).c, uintptr(delta))
}

// SumSize calculates the total number of entries in the table
// by summing all counter-stripes.
//
//go:nosplit
func (table *mapOfTable) SumSize() int {
	var sum uintptr
	for i := 0; i <= table.sizeMask; i++ {
		sum += loadUintptrNoMB(&table.size.At(i).c)
	}
	return int(sum)
}

// IsZero checks if the table is empty by verifying all counter-stripes are
// zero.
//
//go:nosplit
func (table *mapOfTable) IsZero() bool {
	for i := 0; i <= table.sizeMask; i++ {
		if loadUintptrNoMB(&table.size.At(i).c) != 0 {
			return false
		}
	}
	return true
}

// IHashCode defines a custom hash function interface for key types.
// Key types implementing this interface can provide their own hash computation,
// serving as an alternative to WithKeyHasher for type-specific optimization.
//
// This interface is automatically detected during MapOf initialization and
// takes
// precedence over the default built-in hasher but is overridden by explicit
// WithKeyHasher configuration.
//
// Usage:
//
//	type UserID struct {
//		ID int64
//		Tenant string
//	}
//
//	func (u *UserID) HashCode(seed uintptr) uintptr {
//		return uintptr(u.ID) ^ seed
//	}
type IHashCode interface {
	HashCode(seed uintptr) uintptr
}

// IHashOpts defines hash distribution optimization interface for key types.
// Key types implementing this interface can specify their preferred hash
// distribution strategy, serving as an alternative to WithKeyHasher's opts
// parameter.
//
// Note: IHashOpts only works if Key implements IHashCode.
//
// This interface is automatically detected during MapOf initialization and
// provides fine-grained control over hash-to-bucket mapping strategies.
//
// Usage:
//
//	type SequentialID int64
//
//	func (*SequentialID) HashOpts() []HashOptimization {
//		return []HashOptimization{LinearDistribution}
//	}
type IHashOpts interface {
	HashOpts() []HashOptimization
}

// IEqual defines a custom equality comparison interface for value types.
// Value types implementing this interface can provide their own equality logic,
// serving as an alternative to WithValueEqual for type-specific comparison.
//
// This interface is automatically detected during MapOf initialization and is
// essential for non-comparable value types or custom equality semantics.
// It takes precedence over the default built-in comparison but is overridden
// by explicit WithValueEqual configuration.
//
// Usage:
//
//	type UserProfile struct {
//		Name string
//		Tags []string // slice makes this non-comparable
//	}
//
//	func (u *UserProfile) Equal(other UserProfile) bool {
//		return u.Name == other.Name && slices.Equal(u.Tags, other.Tags)
//	}
type IEqual[T any] interface {
	Equal(other T) bool
}

// InitWithOptions initializes the MapOf instance using variadic option
// parameters. This is a convenience method that allows configuring MapOf
// through the functional options pattern.
//
// Configuration Priority (highest to lowest):
//   - Explicit With* functions (WithKeyHasher, WithValueEqual)
//   - Interface implementations (IHashCode, IHashOpts, IEqual)
//   - Default built-in implementations (defaultHasher) - fallback
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

	// parse options
	for _, o := range options {
		o(c)
	}
	m.init(c)
}

func (m *MapOf[K, V]) init(
	c *MapConfig,
) *mapOfTable {
	// parse interface
	if c.KeyHash == nil {
		var zeroK K
		ak := any(&zeroK)
		if _, ok := ak.(IHashCode); ok {
			c.KeyHash = func(ptr unsafe.Pointer, seed uintptr) uintptr {
				return any((*K)(ptr)).(IHashCode).HashCode(seed)
			}
			if i, ok := ak.(IHashOpts); ok {
				c.HashOpts = i.HashOpts()
			}
		}
	}
	if c.ValEqual == nil {
		var zeroV V
		vk := any(&zeroV)
		if _, ok := vk.(IEqual[V]); ok {
			c.ValEqual = func(ptr unsafe.Pointer, other unsafe.Pointer) bool {
				return any((*V)(ptr)).(IEqual[V]).Equal(*(*V)(other))
			}
		}
	}

	// perform initialization
	m.seed = uintptr(rand.Uint64())
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	if c.KeyHash != nil {
		m.keyHash = c.KeyHash
		for _, o := range c.HashOpts {
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
	if c.ValEqual != nil {
		m.valEqual = c.ValEqual
	}
	m.minLen = calcTableLen(c.SizeHint)
	m.shrinkOn = c.ShrinkEnabled

	table := newMapOfTable(m.minLen, runtime.GOMAXPROCS(0))
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	return table
}

// initSlow may be called concurrently by multiple goroutines, so it requires
// synchronization with a "lock" mechanism.
//
//go:noinline
func (m *MapOf[K, V]) initSlow() *mapOfTable {
	rs := (*resizeState)(loadPointerNoMB(&m.resize))
	if rs != nil {
		rs.wg.Wait()
		// Now the table should be initialized
		return (*mapOfTable)(loadPointerNoMB(&m.table))
	}

	// Create a temporary WaitGroup for initialization synchronization
	rs = new(resizeState)
	rs.wg.Add(1)

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !atomic.CompareAndSwapPointer(&m.resize, nil, unsafe.Pointer(rs)) {
		// Another goroutine is initializing, wait for it to complete
		rs = (*resizeState)(loadPointerNoMB(&m.resize))
		if rs != nil {
			rs.wg.Wait()
		}
		// Now the table should be initialized
		return (*mapOfTable)(loadPointerNoMB(&m.table))
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table != nil {
		atomic.StorePointer(&m.resize, nil)
		rs.wg.Done()
		return table
	}

	// Perform initialization
	c := &MapConfig{}
	table = m.init(c)
	atomic.StorePointer(&m.resize, nil)
	rs.wg.Done()
	return table
}

// Load retrieves a value for the given key, compatible with `sync.Map`.
//
//go:nosplit
func (m *MapOf[K, V]) Load(key K) (value V, ok bool) {
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return
	}

	// inline findEntry
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1(hash, m.intKey)
	for b := table.buckets.At(idx); b != nil; b = (*bucketOf)(loadPointerNoMB(&b.next)) {
		meta := loadUint64NoMB(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if e := (*EntryOf[K, V])(loadPointerNoMB(b.At(j))); e != nil {
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	idx := table.mask & h1(hash, m.intKey)
	for b := table.buckets.At(idx); b != nil; b = (*bucketOf)(loadPointerNoMB(&b.next)) {
		meta := loadUint64NoMB(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if e := (*EntryOf[K, V])(loadPointerNoMB(b.At(j))); e != nil {
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
		idx := table.mask & h1v
		root := table.buckets.At(idx)

		root.Lock()

		// This is the first check, checking if there is a resize operation in
		// progress before acquiring the bucket lock
		if rs := (*resizeState)(loadPointerNoMB(&m.resize)); rs != nil &&
			loadPointerNoMB(&rs.table) != nil /*skip init*/ &&
			loadPointerNoMB(&rs.newTable) != nil /*skip newTable is nil*/ {
			root.Unlock()
			// Wait for the current resize operation to complete
			m.helpCopyAndWait(rs)
			table = (*mapOfTable)(loadPointerNoMB(&m.table))
			continue
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := (*mapOfTable)(loadPointerNoMB(&m.table)); table != newTable {
			root.Unlock()
			table = newTable
			continue
		}

		var (
			oldEntry *EntryOf[K, V]
			oldB     *bucketOf
			oldIdx   int
			oldMeta  uint64
			emptyB   *bucketOf
			emptyIdx int
			lastB    *bucketOf
		)

	findLoop:
		for b := root; b != nil; b = (*bucketOf)(b.next) {
			meta := b.meta
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*EntryOf[K, V])(*b.At(j)); e != nil {
					if embeddedHash {
						if e.getHash() == hash && e.Key == *key {
							oldEntry, oldB, oldIdx, oldMeta = e, b, j, meta
							break findLoop
						}
					} else {
						if e.Key == *key {
							oldEntry, oldB, oldIdx, oldMeta = e, b, j, meta
							break findLoop
						}
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

		// --- Processing Logic ---
		newEntry, value, status := fn(oldEntry)

		if oldEntry != nil {
			if newEntry == oldEntry {
				// No entry to update or delete
				root.Unlock()
				return value, status
			}
			if newEntry != nil {
				// Update
				if embeddedHash {
					newEntry.setHash(hash)
				}
				newEntry.Key = *key
				storePointerNoMB(
					oldB.At(oldIdx),
					unsafe.Pointer(newEntry),
				)
				root.Unlock()
				return value, status
			}
			// Delete
			storePointerNoMB(oldB.At(oldIdx), nil)
			newMeta := setByte(oldMeta, emptySlot, oldIdx)
			if oldB == root {
				root.UnlockWithMeta(newMeta)
			} else {
				storeUint64NoMB(&oldB.meta, newMeta)
				root.Unlock()
			}
			table.AddSize(idx, -1)

			// Check if table shrinking is needed
			if m.shrinkOn && newMeta&(^uint64(opByteMask)) == emptyMeta &&
				loadPointerNoMB(&m.resize) == nil {
				tableLen := table.mask + 1
				if m.minLen < tableLen {
					size := table.SumSize()
					if size < tableLen*entriesPerBucket/shrinkFraction {
						m.tryResize(mapShrinkHint, size, 0)
					}
				}
			}
			return value, status
		}

		if newEntry == nil {
			// No entry to insert or delete
			root.Unlock()
			return value, status
		}

		// Insert
		if embeddedHash {
			newEntry.setHash(hash)
		}
		newEntry.Key = *key
		if emptyB != nil {
			// publish pointer first, then meta; readers check meta before
			// pointer so they won't observe a partially-initialized entry,
			// and this reduces the window where meta is visible but pointer is
			// still nil
			storePointerNoMB(emptyB.At(emptyIdx), unsafe.Pointer(newEntry))
			if emptyB == root {
				root.UnlockWithMeta(setByte(emptyB.meta, h2v, emptyIdx))
			} else {
				storeUint64NoMB(
					&emptyB.meta,
					setByte(emptyB.meta, h2v, emptyIdx),
				)
				root.Unlock()
			}
			table.AddSize(idx, 1)
			return value, status
		}

		// No empty slot, create new bucket and insert
		storePointerNoMB(&lastB.next, unsafe.Pointer(&bucketOf{
			meta: setByte(emptyMeta, h2v, 0),
			entries: [entriesPerBucket]unsafe.Pointer{
				unsafe.Pointer(newEntry),
			},
		}))
		root.Unlock()
		table.AddSize(idx, 1)

		// Check if the table needs to grow
		if loadPointerNoMB(&m.resize) == nil {
			tableLen := table.mask + 1
			size := table.SumSize()
			const sizeHintFactor = float64(entriesPerBucket) * loadFactor
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

func (m *MapOf[K, V]) doResize(
	hint mapResizeHint,
	sizeAdd int,
) {
	var size int
	for {
		// Resize check
		table := (*mapOfTable)(loadPointerNoMB(&m.table))
		tableLen := table.mask + 1
		switch hint {
		case mapGrowHint:
			if sizeAdd <= 0 {
				return
			}
			size = table.SumSize()
			newTableLen := calcTableLen(size + sizeAdd)
			if tableLen >= newTableLen {
				return
			}
		case mapShrinkHint:
			if tableLen <= m.minLen {
				return
			}
			// Recalculate the shrink size to avoid over-shrinking
			size = table.SumSize()
			newTableLen := calcTableLen(size)
			if tableLen <= newTableLen {
				return
			}
		default:
			if tableLen == m.minLen && table.IsZero() {
				return
			}
		}
		// Wait resize finish
		if rs := (*resizeState)(loadPointerNoMB(&m.resize)); rs != nil {
			if loadPointerNoMB(&rs.table) != nil /*skip init*/ &&
				loadPointerNoMB(&rs.newTable) != nil /*skip newTable is nil */ {
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
	if !atomic.CompareAndSwapPointer(&m.resize, nil, unsafe.Pointer(rs)) {
		return
	}

	// Resize start
	cpus := runtime.GOMAXPROCS(0)
	if hint == mapClearHint {
		newTable := newMapOfTable(m.minLen, cpus)
		atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
		atomic.StorePointer(&m.resize, nil)
		rs.wg.Done()
		return
	}

	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
		atomic.AddUint32(&m.growths, 1)
	} else {
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
		atomic.AddUint32(&m.shrinks, 1)
	}

	if newLen*int(unsafe.Sizeof(bucketOf{})) >= asyncThreshold && cpus > 1 {
		// The big table, use goroutines to create new table and copy entries
		go m.finalizeResize(table, newLen, rs, cpus)
	} else {
		m.finalizeResize(table, newLen, rs, cpus)
	}
}

func (m *MapOf[K, V]) finalizeResize(
	table *mapOfTable,
	newLen int,
	rs *resizeState,
	cpus int,
) {
	atomic.StorePointer(&rs.table, unsafe.Pointer(table))
	newTable := newMapOfTable(newLen, cpus)
	atomic.StorePointer(&rs.newTable, unsafe.Pointer(newTable))
	m.helpCopyAndWait(rs)
}

//go:noinline
func (m *MapOf[K, V]) helpCopyAndWait(rs *resizeState) {
	table := (*mapOfTable)(loadPointerNoMB(&rs.table))
	tableLen := table.mask + 1
	chunks := int32(table.chunks)
	chunkSz := table.chunkSz
	newTable := (*mapOfTable)(loadPointerNoMB(&rs.newTable))
	isGrowth := (newTable.mask + 1) > tableLen
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
		if isGrowth {
			m.copyBucketOf(table, start, end, newTable)
		} else {
			m.copyBucketOfLock(table, start, end, newTable)
		}
		if atomic.AddInt32(&rs.completed, 1) == chunks {
			// Copying completed
			atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
			atomic.StorePointer(&m.resize, nil)
			rs.wg.Done()
			return
		}
	}
}

// copyBucketOfLock, unlike copyBucketOf, it locks the destination bucket to
// ensure concurrency safety.
func (m *MapOf[K, V]) copyBucketOfLock(
	table *mapOfTable,
	start, end int,
	mewTable *mapOfTable,
) {
	seed := m.seed
	keyHash := m.keyHash
	intKey := m.intKey
	copied := 0
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*bucketOf)(b.next) {
			meta := b.meta
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*EntryOf[K, V])(*b.At(j)); e != nil {
					var hash uintptr
					if embeddedHash {
						hash = e.getHash()
					} else {
						hash = keyHash(noescape(unsafe.Pointer(&e.Key)), seed)
					}
					idx := mewTable.mask & h1(hash, intKey)
					destBucket := mewTable.buckets.At(idx)
					h2v := h2(hash)

					destBucket.Lock()
					b := destBucket
				appendToBucket:
					for {
						meta := b.meta
						empty := (^meta) & metaMask
						if empty != 0 {
							emptyIdx := firstMarkedByteIndex(empty)
							b.meta = setByte(meta, h2v, emptyIdx)
							*b.At(emptyIdx) = unsafe.Pointer(e)
							break appendToBucket
						}
						next := (*bucketOf)(b.next)
						if next == nil {
							b.next = unsafe.Pointer(&bucketOf{
								meta:    setByte(emptyMeta, h2v, 0),
								entries: [entriesPerBucket]unsafe.Pointer{unsafe.Pointer(e)},
							})
							break appendToBucket
						}
						b = next
					}
					destBucket.Unlock()

					copied++
				}
			}
		}
		srcBucket.Unlock()
	}
	if copied != 0 {
		mewTable.AddSize(start, copied)
	}
}

func (m *MapOf[K, V]) copyBucketOf(
	table *mapOfTable,
	start, end int,
	newTable *mapOfTable,
) {
	seed := m.seed
	keyHash := m.keyHash
	intKey := m.intKey
	copied := 0
	for i := start; i < end; i++ {
		srcBucket := table.buckets.At(i)
		srcBucket.Lock()
		for b := srcBucket; b != nil; b = (*bucketOf)(b.next) {
			meta := b.meta
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*EntryOf[K, V])(*b.At(j)); e != nil {
					var hash uintptr
					if embeddedHash {
						hash = e.getHash()
					} else {
						hash = keyHash(noescape(unsafe.Pointer(&e.Key)), seed)
					}
					idx := newTable.mask & h1(hash, intKey)
					destBucket := newTable.buckets.At(idx)
					h2v := h2(hash)

					b := destBucket
				appendToBucket:
					for {
						meta := b.meta
						empty := (^meta) & metaMask
						if empty != 0 {
							emptyIdx := firstMarkedByteIndex(empty)
							b.meta = setByte(meta, h2v, emptyIdx)
							*b.At(emptyIdx) = unsafe.Pointer(e)
							break appendToBucket
						}
						next := (*bucketOf)(b.next)
						if next == nil {
							b.next = unsafe.Pointer(&bucketOf{
								meta:    setByte(emptyMeta, h2v, 0),
								entries: [entriesPerBucket]unsafe.Pointer{unsafe.Pointer(e)},
							})
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
		// copyBucketOf is used during multithreaded growth, requiring a
		// thread-safe AddSize.
		newTable.AddSize(start, copied)
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return
	}

	for i := 0; i <= table.mask; i++ {
		root := table.buckets.At(i)
		root.Lock()
		for b := root; b != nil; b = (*bucketOf)(b.next) {
			meta := b.meta
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*EntryOf[K, V])(*b.At(j)); e != nil {

					newEntry := fn(e)

					if newEntry == e {
						// No entry to update or delete
					} else if newEntry != nil {
						// Update
						if embeddedHash {
							newEntry.setHash(e.getHash())
						}
						newEntry.Key = e.Key
						storePointerNoMB(b.At(j), unsafe.Pointer(newEntry))
					} else {
						// Delete
						storePointerNoMB(b.At(j), nil)
						newMeta := setByte(meta, emptySlot, j)
						storeUint64NoMB(&b.meta, newMeta)
						table.AddSize(i, -1)
					}
				}
			}
		}
		root.Unlock()
	}
}

// Store inserts or updates a key-value pair, compatible with `sync.Map`.
func (m *MapOf[K, V]) Store(key K, value V) {
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		table = m.initSlow()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	return m.processEntry(table, hash, &key, fn)
}

// Clear compatible with `sync.Map`
func (m *MapOf[K, V]) Clear() {
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return
	}
	m.doResize(mapClearHint, 0)
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
	if loadPointerNoMB(&m.table) == nil {
		m.initSlow()
	}
	m.doResize(mapGrowHint, sizeAdd)
}

// Shrink reduces the capacity to fit the current size,
// always executes regardless of WithShrinkEnabled.
func (m *MapOf[K, V]) Shrink() {
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return
	}
	m.doResize(mapShrinkHint, -1)
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return
	}
	for i := 0; i <= table.mask; i++ {
		for b := table.buckets.At(i); b != nil; b = (*bucketOf)(loadPointerNoMB(&b.next)) {
			meta := loadUint64NoMB(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*EntryOf[K, V])(loadPointerNoMB(b.At(j))); e != nil {
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return 0
	}
	return table.SumSize()
}

// IsZero checks zero values, faster than Size().
//
//go:nosplit
func (m *MapOf[K, V]) IsZero() bool {
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return true
	}
	return table.IsZero()
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
//
//lint:ignore U1000
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
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
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return
	}

	clone.seed = m.seed
	clone.keyHash = m.keyHash
	clone.valEqual = m.valEqual
	clone.minLen = m.minLen
	clone.shrinkOn = m.shrinkOn
	clone.intKey = m.intKey
	atomic.StorePointer(&clone.table,
		unsafe.Pointer(newMapOfTable(clone.minLen, runtime.GOMAXPROCS(0))),
	)

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
		TotalGrowths: loadUint32NoMB(&m.growths),
		TotalShrinks: loadUint32NoMB(&m.shrinks),
		MinEntries:   math.MaxInt,
	}
	table := (*mapOfTable)(loadPointerNoMB(&m.table))
	if table == nil {
		return stats
	}
	stats.RootBuckets = table.mask + 1
	stats.Counter = table.SumSize()
	stats.CounterLen = table.sizeMask + 1
	for i := 0; i <= table.mask; i++ {
		entries := 0
		for b := table.buckets.At(i); b != nil; b = (*bucketOf)(loadPointerNoMB(&b.next)) {
			stats.TotalBuckets++
			entriesLocal := 0
			stats.Capacity += entriesPerBucket

			meta := loadUint64NoMB(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*EntryOf[K, V])(loadPointerNoMB(b.At(j))); e != nil {
					stats.Size++
					entriesLocal++
				}
			}
			entries += entriesLocal
			if entriesLocal == 0 {
				stats.EmptyBuckets++
			}
		}

		if entries < stats.MinEntries {
			stats.MinEntries = entries
		}
		if entries > stats.MaxEntries {
			stats.MaxEntries = entries
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
	chunkSz, chunks := calcParallelism(
		itemCount,
		minBatchItems,
		runtime.GOMAXPROCS(0),
	)
	if chunks > 1 {
		var wg sync.WaitGroup
		wg.Add(chunks)
		for i := 0; i < chunks; i++ {
			go func(start, end int) {
				defer wg.Done()
				processor(start, end)
			}(i*chunkSz, min((i+1)*chunkSz, itemCount))
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
//   - chunkSz: Number of items processed per goroutine
//
//go:nosplit
func calcParallelism(items, threshold, cpus int) (chunkSz, chunks int) {
	// If the items are too small, use single-threaded processing.
	// Adjusts the parallel process trigger threshold using a scaling factor.
	// example: items < threshold * 2
	if items <= threshold {
		return items, 1
	}

	chunks = min(items/threshold, cpus)

	chunkSz = (items + chunks - 1) / chunks

	return chunkSz, chunks
}

// calcTableLen computes the bucket count for the table
// return value must be a power of 2
//
//go:nosplit
func calcTableLen(sizeHint int) int {
	tableLen := minTableLen
	const minSizeHintThreshold = int(float64(minTableLen*entriesPerBucket) * loadFactor)
	if sizeHint >= minSizeHintThreshold {
		const invSizeHintFactor = 1.0 / (float64(entriesPerBucket) * loadFactor)
		// +entriesPerBucket-1 is used to compensate for calculation
		// inaccuracies
		tableLen = nextPowOf2(
			int(float64(sizeHint+entriesPerBucket-1) * invSizeHintFactor),
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
func h1(h uintptr, intKey bool) int {
	if intKey {
		// Possible values: [1,2,3,4,...entriesPerBucket].
		return int(h) / entriesPerBucket
	}
	if enableHashSpread {
		return int(spread(h)) >> 7
	}
	return int(h) >> 7
}

// h2 extracts the byte-level hash for in-bucket lookups.
//
//go:nosplit
func h2(h uintptr) uint8 {
	if enableHashSpread {
		return uint8(spread(h)) | slotMask
	}
	return uint8(h) | slotMask
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
// verified. Returns an uint64 with the most significant bit of each byte set if
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

//goland:noinspection ALL
const useAutoDetectedTSO = atomicLevel == -1 &&
	(runtime.GOARCH == "amd64" || runtime.GOARCH == "386" ||
		runtime.GOARCH == "s390x" || runtime.GOARCH == "s390")

//go:nosplit
func loadPointerNoMB(addr *unsafe.Pointer) unsafe.Pointer {
	//goland:noinspection ALL
	if useAutoDetectedTSO || atomicLevel >= 1 {
		return *addr
	} else {
		return atomic.LoadPointer(addr)
	}
}

//go:nosplit
func storePointerNoMB(addr *unsafe.Pointer, val unsafe.Pointer) {
	//goland:noinspection ALL
	if useAutoDetectedTSO || atomicLevel >= 2 {
		*addr = val
	} else {
		atomic.StorePointer(addr, val)
	}
}

//go:nosplit
func loadUintptrNoMB(addr *uintptr) uintptr {
	//goland:noinspection ALL
	if useAutoDetectedTSO || atomicLevel >= 1 {
		return *addr
	} else {
		return atomic.LoadUintptr(addr)
	}
}

//lint:ignore U1000
//go:nosplit
func storeUintptrNoMB(addr *uintptr, val uintptr) {
	//goland:noinspection ALL
	if useAutoDetectedTSO || atomicLevel >= 2 {
		*addr = val
	} else {
		atomic.StoreUintptr(addr, val)
	}
}

//go:nosplit
func loadUint64NoMB(addr *uint64) uint64 {
	//goland:noinspection ALL
	if (useAutoDetectedTSO || atomicLevel >= 1) && bits.UintSize >= 64 {
		return *addr
	} else {
		return atomic.LoadUint64(addr)
	}
}

//go:nosplit
func storeUint64NoMB(addr *uint64, val uint64) {
	//goland:noinspection ALL
	if (useAutoDetectedTSO || atomicLevel >= 2) && bits.UintSize >= 64 {
		*addr = val
	} else {
		atomic.StoreUint64(addr, val)
	}
}

//go:nosplit
func loadUint32NoMB(addr *uint32) uint32 {
	//goland:noinspection ALL
	if (useAutoDetectedTSO || atomicLevel >= 1) && bits.UintSize >= 64 {
		return *addr
	} else {
		return atomic.LoadUint32(addr)
	}
}

//lint:ignore U1000
//go:nosplit
func storeUint32NoMB(addr *uint32, val uint32) {
	//goland:noinspection ALL
	if (useAutoDetectedTSO || atomicLevel >= 2) && bits.UintSize >= 64 {
		*addr = val
	} else {
		atomic.StoreUint32(addr, val)
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

// noCopy may be added to structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
//
// Note that it must not be embedded, due to the Lock and Unlock methods.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// unsafeSlice provides semi-ergonomic limited slice-like functionality
// without bounds checking for fixed sized slices.
type unsafeSlice[T any] struct {
	ptr unsafe.Pointer
}

//go:nosplit
func makeUnsafeSlice[T any](s []T) unsafeSlice[T] {
	return unsafeSlice[T]{ptr: unsafe.Pointer(unsafe.SliceData(s))}
}

//go:nosplit
func (s unsafeSlice[T]) At(i int) *T {
	return (*T)(unsafe.Add(s.ptr, unsafe.Sizeof(*new(T))*uintptr(i)))
}

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
//goland:noinspection ALL
func runtime_canSpin(i int) bool

// nolint:all
//
//go:linkname runtime_doSpin sync.runtime_doSpin
//go:nosplit
//goland:noinspection ALL
func runtime_doSpin()

type (
	HashFunc  func(ptr unsafe.Pointer, seed uintptr) uintptr
	EqualFunc func(ptr unsafe.Pointer, other unsafe.Pointer) bool
)

func defaultHasher[K comparable, V any]() (
	keyHash HashFunc,
	valEqual EqualFunc,
	intKey bool,
) {
	keyHash, valEqual = defaultHasherUsingBuiltIn[K, V]()

	switch any(*new(K)).(type) {
	case uint, int, uintptr:
		return hashUintptr, valEqual, true
	case uint64, int64:
		if bits.UintSize == 32 {
			return hashUint64On32Bit, valEqual, true
		}
		return hashUint64, valEqual, true
	case uint32, int32:
		return hashUint32, valEqual, true
	case uint16, int16:
		return hashUint16, valEqual, true
	case uint8, int8:
		return hashUint8, valEqual, true
	case string:
		return hashString, valEqual, false
	case []byte:
		return hashString, valEqual, false
	default:
		// for types like integers
		var zeroK K
		kType := reflect.TypeOf(zeroK)
		switch kType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Uintptr:
			return hashUintptr, valEqual, true
		case reflect.Int64, reflect.Uint64:
			if bits.UintSize == 32 {
				return hashUint64On32Bit, valEqual, true
			}
			return hashUint64, valEqual, true
		case reflect.Int32, reflect.Uint32:
			return hashUint32, valEqual, true
		case reflect.Int16, reflect.Uint16:
			return hashUint16, valEqual, true
		case reflect.Int8, reflect.Uint8:
			return hashUint8, valEqual, true
		case reflect.String:
			return hashString, valEqual, false
		case reflect.Slice:
			// Check if it's []byte
			if kType.Elem().Kind() == reflect.Uint8 {
				return hashString, valEqual, false
			}
			return keyHash, valEqual, false
		default:
			return keyHash, valEqual, false
		}
	}
}

//go:nosplit
func hashUintptr(ptr unsafe.Pointer, _ uintptr) uintptr {
	return *(*uintptr)(ptr)
}

//go:nosplit
func hashUint64On32Bit(ptr unsafe.Pointer, _ uintptr) uintptr {
	v := *(*uint64)(ptr)
	return uintptr(v) ^ uintptr(v>>32)
}

//go:nosplit
func hashUint64(ptr unsafe.Pointer, _ uintptr) uintptr {
	return uintptr(*(*uint64)(ptr))
}

//go:nosplit
func hashUint32(ptr unsafe.Pointer, _ uintptr) uintptr {
	return uintptr(*(*uint32)(ptr))
}

//go:nosplit
func hashUint16(ptr unsafe.Pointer, _ uintptr) uintptr {
	return uintptr(*(*uint16)(ptr))
}

//go:nosplit
func hashUint8(ptr unsafe.Pointer, _ uintptr) uintptr {
	return uintptr(*(*uint8)(ptr))
}

//go:nosplit
func hashString(ptr unsafe.Pointer, seed uintptr) uintptr {
	// The algorithm has good cache affinity
	type stringHeader struct {
		data unsafe.Pointer
		len  int
	}
	s := (*stringHeader)(ptr)
	if s.len <= 12 {
		for i := 0; i < s.len; i++ {
			seed = seed*31 + uintptr(*(*uint8)(unsafe.Add(noescape(s.data), i)))
		}
		return seed
	}
	// Fallback to the built-in hash function
	return builtInStringHasher(ptr, seed)
}

var builtInStringHasher, _ = defaultHasherUsingBuiltIn[string, struct{}]()

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
	keyHash HashFunc,
	valEqual EqualFunc,
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
