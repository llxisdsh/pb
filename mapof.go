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
	"unsafe"

	"golang.org/x/sys/cpu"
)

const (
	// cacheLineSize is used in structure padding to prevent false sharing.
	// It's automatically calculated using the `golang.org/x/sys/cpu` package.
	cacheLineSize = unsafe.Sizeof(cpu.CacheLinePad{})

	// entriesPerMapOfBucket defines the number of MapOf entries per bucket.
	// This value is automatically calculated to fit within a cache line,
	// but will not exceed 8, which is the upper limit supported by the meta field.
	entriesPerMapOfBucket = min(8, (int(cacheLineSize)-int(unsafe.Sizeof(struct {
		meta uint64
		// entries [entriesPerMapOfBucket]unsafe.Pointer
		next unsafe.Pointer
		mu   sync.Mutex
	}{})))/int(unsafe.Sizeof(unsafe.Pointer(nil))))

	defaultMeta       uint64 = 0x8080808080808080
	metaMask          uint64 = 0xffffffffff
	defaultMetaMasked uint64 = defaultMeta & metaMask
	emptyMetaSlot     uint8  = 0x80
)

type mapResizeHint int

const (
	mapGrowHint   mapResizeHint = 0
	mapShrinkHint mapResizeHint = 1
	mapClearHint  mapResizeHint = 2
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
	// minParallelResizeThreshold defines the minimum table size required to trigger parallel resizing.
	// Tables smaller than this threshold will use single-threaded resizing for better efficiency.
	minParallelResizeThreshold = 1024
	// minBucketsPerGoroutine defines the minimum number of buckets each goroutine handles
	// during parallel resizing operations to ensure efficient workload distribution.
	minBucketsPerGoroutine = 32
	// minParallelBatchItems defines the minimum number of items required for parallel batch processing.
	// Below this threshold, serial processing is used to avoid the overhead of goroutine creation.
	minParallelBatchItems = 256
	// minItemsPerGoroutine defines the minimum number of items each goroutine processes
	// during batch operations, used to control the degree of parallelism.
	minItemsPerGoroutine = 32
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
//   - Includes rich functional extensions such as LoadOrCompute, ProcessEntry, Size, IsZero,
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
	pad [(cacheLineSize - unsafe.Sizeof(struct {
		table        *mapOfTable[K, V]
		resizeWg     atomic.Pointer[sync.WaitGroup]
		totalGrowths int
		totalShrinks int
		keyHash      hashFunc
		valEqual     equalFunc
		minTableLen  int
		growOnly     bool
	}{})%cacheLineSize) % cacheLineSize]byte

	table        *mapOfTable[K, V]
	resizeWg     atomic.Pointer[sync.WaitGroup]
	totalGrowths int
	totalShrinks int
	keyHash      hashFunc
	valEqual     equalFunc
	minTableLen  int  // WithPresize
	growOnly     bool // WithGrowOnly
}

// mapOfTable represents the internal hash table structure.
type mapOfTable[K comparable, V any] struct {
	//lint:ignore U1000 prevents false sharing
	pad [(cacheLineSize - unsafe.Sizeof(struct {
		buckets []bucketOf[K, V]
		size    []counterStripe
		seed    uintptr
	}{})%cacheLineSize) % cacheLineSize]byte

	buckets []bucketOf[K, V]
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory on enablepadding=true
	size []counterStripe
	seed uintptr
}

// bucketOf represents a single bucket in the hash table.
type bucketOf[K comparable, V any] struct {
	//lint:ignore U1000 prevents false sharing
	pad [(cacheLineSize - unsafe.Sizeof(struct {
		meta    uint64
		entries [entriesPerMapOfBucket]*EntryOf[K, V]
		next    *bucketOf[K, V]
		mu      sync.Mutex
	}{})%cacheLineSize) % cacheLineSize]byte

	meta    uint64                                // Metadata for fast entry lookups using SWAR techniques
	entries [entriesPerMapOfBucket]*EntryOf[K, V] // Pointers to *EntryOf instances
	next    *bucketOf[K, V]                       // Pointer to the next bucket (*bucketOf) in the chain
	mu      sync.Mutex                            // Lock for bucket modifications
}

// EntryOf is an immutable map entry.
type EntryOf[K comparable, V any] struct {
	Key   K
	Value V
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

func newMapOfTable[K comparable, V any](tableLen int) *mapOfTable[K, V] {
	buckets := make([]bucketOf[K, V], tableLen)
	for i := range buckets {
		buckets[i].meta = defaultMeta
	}

	counterLen := calcSizeLen(tableLen)

	t := &mapOfTable[K, V]{
		buckets: buckets,
		size:    make([]counterStripe, counterLen),
		seed:    uintptr(rand.Uint64()),
	}
	return t
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
func (m *MapOf[K, V]) Init(keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig)) {

	m.init(keyHash, valEqual, options...)
}

func (m *MapOf[K, V]) init(keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig)) *mapOfTable[K, V] {

	c := &MapConfig{
		sizeHint: defaultMinMapTableLen * entriesPerMapOfBucket,
	}
	for _, o := range options {
		o(c)
	}
	m.keyHash, m.valEqual = defaultHasherUsingBuiltIn[K, V]()
	if keyHash != nil {
		m.keyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
			return keyHash(*(*K)(pointer), u)
		}
	}
	if valEqual != nil {
		m.valEqual = func(val unsafe.Pointer, val2 unsafe.Pointer) bool {
			return valEqual(*(*V)(val), *(*V)(val2))
		}
	}

	m.minTableLen = calcTableLen(c.sizeHint)
	m.growOnly = c.growOnly

	table := newMapOfTable[K, V](m.minTableLen)
	m.table = table
	return table
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

// initSlow may be called concurrently by multiple goroutines, so it requires
// synchronization with a "lock" mechanism.
//
//go:noinline
func (m *MapOf[K, V]) initSlow() *mapOfTable[K, V] {
	// Create a temporary WaitGroup for initialization synchronization
	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !m.resizeWg.CompareAndSwap(nil, wg) {
		// Another goroutine is initializing, wait for it to complete
		if wg = m.resizeWg.Load(); wg != nil {
			wg.Wait()
		}
		return m.table
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := m.table
	if table != nil {
		m.resizeWg.Store(nil)
		wg.Done()
		return table
	}

	// Perform initialization
	table = m.init(nil, nil)
	m.table = table
	m.resizeWg.Store(nil)
	wg.Done()
	return table
}

// Load retrieves a value for a key, compatible with `sync.Map`.
func (m *MapOf[K, V]) Load(key K) (value V, ok bool) {
	table := m.table
	if table == nil {
		return
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	h2val := h2(hash)
	h2w := broadcast(h2val)
	bidx := uintptr(len(table.buckets)-1) & h1(hash)
	rootb := &table.buckets[bidx]

	for b := rootb; b != nil; b = b.next {
		metaw := b.meta
		for markedw := markZeroBytes(metaw^h2w) & metaMask; markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := b.entries[idx]; e != nil {
				if e.Key == key {
					return e.Value, true
				}
			}
		}
	}
	return
}

// Store inserts or updates a key-value pair, compatible with `sync.Map`.
func (m *MapOf[K, V]) Store(key K, value V) {
	table := m.table
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	m.mockSyncMap(table, hash, key, nil, &value, false)
}

// Swap stores a key-value pair and returns the previous value if any, compatible with `sync.Map`.
func (m *MapOf[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	table := m.table
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.mockSyncMap(table, hash, key, nil, &value, false)
}

// LoadOrStore retrieves an existing value or stores a new one if the key doesn't exist,
// compatible with `sync.Map`.
func (m *MapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := m.table
	if table == nil {
		table = m.initSlow()
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
		return m.mockSyncMap(table, hash, key, nil, &value, true)
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	if e := m.findEntry(table, hash, key); e != nil {
		return e.Value, true
	}
	return m.mockSyncMap(table, hash, key, nil, &value, true)
}

// Delete removes a key-value pair, compatible with `sync.Map`.
func (m *MapOf[K, V]) Delete(key K) {
	table := m.table
	if table == nil {
		return
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	m.mockSyncMap(table, hash, key, nil, nil, false)
}

// LoadAndDelete retrieves the value for a key and deletes it from the map,
// compatible with `sync.Map`.
func (m *MapOf[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	table := m.table
	if table == nil {
		return *new(V), false
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.mockSyncMap(table, hash, key, nil, nil, false)
}

// CompareAndSwap atomically replaces an existing value with a new value
// if the existing value matches the expected value, compatible with `sync.Map`.
func (m *MapOf[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	table := m.table
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndSwap when value is not of comparable type")
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	_, swapped = m.mockSyncMap(table, hash, key, &old, &new, false)
	return
}

// CompareAndDelete atomically deletes an existing entry
// if its value matches the expected value, compatible with `sync.Map`.
func (m *MapOf[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	table := m.table
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndDelete when value is not of comparable type")
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	_, deleted = m.mockSyncMap(table, hash, key, &old, nil, false)
	return
}

func (m *MapOf[K, V]) mockSyncMap(
	table *mapOfTable[K, V],
	hash uintptr,
	key K,
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
				if cmpValue != nil && !m.valEqual(unsafe.Pointer(&loaded.Value), noescape(unsafe.Pointer(cmpValue))) {
					return loaded, loaded.Value, false
				}
				if newValue == nil {
					// Delete
					return nil, loaded.Value, true
				}
				// Update
				newe := &EntryOf[K, V]{Value: *newValue}
				return newe, loaded.Value, true
			}

			if cmpValue != nil || newValue == nil {
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

// LoadAndStore loads the existing value for a key if present,
// otherwise stores the given value. Returns the actual value and
// a boolean indicating whether the key was present.
//
// Compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) LoadAndStore(key K, value V) (actual V, loaded bool) {
	table := m.table
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.processEntry(table, hash, key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: value}, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: value}, value, false
		},
	)
}

// LoadOrCompute loads the existing value for a key if present,
// otherwise computes and stores a new value using the provided function.
// LoadOrCompute compatible with `xsync.MapOf`.
//
// Compatible with `xsync.MapOf`.
// Alias: LoadOrStoreFn
func (m *MapOf[K, V]) LoadOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	table := m.table
	if table == nil {
		table = m.initSlow()
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
		return m.processEntry(table, hash, key,
			func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if loaded != nil {
					return loaded, loaded.Value, true
				}
				newValue := valueFn()
				return &EntryOf[K, V]{Value: newValue}, newValue, false
			},
		)
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	if e := m.findEntry(table, hash, key); e != nil {
		return e.Value, true
	}
	return m.processEntry(table, hash, key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return loaded, loaded.Value, true
			}
			newValue := valueFn()
			return &EntryOf[K, V]{Value: newValue}, newValue, false
		},
	)
}

// LoadOrTryCompute loads the existing value for a key if present,
// otherwise attempts to compute a new value using the provided function.
// The function can signal that it doesn't want to store a value by returning cancel=true.
//
// Compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) LoadOrTryCompute(
	key K,
	valueFn func() (newValue V, cancel bool),
) (value V, loaded bool) {

	table := m.table
	if table == nil {
		table = m.initSlow()
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
		return m.processEntry(table, hash, key,
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

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	if e := m.findEntry(table, hash, key); e != nil {
		return e.Value, true
	}
	return m.processEntry(table, hash, key,
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

// Compute either inserts a new value for a key or updates an existing value.
// The provided function receives the current value (if any) and decides what to do:
// - Return a new value to store
// - Return delete=true to remove the entry
//
// Compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) Compute(
	key K,
	valueFn func(oldValue V, loaded bool) (newValue V, delete bool),
) (actual V, ok bool) {

	table := m.table
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.processEntry(table, hash, key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				newValue, del := valueFn(loaded.Value, true)
				if del {
					return nil, loaded.Value, false
				}
				return &EntryOf[K, V]{Value: newValue}, newValue, true
			}
			newValue, del := valueFn(*new(V), false)
			if del {
				return nil, *new(V), false
			}
			return &EntryOf[K, V]{Value: newValue}, newValue, true
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

	table := m.table
	if table == nil {
		table = m.initSlow()
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
		return m.processEntry(table, hash, key,
			func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if loaded != nil {
					return loaded, loaded.Value, true
				}
				return valueFn()
			},
		)
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	if e := m.findEntry(table, hash, key); e != nil {
		return e.Value, true
	}
	return m.processEntry(table, hash, key,
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

	table := m.table
	if table == nil {
		table = m.initSlow()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.processEntry(table, hash, key, fn)
}

func (m *MapOf[K, V]) findEntry(table *mapOfTable[K, V], hash uintptr, key K) *EntryOf[K, V] {
	h2val := h2(hash)
	h2w := broadcast(h2val)
	bidx := uintptr(len(table.buckets)-1) & h1(hash)
	rootb := &table.buckets[bidx]

	for b := rootb; b != nil; b = b.next {
		metaw := b.meta
		for markedw := markZeroBytes(metaw^h2w) & metaMask; markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := b.entries[idx]; e != nil {
				if e.Key == key {
					return e
				}
			}
		}
	}
	return nil
}

func (m *MapOf[K, V]) processEntry(
	table *mapOfTable[K, V],
	hash uintptr,
	key K,
	fn func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (V, bool) {

	for {
		h2val := h2(hash)
		h2w := broadcast(h2val)
		bidx := uintptr(len(table.buckets)-1) & h1(hash)
		rootb := &table.buckets[bidx]

		rootb.mu.Lock()

		// Check if a resize is needed.
		// This is the first check, checking if there is a resize operation in progress
		// before acquiring the bucket lock
		//if wg := (*sync.WaitGroup)(atomic.LoadPointer(&m.resizeWg)); wg != nil {
		if wg := m.resizeWg.Load(); wg != nil {
			rootb.mu.Unlock()
			// Wait for the current resize operation to complete
			wg.Wait()
			table = m.table
			hash = m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
			continue
		}

		// Check if the table has changed.
		// This is the second check, checking if the table has been replaced by another
		// goroutine after acquiring the bucket lock
		// This is necessary because another goroutine may have completed a resize operation
		// between the first check and acquiring the bucket lock
		if newTable := m.table; table != newTable {
			rootb.mu.Unlock()
			table = newTable
			hash = m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
			continue
		}

		// Find an empty slot in advance
		var emptyb *bucketOf[K, V]
		var emptyidx int
		// If no empty slot is found, use the last slot
		var lastBucket *bucketOf[K, V]

		for b := rootb; b != nil; b = b.next {
			lastBucket = b
			metaw := b.meta

			if emptyb == nil {
				emptyw := metaw & defaultMetaMasked
				if emptyw != 0 {
					emptyb = b
					emptyidx = firstMarkedByteIndex(emptyw)
				}
			}

			for markedw := markZeroBytes(metaw^h2w) & metaMask; markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				if e := b.entries[idx]; e != nil {
					if e.Key == key {

						newe, value, status := fn(e)

						if newe == nil {
							// Delete
							newmetaw := setByte(metaw, emptyMetaSlot, idx)
							b.meta = newmetaw
							b.entries[idx] = nil
							rootb.mu.Unlock()
							table.addSize(bidx, -1)
							if newmetaw == defaultMeta {
								tableLen := len(table.buckets)
								if !m.growOnly &&
									m.minTableLen < tableLen &&
									table.sumSize() <= (tableLen*entriesPerMapOfBucket)/mapShrinkFraction {
									m.resize(table, mapShrinkHint)
								}
							}
							return value, status
						}
						if newe != e {
							// Update
							newe.Key = e.Key
							b.entries[idx] = newe
						}
						rootb.mu.Unlock()
						return value, status
					}
				}
			}
		}

		newe, value, status := fn(nil)
		if newe == nil {
			rootb.mu.Unlock()
			return value, status
		}
		// Insert
		newe.Key = key

		// Insert into empty slot
		if emptyb != nil {
			emptyb.meta = setByte(emptyb.meta, h2val, emptyidx)
			emptyb.entries[emptyidx] = newe
			rootb.mu.Unlock()
			table.addSize(bidx, 1)
			return value, status
		}

		// Check if expansion is needed
		growThreshold := int(float64(len(table.buckets)) * float64(entriesPerMapOfBucket) * mapLoadFactor)
		if table.sumSize() >= growThreshold { // table.sumSize()+1 > growThreshold
			rootb.mu.Unlock()
			table, _ = m.resize(table, mapGrowHint)
			hash = m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
			continue
		}

		// Create new bucket and insert
		lastBucket.next = &bucketOf[K, V]{
			meta:    setByte(defaultMeta, h2val, 0),
			entries: [entriesPerMapOfBucket]*EntryOf[K, V]{newe},
		}

		rootb.mu.Unlock()
		table.addSize(bidx, 1)
		return value, status
	}
}

// resize returns the current table and indicates whether it was created by the calling goroutine
//
// Parameters:
//   - knownTable: The previously obtained table reference.
//   - hint: The type of resize operation to perform.
//   - sizeHint: If provided, specifies the target size for growth.
//
// Returns:
//   - *mapOfTable: The most up-to-date table reference.
//   - bool: True if this goroutine performed the resize, false if another goroutine already did it.
//
//go:noinline
func (m *MapOf[K, V]) resize(
	knownTable *mapOfTable[K, V],
	hint mapResizeHint,
	sizeHint ...int) (*mapOfTable[K, V], bool) {
	// Create a new WaitGroup for the current resize operation
	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !m.resizeWg.CompareAndSwap(nil, wg) {
		// Someone else started resize. Wait for it to finish.
		if wg = m.resizeWg.Load(); wg != nil {
			wg.Wait()
		}
		return m.table, false
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := m.table
	if table != knownTable {
		// If the table has already been changed, return directly
		m.resizeWg.Store(nil)
		wg.Done()
		return table, false
	}
	tableLen := len(table.buckets)

	var newTableLen int
	if hint == mapGrowHint {
		if len(sizeHint) == 0 {
			// Grow the table with factor of 2.
			newTableLen = tableLen << 1
		} else {
			// Grow the table to sizeHint.
			newTableLen = calcTableLen(sizeHint[0])
		}
		m.totalGrowths++
	} else if hint == mapShrinkHint {
		// Shrink the table with factor of 2.
		newTableLen = tableLen >> 1
		m.totalShrinks--
	} else {
		newTableLen = m.minTableLen
	}
	newTable := newMapOfTable[K, V](newTableLen)
	if hint != mapClearHint {
		// Calculate the parallel count
		chunks := calcParallelism(tableLen, minParallelResizeThreshold, minBucketsPerGoroutine)
		if chunks > 1 {
			var copyWg sync.WaitGroup
			// Simply evenly distributed
			chunkSize := (tableLen + chunks - 1) / chunks

			for c := 0; c < chunks; c++ {
				copyWg.Add(1)
				go func(start, end int) {
					for i := start; i < end && i < tableLen; i++ {
						copied := copyBucketOfParallel[K, V](&table.buckets[i], newTable, m.keyHash)
						if copied > 0 {
							newTable.addSize(uintptr(i), copied)
						}
					}
					copyWg.Done()
				}(c*chunkSize, min((c+1)*chunkSize, tableLen))
			}
			copyWg.Wait()
		} else {
			// Serial processing
			for i := 0; i < tableLen; i++ {
				copied := copyBucketOf[K, V](&table.buckets[i], newTable, m.keyHash)
				newTable.addSizePlain(uintptr(i), copied)
			}
		}
	}
	m.table = newTable
	m.resizeWg.Store(nil)
	wg.Done()
	return newTable, true
}

// calcParallelism calculates the number of goroutines for parallel processing.
//
// Parameters:
//   - items: Number of items to process.
//   - threshold: Minimum threshold to enable parallel processing.
//   - minItemsPerGoroutine: Minimum number of items each goroutine should process.
//
// Returns:
//   - Suggested degree of parallelism (number of goroutines).
func calcParallelism(items, threshold, minItemsPerGoroutine int) int {
	// If the items is too small, use single-threaded processing.
	if items < threshold {
		return 1
	}

	// If there is only one processor, use single-threaded processing.
	numCPU := runtime.GOMAXPROCS(0)
	if numCPU <= 1 {
		return 1
	}

	// Use a logarithmic function to smoothly increase the degree of parallelism
	logFactor := 1 + bits.Len(uint(items)) - bits.Len(uint(threshold))
	chunks := min(numCPU*logFactor/2, items/minItemsPerGoroutine)
	return max(1, chunks) // Ensure there is at least 1 chunk
}

func copyBucketOf[K comparable, V any](
	srcBucket *bucketOf[K, V],
	destTable *mapOfTable[K, V],
	hasher hashFunc,
) (copied int) {
	b := srcBucket
	srcBucket.mu.Lock()
	for {
		for i := 0; i < entriesPerMapOfBucket; i++ {
			if e := (*EntryOf[K, V])(b.entries[i]); e != nil {
				// We could store the hash value in the Entry during processEntry to avoid
				// recalculating it here, which would speed up the resize process.
				// However, for simple integer keys, this approach would actually slow down
				// the load operation. Therefore, recalculating the hash value is the better approach.
				hash := hasher(noescape(unsafe.Pointer(&e.Key)), destTable.seed)
				bidx := uintptr(len(destTable.buckets)-1) & h1(hash)
				destb := &destTable.buckets[bidx]
				appendToBucketOf[K, V](b.entries[i], destb, h2(hash))
				copied++
			}
		}
		b = b.next
		if b == nil {
			srcBucket.mu.Unlock()
			return
		}
	}
}

// copyBucketOfParallel unlike copyBucketOf, it locks the destination bucket to ensure concurrency safety.
func copyBucketOfParallel[K comparable, V any](
	srcBucket *bucketOf[K, V],
	destTable *mapOfTable[K, V],
	hasher hashFunc,
) (copied int) {
	b := srcBucket
	srcBucket.mu.Lock()
	for {
		for i := 0; i < entriesPerMapOfBucket; i++ {
			if e := b.entries[i]; e != nil {
				hash := hasher(noescape(unsafe.Pointer(&e.Key)), destTable.seed)
				bidx := uintptr(len(destTable.buckets)-1) & h1(hash)
				destb := &destTable.buckets[bidx]
				// Locking the buckets of the target table is necessary during parallel copying,
				// when copying in a single goroutine, it's not necessary, but due to the spinning of the mutex,
				// it remains extremely fast.
				destb.mu.Lock()
				appendToBucketOf[K, V](b.entries[i], destb, h2(hash))
				destb.mu.Unlock()
				copied++
			}
		}
		b = b.next
		if b == nil {
			srcBucket.mu.Unlock()
			return
		}
	}
}

// appendToBucketOf appends an entry pointer to a bucket chain.
//
// Parameters:
//   - entryPtr: Pointer to the entry to be appended.
//   - b: Pointer to the bucket where the entry should be appended.
//   - h2: The h2 hash value for the entry.
func appendToBucketOf[K comparable, V any](
	entryPtr *EntryOf[K, V],
	b *bucketOf[K, V],
	h2 uint8,
) {
	for {
		for i := 0; i < entriesPerMapOfBucket; i++ {
			if b.entries[i] == nil {
				b.meta = setByte(b.meta, h2, i)
				b.entries[i] = entryPtr
				return
			}
		}
		if b.next == nil {
			b.next = &bucketOf[K, V]{
				meta:    setByte(defaultMeta, h2, 0),
				entries: [entriesPerMapOfBucket]*EntryOf[K, V]{entryPtr},
			}
			return
		}
		b = b.next
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

// RangeEntry iterates over all entries in the map.
//
// Notes:
//   - Never modify the Key or Value in an Entry under any circumstances.
//   - Range operates on the current table snapshot, which may not reflect the most up-to-date state.
//     Similar to `sync.Map`, this provides eventual consistency for reads.
func (m *MapOf[K, V]) RangeEntry(yield func(e *EntryOf[K, V]) bool) {
	table := m.table
	if table == nil {
		return
	}

	for i := range table.buckets {
		rootb := &table.buckets[i]
		for b := rootb; b != nil; b = b.next {
			for i := 0; i < entriesPerMapOfBucket; i++ {
				if e := b.entries[i]; e != nil {
					if !yield(e) {
						return
					}
				}
			}
		}
	}
	return
}

// Clear compatible with `sync.Map`
func (m *MapOf[K, V]) Clear() {
	table := m.table
	if table == nil {
		return
	}
	var ok bool
	for {
		if table, ok = m.resize(table, mapClearHint); ok {
			return
		}
	}
}

// Size returns the number of key-value pairs in the map.
// This is an O(1) operation.
//
// Compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) Size() int {
	table := m.table
	if table == nil {
		return 0
	}
	return table.sumSize()
}

// IsZero checks zero values, faster than Size().
func (m *MapOf[K, V]) IsZero() bool {
	table := m.table
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
	table := m.table
	if table == nil {
		return false
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.findEntry(table, hash, key) != nil
}

// String implement the formatting output interface fmt.Stringer
func (m *MapOf[K, V]) String() string {
	return strings.Replace(fmt.Sprint(m.ToMapWithLimit(1024)), "map[", "MapOf[", 1)
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

	for k, v := range a {
		m.Store(k, v)
	}
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
//   - parallelism: Controls the degree of parallelism:
//     0: Serial processing.
//     <0: Automatic determination based on workload.
//     >0: Specific number of goroutines to use.
func (m *MapOf[K, V]) batchProcess(
	table *mapOfTable[K, V],
	itemCount int,
	growFactor float64,
	processor func(table *mapOfTable[K, V], start, end int),
	parallelism int,
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
			var ok bool
			for {
				growThreshold := int(float64(len(table.buckets)) * float64(entriesPerMapOfBucket) * mapLoadFactor)
				sizeHint := table.sumSize() + newItemsEstimate
				if sizeHint <= growThreshold {
					break
				}
				if table, ok = m.resize(table, mapGrowHint, int(sizeHint)); ok {
					break
				}
			}
		}
	}

	// Calculate the parallel count
	chunks := parallelism
	if parallelism < 0 {
		chunks = calcParallelism(itemCount, minParallelBatchItems, minItemsPerGoroutine)
	}

	if chunks > 1 {
		// Calculate data volume for each goroutine
		chunkSize := (itemCount + chunks - 1) / chunks

		var wg sync.WaitGroup
		for i := 0; i < chunks; i++ {
			start := i * chunkSize
			end := min((i+1)*chunkSize, itemCount)
			if start >= end {
				break
			}

			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				processor(table, start, end)
			}(start, end)
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
//   - processFn: Function that receives entry and current value (if exists), returns new value, result value, and status.
//   - parallelism: Degree of parallelism (see batchProcess).
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
	parallelism int,
) (values []V, status []bool) {
	if len(immutableEntries) == 0 {
		return
	}

	values = make([]V, len(immutableEntries))
	status = make([]bool, len(immutableEntries))

	table := m.table

	m.batchProcess(table, len(immutableEntries), growFactor, func(table *mapOfTable[K, V], start, end int) {
		for i := start; i < end; i++ {
			hash := m.keyHash(noescape(unsafe.Pointer(&immutableEntries[i].Key)), table.seed)
			values[i], status[i] = m.processEntry(table, hash, immutableEntries[i].Key,
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(immutableEntries[i], loaded)
				},
			)
		}
	}, parallelism)

	return
}

// BatchProcessEntries batch processes multiple key-value pairs with a custom function
//
// Parameters:
//   - entries: slice of key-value pairs to process
//   - growFactor: capacity change coefficient (see batchProcess)
//   - processFn: function that receives entry and current value (if exists), returns new value, result value, and status
//   - parallelism: degree of parallelism (see batchProcess)
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
	parallelism int,
) (values []V, status []bool) {
	if len(entries) == 0 {
		return
	}

	values = make([]V, len(entries))
	status = make([]bool, len(entries))

	table := m.table

	m.batchProcess(table, len(entries), growFactor, func(table *mapOfTable[K, V], start, end int) {
		for i := start; i < end; i++ {
			hash := m.keyHash(noescape(unsafe.Pointer(&entries[i].Key)), table.seed)
			values[i], status[i] = m.processEntry(table, hash, entries[i].Key,
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(&entries[i], loaded)
				},
			)
		}
	}, parallelism)

	return
}

// BatchProcessKeys batch processes multiple keys with a custom function
//
// Parameters:
//   - keys: slice of keys to process
//   - growFactor: capacity change coefficient (see batchProcess)
//   - processFn: function that receives key and current value (if exists), returns new value, result value, and status
//   - parallelism: degree of parallelism (see batchProcess)
//
// Returns:
//   - values []V: slice of values from processFn
//   - status []bool: slice of status values from processFn
func (m *MapOf[K, V]) BatchProcessKeys(
	keys []K,
	growFactor float64,
	processFn func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
	parallelism int,
) (values []V, status []bool) {
	if len(keys) == 0 {
		return
	}

	values = make([]V, len(keys))
	status = make([]bool, len(keys))

	table := m.table

	m.batchProcess(table, len(keys), growFactor, func(table *mapOfTable[K, V], start, end int) {
		for i := start; i < end; i++ {
			hash := m.keyHash(noescape(unsafe.Pointer(&keys[i])), table.seed)
			values[i], status[i] = m.processEntry(table, hash, keys[i],
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(keys[i], loaded)
				},
			)
		}
	}, parallelism)

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
	return m.BatchProcessEntries(
		entries, 1.0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: entry.Value}, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: entry.Value}, *new(V), false
		},
		-1,
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
	return m.BatchProcessEntries(
		entries, 1.0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return loaded, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: entry.Value}, entry.Value, false
		},
		-1,
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
	return m.BatchProcessKeys(
		keys, 0,
		func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return nil, loaded.Value, true
			}
			return nil, *new(V), false
		},
		-1,
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
	return m.BatchProcessEntries(
		entries, 0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: entry.Value}, loaded.Value, true
			}
			return nil, *new(V), false
		},
		-1,
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

	table := m.table

	m.batchProcess(table, len(source), 1.0,
		func(table *mapOfTable[K, V], _, _ int) {
			// Directly iterate and process all key-value pairs
			for k, v := range source {
				hash := m.keyHash(noescape(unsafe.Pointer(&k)), table.seed)
				m.processEntry(table, hash, k,
					func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
						return &EntryOf[K, V]{Value: v}, v, e != nil
					},
				)
			}
		}, 0)
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
	table := m.table
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
		m.BatchProcessImmutableEntries(
			toUpsert, 1.0,
			func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				return entry, entry.Value, false
			},
			-1,
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
	table := m.table
	otherSize := other.Size()

	m.batchProcess(table, otherSize, 1.0,
		func(table *mapOfTable[K, V], _, _ int) {
			other.RangeEntry(func(other *EntryOf[K, V]) bool {
				hash := m.keyHash(noescape(unsafe.Pointer(&other.Key)), table.seed)
				m.processEntry(table, hash, other.Key,
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
		}, 0)
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
	if m.IsZero() {
		return &MapOf[K, V]{}
	}

	// Create a new MapOf with the same configuration as the original
	clone := &MapOf[K, V]{
		keyHash:     m.keyHash,
		valEqual:    m.valEqual,
		minTableLen: m.minTableLen,
		growOnly:    m.growOnly,
	}

	// Pre-fetch size to optimize initial capacity
	size := m.Size()
	if size > 0 {
		table := newMapOfTable[K, V](clone.minTableLen)
		clone.table = table
		clone.batchProcess(table, size, 1.0,
			func(table *mapOfTable[K, V], _, _ int) {
				// Directly iterate and process all key-value pairs
				m.RangeEntry(func(e *EntryOf[K, V]) bool {
					hash := clone.keyHash(noescape(unsafe.Pointer(&e.Key)), table.seed)
					clone.processEntry(table, hash, e.Key,
						func(_ *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
							return e, e.Value, false
						},
					)
					return true
				})
			}, 0)
	}

	return clone
}

// addSize atomically adds delta to the size counter for the given bucket index.
func (table *mapOfTable[K, V]) addSize(bucketIdx uintptr, delta int) {
	cidx := uintptr(len(table.size)-1) & bucketIdx
	atomic.AddUintptr(&table.size[cidx].c, uintptr(delta))
}

// addSizePlain adds delta to the size counter without atomic operations.
// This method should only be used when thread safety is guaranteed by the caller.
func (table *mapOfTable[K, V]) addSizePlain(bucketIdx uintptr, delta int) {
	cidx := uintptr(len(table.size)-1) & bucketIdx
	table.size[cidx].c += uintptr(delta)
}

// sumSize calculates the total number of entries in the table by summing all counter stripes.
func (table *mapOfTable[K, V]) sumSize() int {
	var sum int
	for i := range table.size {
		sum += int(atomic.LoadUintptr(&table.size[i].c))
	}
	return sum
}

// isZero checks if the table is empty by verifying all counter stripes are zero.
func (table *mapOfTable[K, V]) isZero() bool {
	for i := range table.size {
		if atomic.LoadUintptr(&table.size[i].c) != 0 {
			return false
		}
	}
	return true
}

// Stats returns statistics for the MapOf. Just like other map
// methods, this one is thread-safe. Yet it's an O(N) operation,
// so it should be used only for diagnostics or debugging purposes.
func (m *MapOf[K, V]) Stats() *MapStats {
	stats := &MapStats{
		TotalGrowths: m.totalGrowths,
		TotalShrinks: m.totalShrinks,
		MinEntries:   math.MaxInt,
	}
	table := m.table
	if table == nil {
		return stats
	}
	stats.RootBuckets = len(table.buckets)
	stats.Counter = table.sumSize()
	stats.CounterLen = len(table.size)
	for i := range table.buckets {
		nentries := 0
		b := &table.buckets[i]
		stats.TotalBuckets++
		for {
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
			if b.next == nil {
				break
			}
			b = b.next
			stats.TotalBuckets++
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
	TotalGrowths int
	// TotalGrowths is the number of times the hash table shrinked.
	TotalShrinks int
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

// h1 extracts the bucket index from a hash value.
func h1(h uintptr) uintptr {
	return h >> 7
}

// h2 extracts the byte-level hash for in-bucket lookups.
func h2(h uintptr) uint8 {
	return uint8(h & 0x7f)
}

// broadcast replicates a byte value across all bytes of a uint64.
func broadcast(b uint8) uint64 {
	return 0x101010101010101 * uint64(b)
}

// firstMarkedByteIndex finds the index of the first marked byte in a uint64.
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
// Returns a uint64 with the most significant bit of each byte set if that byte is zero.
func markZeroBytes(w uint64) uint64 {
	return (w - 0x0101010101010101) & (^w) & 0x8080808080808080
}

// setByte sets the byte at index idx in the uint64 w to the value b.
// Returns the modified uint64 value.
func setByte(w uint64, b uint8, idx int) uint64 {
	shift := idx << 3
	return (w &^ (0xff << shift)) | (uint64(b) << shift)
}

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

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
// nolint:all
//
//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

type hashFunc func(unsafe.Pointer, uintptr) uintptr
type equalFunc func(unsafe.Pointer, unsafe.Pointer) bool

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
