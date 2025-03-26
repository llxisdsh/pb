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
)

const (
	// number of MapOf entries per bucket; 5 entries lead to size of 64B
	// (one cache line) on 64-bit machines
	entriesPerMapOfBucket        = 5
	defaultMeta           uint64 = 0x8080808080808080
	metaMask              uint64 = 0xffffffffff
	defaultMetaMasked     uint64 = defaultMeta & metaMask
	emptyMetaSlot         uint8  = 0x80
	// cacheLineSize is used in paddings to prevent false sharing;
	// 64B are used instead of 128B as a compromise between
	// memory footprint and performance; 128B usage may give ~30%
	// improvement on NUMA machines.
	cacheLineSize = 64
)

type mapResizeHint int

const (
	mapGrowHint   mapResizeHint = 0
	mapShrinkHint mapResizeHint = 1
	mapClearHint  mapResizeHint = 2
)

const (
	// threshold fraction of table occupation to start a table shrinking
	// when deleting the last entry in a bucket chain
	mapShrinkFraction = 128
	// map findEntry factor to trigger a table resize during insertion;
	// a map holds up to mapLoadFactor*entriesPerMapBucket*mapTableLen
	// key-value pairs (this is a soft limit)
	mapLoadFactor = 0.75
	// minimal table size, i.e. number of buckets; thus, minimal map
	// capacity can be calculated as entriesPerMapBucket*defaultMinMapTableLen
	defaultMinMapTableLen = 32
	// minimum counter stripes to use
	minMapCounterLen = 8
	// maximum counter stripes to use; stands for around 4KB of memory
	maxMapCounterLen = 32
	// minimum table size threshold for parallel resizing
	minParallelResizeThreshold = 1024
	// minimum number of buckets each goroutine handles during parallel resizing
	minBucketsPerGoroutine = 32
	// minimum number of items for parallel processing,
	// below this threshold serial processing is used
	minParallelBatchItems = 64
	// minimum number of items each goroutine processes, used to control parallelism degree
	minItemsPerGoroutine = 32
)

// MapOf is compatible with sync.Map.
// Demonstrates 10x higher performance than `sync.Map in go 1.24` under large datasets
//
// Key features of pb.MapOf:
//   - Uses cache-line aligned for all structures
//   - Implements zero-value usability
//   - Lazy initialization
//   - Defaults to the Go built-in hash, customizable on creation or initialization.
//   - Provides complete sync.Map and compatibility
//   - Specially optimized for read operations
//   - Supports parallel resizing
//   - Offers rich functional extensions, as well as LoadOrCompute, ProcessEntry, Size, IsZero,
//     Clone, Batch processing functions, etc.
//   - All tests passed
//   - Extremely fast, see benchmark tests below
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
		table         unsafe.Pointer
		resizeWg      unsafe.Pointer
		totalGrowths  int64
		totalShrinks  int64
		keyHash       hashFunc
		valEqual      equalFunc
		minTableLen   int
		growOnly      bool
		enablePadding bool
	}{})%cacheLineSize) % cacheLineSize]byte

	table         unsafe.Pointer // *mapOfTable
	resizeWg      unsafe.Pointer // *sync.WaitGroup
	totalGrowths  int64
	totalShrinks  int64
	keyHash       hashFunc
	valEqual      equalFunc
	minTableLen   int  // WithPresize
	growOnly      bool // WithGrowOnly
	enablePadding bool // WithPadding
}

type mapOfTable struct {
	//lint:ignore U1000 prevents false sharing
	pad [(cacheLineSize - unsafe.Sizeof(struct {
		buckets []bucketOf
		size    []counterStripe
		seed    uintptr
	}{})%cacheLineSize) % cacheLineSize]byte

	buckets []bucketOf
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	size []counterStripe
	seed uintptr
}

type counterStripe struct {
	c int64
}

type counterStripeWithPadding struct {
	c int64
	//lint:ignore U1000 prevents false sharing
	pad [cacheLineSize - 8]byte
}

type bucketOf struct {
	//lint:ignore U1000 prevents false sharing
	pad [(cacheLineSize - unsafe.Sizeof(struct {
		meta    uint64
		entries [entriesPerMapOfBucket]unsafe.Pointer
		next    unsafe.Pointer
		mu      sync.Mutex
	}{})%cacheLineSize) % cacheLineSize]byte

	meta    uint64
	entries [entriesPerMapOfBucket]unsafe.Pointer // *EntryOf
	next    unsafe.Pointer                        // *bucketOf
	mu      sync.Mutex
}

// EntryOf is an immutable map entry.
type EntryOf[K comparable, V any] struct {
	Key   K
	Value V
}

// NewMapOf creates a new MapOf instance. Direct initialization is also supported.
//
// Parameters:
//
//   - WithPresize option for initial capacity,
//   - WithGrowOnly option to disable shrinking,
func NewMapOf[K comparable, V any](
	options ...func(*MapConfig),
) *MapOf[K, V] {
	return NewMapOfWithHasher[K, V](nil, nil, options...)
}

// NewMapOfWithHasher creates a MapOf with custom hashing and equality functions.
// Allows custom key hashing (keyHash) and value equality (valEqual) functions for compare-and-swap operations
//
// Parameters:
//
//   - keyHash: nil uses the built-in hasher
//   - valEqual: nil uses the built-in comparison, but if the value is not of a comparable type,
//     using the Compare series of functions will cause a panic
//   - WithPresize option for initial capacity,
//   - WithGrowOnly option to disable shrinking,
func NewMapOfWithHasher[K comparable, V any](
	keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig),
) *MapOf[K, V] {
	m := &MapOf[K, V]{}
	m.init(keyHash, valEqual, options...)
	return m
}

func newMapOfTable(minTableLen int, usePaddingCounter bool) *mapOfTable {
	buckets := make([]bucketOf, minTableLen)
	for i := range buckets {
		buckets[i].meta = defaultMeta
	}

	counterLen := minTableLen >> 10
	if counterLen < minMapCounterLen {
		counterLen = minMapCounterLen
	} else if counterLen > maxMapCounterLen {
		counterLen = maxMapCounterLen
	}

	var counter []counterStripe
	if enablePadding || usePaddingCounter {
		paddedCounter := make([]counterStripeWithPadding, counterLen)
		counter = unsafe.Slice((*counterStripe)(unsafe.Pointer(&paddedCounter[0])), counterLen)
	} else {
		counter = make([]counterStripe, counterLen)
	}

	t := &mapOfTable{
		buckets: buckets,
		size:    counter,
		seed:    uintptr(rand.Uint64()),
	}
	return t
}

var (
	jsonMarshal   func(v any) ([]byte, error)
	jsonUnmarshal func(data []byte, v any) error
)

// SetDefaultJsonMarshal sets the default JSON serialization and deserialization functions.
// If not set, the standard library is used by default.
func SetDefaultJsonMarshal(marshal func(v any) ([]byte, error), unmarshal func(data []byte, v any) error) {
	jsonMarshal, jsonUnmarshal = marshal, unmarshal
}

// Init Initialization the MapOf, Allows custom key hasher (keyHash) and value equality (valEqual) functions for compare-and-swap operations
// This function is not thread-safe, Even if this Init is not called, the Map will still be initialized automatically.
//
// Parameters:
//
//   - keyHash: nil uses the built-in hasher
//   - valEqual: nil uses the built-in comparison, but if the value is not of a comparable type,
//     using the Compare series of functions will cause a panic
//   - WithPresize option for initial capacity,
//   - WithGrowOnly option to disable shrinking,
func (m *MapOf[K, V]) Init(keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig)) {

	m.init(keyHash, valEqual, options...)
}

func (m *MapOf[K, V]) init(keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig)) *mapOfTable {

	c := &MapConfig{
		sizeHint: defaultMinMapTableLen * entriesPerMapOfBucket,
	}
	for _, o := range options {
		o(c)
	}
	m.keyHash, m.valEqual = defaultHasherByBuiltIn[K, V]()
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
	m.enablePadding = c.enablePadding

	table := newMapOfTable(m.minTableLen, m.enablePadding)
	m.table = unsafe.Pointer(table)
	return table
}

func calcTableLen(sizeHint int) int {
	tableLen := defaultMinMapTableLen
	if sizeHint > defaultMinMapTableLen*entriesPerMapOfBucket {
		tableLen = nextPowOf2(int((float64(sizeHint) / entriesPerMapOfBucket) / mapLoadFactor))
	}
	return tableLen
}

// initSlow will be called by multiple threads, so it needs to be synchronized with a "lock"
//
//go:noinline
func (m *MapOf[K, V]) initSlow() *mapOfTable {
	// Create a temporary WaitGroup for initialization synchronization
	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !atomic.CompareAndSwapPointer(&m.resizeWg, nil, unsafe.Pointer(wg)) {
		// Another thread is initializing, wait for it to complete
		wg = (*sync.WaitGroup)(atomic.LoadPointer(&m.resizeWg))
		if wg != nil {
			wg.Wait()
		}
		// Now the table should be initialized
		return (*mapOfTable)(atomic.LoadPointer(&m.table))
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table != nil {
		atomic.StorePointer(&m.resizeWg, nil)
		wg.Done()
		return table
	}

	// Perform initialization
	table = m.init(nil, nil)
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	atomic.StorePointer(&m.resizeWg, nil)
	wg.Done()
	return table
}

// Load compatible with `sync.Map`
//
// Notes:
// Using the build option `mapof_atomicreads` enables atomic reads,
// boosting consistency but slowing performance slightly. See its description for details.
func (m *MapOf[K, V]) Load(key K) (value V, ok bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return
	}

	// inline findEntry
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	h2val := h2(hash)
	h2w := broadcast(h2val)
	bidx := uintptr(len(table.buckets)-1) & h1(hash)
	rootb := &table.buckets[bidx]

	if //goland:noinspection GoBoolExpressions
	!atomicReads {
		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			metaw := b.meta
			for markedw := markZeroBytes(metaw^h2w) & metaMask; markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(b.entries[idx]); e != nil {
					if e.Key == key {
						return e.Value, true
					}
				}
			}
		}
		return
	}

	for b := rootb; b != nil; b = (*bucketOf)(atomic.LoadPointer(&b.next)) {
		metaw := atomic.LoadUint64(&b.meta)
		for markedw := markZeroBytes(metaw^h2w) & metaMask; markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := (*EntryOf[K, V])(atomic.LoadPointer(&b.entries[idx])); e != nil {
				if e.Key == key {
					return e.Value, true
				}
			}
		}
	}
	return
}

// Store compatible with `sync.Map`
func (m *MapOf[K, V]) Store(key K, value V) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	m.mockSyncMap(table, hash, key, nil, &value, false)
}

// Swap compatible with `sync.Map`
func (m *MapOf[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		table = m.initSlow()
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.mockSyncMap(table, hash, key, nil, &value, false)
}

// LoadOrStore compatible with `sync.Map`
func (m *MapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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

// Delete compatible with `sync.Map`
func (m *MapOf[K, V]) Delete(key K) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	m.mockSyncMap(table, hash, key, nil, nil, false)
}

// LoadAndDelete compatible with `sync.Map`
func (m *MapOf[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return *new(V), false
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.mockSyncMap(table, hash, key, nil, nil, false)
}

// CompareAndSwap compatible with `sync.Map`
func (m *MapOf[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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

// CompareAndDelete compatible with `sync.Map`
func (m *MapOf[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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
	table *mapOfTable,
	hash uintptr,
	key K,
	cmpValue *V,
	newValue *V,
	loadOrStore bool,
) (result V, ok bool) {
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

// LoadAndStore compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) LoadAndStore(key K, value V) (actual V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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

// LoadOrCompute compatible with `xsync.MapOf`.
// Alias: LoadOrStoreFn
func (m *MapOf[K, V]) LoadOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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

// LoadOrTryCompute compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) LoadOrTryCompute(
	key K,
	valueFn func() (newValue V, cancel bool),
) (value V, loaded bool) {

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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

// Compute compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) Compute(
	key K,
	valueFn func(oldValue V, loaded bool) (newValue V, delete bool),
) (actual V, ok bool) {

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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
//
//	key: The key to look up or process
//
//	valueFn: Function called when the key doesn't exist, returns:
//	 - *EntryOf[K, V]: New entry, nil means don't store any value
//	 - V: Result value to return to the caller
//	 - bool: Whether the operation succeeded
//
// Returns:
//   - result V: Existing value if key exists; otherwise the value returned by valueFn
//   - success bool: true if key exists; otherwise the bool value returned by valueFn
//
// Notes:
//   - The fn function is executed while holding an internal lock.
//     Keep the execution time short to avoid blocking other operations.
//   - Avoid calling other map methods inside fn to prevent deadlocks.
func (m *MapOf[K, V]) LoadOrProcessEntry(
	key K,
	valueFn func() (*EntryOf[K, V], V, bool),
) (result V, success bool) {

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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
//
//	key: The key to process
//
//	fn: Processing function that receives the current entry and returns the result:
//	  - Input parameter loaded *EntryOf[K, V]: Current entry, nil means key doesn't exist;
//	  - Return value *EntryOf[K, V]:
//		nil means delete the entry,
//		same as input means no modification,
//	 	new entry means update the value;
//	  - Return value V: Returned as ProcessEntry's result;
//	  - Return value bool: Returned as ProcessEntry's status;
//
// Returns:
//   - result V: First return value from fn
//   - success bool: Second return value from fn
//
// Notes:
//   - The input parameter loaded is immutable and should not be modified directly
//   - This method internally ensures thread safety and consistency
//   - If you need to modify a value, return a new EntryOf instance
//   - The fn function is executed while holding an internal lock.
//     Keep the execution time short to avoid blocking other operations.
//   - Avoid calling other map methods inside fn to prevent deadlocks.
//   - Do not perform expensive computations or I/O operations inside fn.
func (m *MapOf[K, V]) ProcessEntry(
	key K,
	fn func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (result V, success bool) {

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		table = m.initSlow()
	}

	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.processEntry(table, hash, key, fn)
}

func (m *MapOf[K, V]) findEntry(table *mapOfTable, hash uintptr, key K) *EntryOf[K, V] {
	h2val := h2(hash)
	h2w := broadcast(h2val)
	bidx := uintptr(len(table.buckets)-1) & h1(hash)
	rootb := &table.buckets[bidx]

	if //goland:noinspection GoBoolExpressions
	!atomicReads {
		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			metaw := b.meta
			for markedw := markZeroBytes(metaw^h2w) & metaMask; markedw != 0; markedw &= markedw - 1 {
				idx := firstMarkedByteIndex(markedw)
				if e := (*EntryOf[K, V])(b.entries[idx]); e != nil {
					if e.Key == key {
						return e
					}
				}
			}
		}
		return nil
	}

	for b := rootb; b != nil; b = (*bucketOf)(atomic.LoadPointer(&b.next)) {
		metaw := atomic.LoadUint64(&b.meta)
		for markedw := markZeroBytes(metaw^h2w) & metaMask; markedw != 0; markedw &= markedw - 1 {
			idx := firstMarkedByteIndex(markedw)
			if e := (*EntryOf[K, V])(atomic.LoadPointer(&b.entries[idx])); e != nil {
				if e.Key == key {
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
		if wg := (*sync.WaitGroup)(atomic.LoadPointer(&m.resizeWg)); wg != nil {
			rootb.mu.Unlock()
			// Wait for the current resize operation to complete
			wg.Wait()
			table = (*mapOfTable)(atomic.LoadPointer(&m.table))
			hash = m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
			continue
		}

		// Check if the table has changed.
		// This is the second check, checking if the table has been replaced by another
		// thread after acquiring the bucket lock
		// This is necessary because another thread may have completed a resize operation
		// between the first check and acquiring the bucket lock
		if newTable := (*mapOfTable)(atomic.LoadPointer(&m.table)); table != newTable {
			rootb.mu.Unlock()
			table = newTable
			hash = m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
			continue
		}

		// Find an empty slot in advance
		var emptyb *bucketOf
		var emptyidx int
		// If no empty slot is found, use the last slot
		var lastBucket *bucketOf

		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
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
				if e := (*EntryOf[K, V])(b.entries[idx]); e != nil {
					if e.Key == key {

						newe, result, ok := fn(e)

						if newe == nil {
							// Delete
							newmetaw := setByte(metaw, emptyMetaSlot, idx)
							atomic.StoreUint64(&b.meta, newmetaw)
							atomic.StorePointer(&b.entries[idx], nil)
							rootb.mu.Unlock()
							table.addSize(bidx, -1)
							if newmetaw == defaultMeta {
								tableLen := len(table.buckets)
								if !m.growOnly &&
									m.minTableLen < tableLen &&
									table.sumSize() <= int64((tableLen*entriesPerMapOfBucket)/mapShrinkFraction) {
									m.resize(table, mapShrinkHint)
								}
							}
							return result, ok
						}
						if newe != e {
							// Update
							newe.Key = e.Key
							atomic.StorePointer(&b.entries[idx], unsafe.Pointer(newe))
						}
						rootb.mu.Unlock()
						return result, ok
					}
				}
			}
		}

		newe, result, ok := fn(nil)
		if newe == nil {
			rootb.mu.Unlock()
			return result, ok
		}
		// Insert
		newe.Key = key

		// Insert into empty slot
		if emptyb != nil {
			atomic.StoreUint64(&emptyb.meta, setByte(emptyb.meta, h2val, emptyidx))
			atomic.StorePointer(&emptyb.entries[emptyidx], unsafe.Pointer(newe))
			rootb.mu.Unlock()
			table.addSize(bidx, 1)
			return result, ok
		}

		// Check if expansion is needed
		growThreshold := int64(float64(len(table.buckets)) * entriesPerMapOfBucket * mapLoadFactor)
		if table.sumSize() >= growThreshold { // table.sumSize()+1 > growThreshold
			rootb.mu.Unlock()
			table, _ = m.resize(table, mapGrowHint)
			hash = m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
			continue
		}

		// Create new bucket and insert
		atomic.StorePointer(&lastBucket.next, unsafe.Pointer(&bucketOf{
			meta:    setByte(defaultMeta, h2val, 0),
			entries: [entriesPerMapOfBucket]unsafe.Pointer{unsafe.Pointer(newe)},
		}))
		rootb.mu.Unlock()
		table.addSize(bidx, 1)
		return result, ok
	}
}

// resize Returns the current latest table and whether it was created by current thread
//
// Parameters:
//   - knownTable: The table that was previously obtained.
//   - hint: The type of resize.
//   - sizeHint: if provided, specifies the target size for the growth.
//
// Returns:
//   - *mapOfTable: Always returns the latest table.
//   - bool: The return value false indicates that another thread has already performed the resize.
func (m *MapOf[K, V]) resize(
	knownTable *mapOfTable,
	hint mapResizeHint,
	sizeHint ...int) (*mapOfTable, bool) {
	// Create a new WaitGroup for the current resize operation
	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !atomic.CompareAndSwapPointer(&m.resizeWg, nil, unsafe.Pointer(wg)) {
		// Someone else started resize. Wait for it to finish.
		if wg = (*sync.WaitGroup)(atomic.LoadPointer(&m.resizeWg)); wg != nil {
			wg.Wait()
		}
		return (*mapOfTable)(atomic.LoadPointer(&m.table)), false
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table != knownTable {
		// If the table has already been changed, return directly
		atomic.StorePointer(&m.resizeWg, nil)
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
		atomic.AddInt64(&m.totalGrowths, 1)
	} else if hint == mapShrinkHint {
		// Shrink the table with factor of 2.
		newTableLen = tableLen >> 1
		atomic.AddInt64(&m.totalShrinks, 1)
	} else {
		newTableLen = m.minTableLen
	}
	newTable := newMapOfTable(newTableLen, m.enablePadding)
	if hint != mapClearHint {
		// Parallel copying is only performed if the table size is at least minParallelResizeThreshold
		// and there are multiple CPUs.
		// The degree of parallelism is min(number of CPUs, table size / minBucketsPerGoroutine),
		// ensuring that each goroutine handles at least minBucketsPerGoroutine buckets.
		numCPU := runtime.NumCPU()
		chunks := 1
		if numCPU > 1 && tableLen >= minParallelResizeThreshold {
			chunks = min(numCPU, tableLen/minBucketsPerGoroutine)
		}

		if chunks > 1 {
			var copyWg sync.WaitGroup
			// Simply evenly distributed
			chunkSize := (tableLen + chunks - 1) / chunks

			for c := 0; c < chunks; c++ {
				copyWg.Add(1)
				go func(start, end int) {
					for i := start; i < end && i < tableLen; i++ {
						copied := copyBucketOf[K, V](&table.buckets[i], newTable, m.keyHash)
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
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	atomic.StorePointer(&m.resizeWg, nil)
	wg.Done()
	return newTable, true
}

func copyBucketOf[K comparable, V any](
	srcBucket *bucketOf,
	destTable *mapOfTable,
	hasher hashFunc,
) (copied int) {
	b := srcBucket
	srcBucket.mu.Lock()
	for {
		for i := 0; i < entriesPerMapOfBucket; i++ {
			if e := (*EntryOf[K, V])(b.entries[i]); e != nil {
				// It is also possible to store the hash value in the Entry during processEntry,
				// saving the need to recalculate it here, which can speed up the resize process.
				// However, for keys of simple int type, this would actually slow down the load operation.
				// Therefore, it is better to recalculate the hash value.
				hash := hasher(noescape(unsafe.Pointer(&e.Key)), destTable.seed)
				bidx := uintptr(len(destTable.buckets)-1) & h1(hash)
				destb := &destTable.buckets[bidx]

				// Locking the buckets of the target table is necessary during parallel copying,
				// when copying in a single thread, it's not necessary, but due to the spinning of the mutex,
				// it remains extremely fast.
				destb.mu.Lock()
				appendToBucketOf(b.entries[i], destb, h2(hash))
				destb.mu.Unlock()
				copied++
			}
		}
		b = (*bucketOf)(b.next)
		if b == nil {
			srcBucket.mu.Unlock()
			return
		}
	}
}

func appendToBucketOf(
	entryPtr unsafe.Pointer,
	b *bucketOf,
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
			b.next = unsafe.Pointer(&bucketOf{
				meta:    setByte(defaultMeta, h2, 0),
				entries: [entriesPerMapOfBucket]unsafe.Pointer{entryPtr},
			})
			return
		}
		b = (*bucketOf)(b.next)
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

// RangeEntry is used to iterate over all entries.
//
// Notes:
//   - Never modify the Key or Value in an Entry under any circumstances.
//   - Range always uses the current table, which may not reflect the most up-to-date situation.
//     Like `sync.Map`, reads only satisfy eventual consistency.
//   - Using the build option `mapof_atomicreads` enables atomic reads,
//     boosting consistency but slowing performance slightly. See its description for details.
//     By default, it performs lock-free iteration.
func (m *MapOf[K, V]) RangeEntry(yield func(e *EntryOf[K, V]) bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return
	}

	if //goland:noinspection GoBoolExpressions
	!atomicReads {
		for i := range table.buckets {
			rootb := &table.buckets[i]
			for b := rootb; b != nil; b = (*bucketOf)(b.next) {
				for i := 0; i < entriesPerMapOfBucket; i++ {
					if e := (*EntryOf[K, V])(b.entries[i]); e != nil {
						if !yield(e) {
							return
						}
					}
				}
			}
		}
		return
	}

	// Pre-allocate array big enough to fit entries for most hash tables.
	entries := make([]*EntryOf[K, V], 0, 16*entriesPerMapOfBucket)
	for i := range table.buckets {
		rootb := &table.buckets[i]
		rootb.mu.Lock()
		for b := rootb; b != nil; b = (*bucketOf)(b.next) {
			for i := 0; i < entriesPerMapOfBucket; i++ {
				if e := (*EntryOf[K, V])(b.entries[i]); e != nil {
					entries = append(entries, e)
				}
			}
		}
		rootb.mu.Unlock()
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

// Clear compatible with `sync.Map`
func (m *MapOf[K, V]) Clear() {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
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

// Size compatible with `xsync.MapOf`
func (m *MapOf[K, V]) Size() int {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return 0
	}
	return int(table.sumSize())
}

// IsZero checks zero values, faster than Size().
func (m *MapOf[K, V]) IsZero() bool {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return true
	}
	return table.isZero()
}

// ToMap Collect all entries and return a map[K]V
func (m *MapOf[K, V]) ToMap() map[K]V {
	a := make(map[K]V, m.Size())
	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		a[e.Key] = e.Value
		return true
	})
	return a
}

// ToMapWithLimit Collect up to limit entries into a map[K]V, limit < 0 is no limit
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
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return false
	}
	hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
	return m.findEntry(table, hash, key) != nil
}

// String Implement the formatting output interface fmt.Stringer
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

// batchProcess is the core generic function for batch operations
// It processes a group of keys or key-value pairs and applies the specified processing function to each item
//
// Parameters:
//   - table: current hash table
//   - itemCount: number of items to be processed
//   - growFactor: capacity change coefficient,
//     > 0: estimate new items as itemCount * growFactor,
//     <=0: no estimation for new items,
//   - processor: function to process each item
//   - parallelism: degree of parallelism, 0 for serial processing, negative for using CPU core count
func (m *MapOf[K, V]) batchProcess(
	table *mapOfTable,
	itemCount int,
	growFactor float64,
	processor func(table *mapOfTable, start, end int),
	parallelism int,
) {
	if table == nil {
		table = m.initSlow()
	}

	// Calculate estimated new items based on growFactor
	if growFactor > 0 {
		// Estimate new items
		newItemsEstimate := int64(float64(itemCount) * growFactor)

		// Pre-growth check
		if newItemsEstimate > 0 {
			// Retry the resize until it succeeds
			var ok bool
			for {
				growThreshold := int64(float64(len(table.buckets)) * entriesPerMapOfBucket * mapLoadFactor)
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

	// Determine whether to use parallel processing
	if parallelism != 0 && itemCount >= minParallelBatchItems {
		// Calculate optimal parallelism
		numCPU := runtime.NumCPU()

		// If user specified negative parallelism, use CPU core count
		if parallelism < 0 {
			parallelism = numCPU
		}

		// Calculate reasonable parallelism based on data volume and minimum items per goroutine
		// Ensure each goroutine processes at least minItemsPerGoroutine items
		optimalParallelism := min(parallelism, itemCount/minItemsPerGoroutine)

		// Ensure parallelism is at least 1
		if optimalParallelism > 1 {
			// Calculate data volume for each goroutine
			chunkSize := (itemCount + optimalParallelism - 1) / optimalParallelism

			var wg sync.WaitGroup
			for i := 0; i < optimalParallelism; i++ {
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
	}

	// Serial processing
	processor(table, 0, itemCount)
}

// batchProcessEntries is a generic function for processing key-value pair slices
// It applies the processFn to each entry in the slice with appropriate parallelism
func (m *MapOf[K, V]) batchProcessEntries(
	entries []EntryOf[K, V],
	growFactor float64,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
	parallelism int,
) ([]V, []bool) {
	if len(entries) == 0 {
		return nil, nil
	}

	values := make([]V, len(entries))
	status := make([]bool, len(entries))

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))

	m.batchProcess(table, len(entries), growFactor, func(table *mapOfTable, start, end int) {
		for i := start; i < end; i++ {
			hash := m.keyHash(noescape(unsafe.Pointer(&entries[i].Key)), table.seed)
			values[i], status[i] = m.processEntry(table, hash, entries[i].Key,
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(&entries[i], loaded)
				},
			)
		}
	}, parallelism)

	return values, status
}

// batchProcessKeys is a generic function for processing key slices
// It applies the processFn to each key in the slice with appropriate parallelism
func (m *MapOf[K, V]) batchProcessKeys(
	keys []K,
	growFactor float64,
	processFn func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
	parallelism int,
) ([]V, []bool) {
	if len(keys) == 0 {
		return nil, nil
	}

	values := make([]V, len(keys))
	status := make([]bool, len(keys))

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))

	m.batchProcess(table, len(keys), growFactor, func(table *mapOfTable, start, end int) {
		for i := start; i < end; i++ {
			hash := m.keyHash(noescape(unsafe.Pointer(&keys[i])), table.seed)
			values[i], status[i] = m.processEntry(table, hash, keys[i],
				func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
					return processFn(keys[i], loaded)
				},
			)
		}
	}, parallelism)

	return values, status
}

// BatchUpsert batch updates or inserts multiple key-value pairs, returning previous values
//
// Parameters:
//   - entries: slice of key-value pairs to upsert
//   - parallelism: degree of parallelism, 0 for serial processing, negative for using CPU core count
//
// Returns:
//   - previous: slice of previous values for each key
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchUpsert(entries []EntryOf[K, V], parallelism int) (previous []V, loaded []bool) {
	return m.batchProcessEntries(
		entries, 1.0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: entry.Value}, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: entry.Value}, *new(V), false
		},
		parallelism,
	)
}

// BatchInsert batch inserts multiple key-value pairs, not modifying existing keys
//
// Parameters:
//   - entries: slice of key-value pairs to insert
//   - parallelism: degree of parallelism, 0 for serial processing, negative for using CPU core count
//
// Returns:
//   - actual: slice of actual values for each key (either existing or newly inserted)
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchInsert(entries []EntryOf[K, V], parallelism int) (actual []V, loaded []bool) {
	return m.batchProcessEntries(
		entries, 1.0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return loaded, loaded.Value, true
			}
			return &EntryOf[K, V]{Value: entry.Value}, entry.Value, false
		},
		parallelism,
	)
}

// BatchDelete batch deletes multiple keys, returning previous values
//
// Parameters:
//   - keys: slice of keys to delete
//   - parallelism: degree of parallelism, 0 for serial processing, negative for using CPU core count
//
// Returns:
//   - previous: slice of previous values for each key
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchDelete(keys []K, parallelism int) (previous []V, loaded []bool) {
	return m.batchProcessKeys(
		keys, 0,
		func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return nil, loaded.Value, true
			}
			return nil, *new(V), false
		},
		parallelism,
	)
}

// BatchUpdate batch updates multiple existing keys, returning previous values
//
// Parameters:
//   - entries: slice of key-value pairs to update
//   - parallelism: degree of parallelism, 0 for serial processing, negative for using CPU core count
//
// Returns:
//   - previous: slice of previous values for each key
//   - loaded: slice of booleans indicating whether each key existed before
func (m *MapOf[K, V]) BatchUpdate(entries []EntryOf[K, V], parallelism int) (previous []V, loaded []bool) {
	return m.batchProcessEntries(
		entries, 0,
		func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if loaded != nil {
				return &EntryOf[K, V]{Value: entry.Value}, loaded.Value, true
			}
			return nil, *new(V), false
		},
		parallelism,
	)
}

// BatchProcessKeys batch processes multiple keys with a custom function
//
// Parameters:
//   - keys: slice of keys to process
//   - growFactor: capacity change coefficient (see batchProcess)
//   - processFn: function that receives key and current value (if exists), returns new value, result value, and success status
//   - parallelism: degree of parallelism, 0 for serial processing, negative for using CPU core count
//
// Returns:
//   - results []V: slice of result values from processFn
//   - success []bool: slice of success status values from processFn
func (m *MapOf[K, V]) BatchProcessKeys(
	keys []K,
	growFactor float64,
	processFn func(key K, loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
	parallelism int,
) (results []V, success []bool) {
	return m.batchProcessKeys(keys, growFactor, processFn, parallelism)
}

// BatchProcessEntries batch processes multiple key-value pairs with a custom function
//
// Parameters:
//   - entries: slice of key-value pairs to process
//   - growFactor: capacity change coefficient (see batchProcess)
//   - processFn: function that receives entry and current value (if exists), returns new value, result value, and success status
//   - parallelism: degree of parallelism, 0 for serial processing, negative for using CPU core count
//
// Returns:
//   - results []V: slice of result values from processFn
//   - success []bool: slice of success status values from processFn
func (m *MapOf[K, V]) BatchProcessEntries(
	entries []EntryOf[K, V],
	growFactor float64,
	processFn func(entry *EntryOf[K, V], loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
	parallelism int,
) (results []V, success []bool) {
	return m.batchProcessEntries(entries, growFactor, processFn, parallelism)
}

// FromMap imports key-value pairs from a standard Go map
//
// Parameters:
//   - source: standard Go map to import from
func (m *MapOf[K, V]) FromMap(source map[K]V) {
	if len(source) == 0 {
		return
	}

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))

	m.batchProcess(table, len(source), 1.0,
		func(table *mapOfTable, _, _ int) {
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
//   - transformFn: transforms values of kept elements, returns new value and whether update is needed
//     if nil, only filtering is performed
func (m *MapOf[K, V]) FilterAndTransform(
	filterFn func(key K, value V) bool,
	transformFn func(key K, value V) (V, bool),
) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return
	}

	// Process elements directly during iteration to avoid extra memory allocation
	var toProcess []EntryOf[K, V]
	var toDelete []K

	m.RangeEntry(func(e *EntryOf[K, V]) bool {
		key, value := e.Key, e.Value
		if !filterFn(key, value) {
			toDelete = append(toDelete, key)
		} else if transformFn != nil {
			newValue, needUpdate := transformFn(key, value)
			if needUpdate {
				toProcess = append(toProcess, EntryOf[K, V]{Key: key, Value: newValue})
			}
		}
		return true
	})

	// Batch delete elements that don't meet the condition
	if len(toDelete) > 0 {
		m.BatchDelete(toDelete, 0)
	}

	// Batch update elements that meet the condition
	if len(toProcess) > 0 {
		m.BatchUpsert(toProcess, 0)
	}
}

// Merge merges another MapOf into the current one
//
// Parameters:
//   - other: the MapOf to merge from
//   - conflictFn: conflict resolution function called when a key exists in both maps
//     if nil, values from other map override current map values
func (m *MapOf[K, V]) Merge(
	other *MapOf[K, V],
	conflictFn func(key K, thisVal, otherVal V) V,
) {
	if other == nil || other.IsZero() {
		return
	}

	// Default conflict handler: use value from other map
	if conflictFn == nil {
		conflictFn = func(_ K, _, otherVal V) V {
			return otherVal
		}
	}

	// Pre-fetch target table to avoid multiple checks in loop
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	otherSize := other.Size()

	m.batchProcess(table, otherSize, 1.0,
		func(table *mapOfTable, _, _ int) {
			other.RangeEntry(func(e *EntryOf[K, V]) bool {
				key, otherVal := e.Key, e.Value
				hash := m.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
				m.processEntry(table, hash, key,
					func(Loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
						if Loaded == nil {
							// Key doesn't exist in current Map, add directly
							return &EntryOf[K, V]{Value: otherVal}, otherVal, false
						}
						// Key exists in both Maps, use conflict handler
						newVal := conflictFn(key, Loaded.Value, otherVal)
						return &EntryOf[K, V]{Value: newVal}, newVal, true
					},
				)
				return true
			})
		}, 0)
}

// Clone creates a complete copy of the current MapOf
// Returns a new MapOf containing the same key-value pairs
func (m *MapOf[K, V]) Clone() *MapOf[K, V] {
	if m.IsZero() {
		return &MapOf[K, V]{}
	}

	// Create a new MapOf with the same configuration as the original
	clone := &MapOf[K, V]{
		keyHash:       m.keyHash,
		valEqual:      m.valEqual,
		minTableLen:   m.minTableLen,
		growOnly:      m.growOnly,
		enablePadding: m.enablePadding,
	}

	// Pre-fetch size to optimize initial capacity
	size := m.Size()
	if size > 0 {
		table := newMapOfTable(clone.minTableLen, clone.enablePadding)
		clone.table = unsafe.Pointer(table)
		clone.batchProcess(table, size, 1.0,
			func(table *mapOfTable, _, _ int) {
				// Directly iterate and process all key-value pairs
				m.RangeEntry(func(e *EntryOf[K, V]) bool {
					key, value := e.Key, e.Value
					hash := clone.keyHash(noescape(unsafe.Pointer(&key)), table.seed)
					clone.processEntry(table, hash, key,
						func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
							return &EntryOf[K, V]{Value: value}, value, loaded != nil
						},
					)
					return true
				})
			}, 0)
	}

	return clone
}

func (table *mapOfTable) addSize(bucketIdx uintptr, delta int) {
	cidx := uintptr(len(table.size)-1) & bucketIdx
	atomic.AddInt64(&table.size[cidx].c, int64(delta))
}

func (table *mapOfTable) addSizePlain(bucketIdx uintptr, delta int) {
	cidx := uintptr(len(table.size)-1) & bucketIdx
	table.size[cidx].c += int64(delta)
}

func (table *mapOfTable) sumSize() int64 {
	sum := int64(0)
	for i := range table.size {
		sum += atomic.LoadInt64(&table.size[i].c)
	}
	return sum
}

func (table *mapOfTable) isZero() bool {
	for i := range table.size {
		if atomic.LoadInt64(&table.size[i].c) != 0 {
			return false
		}
	}
	return true
}

// Stats returns statistics for the MapOf. Just like other map
// methods, this one is thread-safe. Yet it's an O(N) operation,
// so it should be used only for diagnostics or debugging purposes.
func (m *MapOf[K, V]) Stats() MapStats {
	stats := MapStats{
		TotalGrowths: atomic.LoadInt64(&m.totalGrowths),
		TotalShrinks: atomic.LoadInt64(&m.totalShrinks),
		MinEntries:   math.MaxInt32,
	}
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return stats
	}
	stats.RootBuckets = len(table.buckets)
	stats.Counter = int(table.sumSize())
	stats.CounterLen = len(table.size)
	for i := range table.buckets {
		nentries := 0
		b := &table.buckets[i]
		stats.TotalBuckets++
		for {
			nentriesLocal := 0
			stats.Capacity += entriesPerMapOfBucket
			for i := 0; i < entriesPerMapOfBucket; i++ {
				if atomic.LoadPointer(&b.entries[i]) != nil {
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
			b = (*bucketOf)(atomic.LoadPointer(&b.next))
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
	sizeHint      int
	growOnly      bool
	enablePadding bool
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

// WithPadding configures whether a new MapOf instance enables padding to avoid false sharing.
// Enabling padding may improve performance in high-concurrency environments but will increase memory usage.
//
// Deprecated: use the build option `mapof_enablepadding` instead.
func WithPadding() func(*MapConfig) {
	return func(c *MapConfig) {
		c.enablePadding = true
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
	TotalGrowths int64
	// TotalGrowths is the number of times the hash table shrinked.
	TotalShrinks int64
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

func h1(h uintptr) uintptr {
	return h >> 7
}

func h2(h uintptr) uint8 {
	return uint8(h & 0x7f)
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
func broadcast(b uint8) uint64 {
	return 0x101010101010101 * uint64(b)
}

func firstMarkedByteIndex(w uint64) int {
	return bits.TrailingZeros64(w) >> 3
}

// SWAR byte search: may produce false positives, e.g. for 0x0100,
// so make sure to double-check bytes found by this function.
func markZeroBytes(w uint64) uint64 {
	return (w - 0x0101010101010101) & (^w) & 0x8080808080808080
}

func setByte(w uint64, b uint8, idx int) uint64 {
	shift := idx << 3
	return (w &^ (0xff << shift)) | (uint64(b) << shift)
}

type hashFunc func(unsafe.Pointer, uintptr) uintptr
type equalFunc func(unsafe.Pointer, unsafe.Pointer) bool

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

func defaultHasherByBuiltIn[K comparable, V any]() (keyHash hashFunc, valEqual equalFunc) {
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
