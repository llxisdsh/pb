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
	minParallelResizeThreshold = 256
	// minimum number of buckets each goroutine handles during parallel resizing
	minBucketsPerGoroutine = 16
)

// MapOf is compatible with sync.Map.
// It draws heavily from `xsync.MapOf` v3 and further implements fine-grained optimizations.
// Demonstrates 10x higher performance than `sync.Map in go 1.24` under large datasets
//
// Unlike `xsync.MapOf`:
//
// # Zero-initialization ready to use
//
// # Full `sync.Map` compatibility (all tests passed)
//
// # Further abstracted the logic, added functions such as `ProcessEntry` and `IsZero`
//
// # Performance is slightly better than the `xsync.MapOf` v3
//
// Must thank the authors of [xsync](https://github.com/puzpuzpuz/xsync) for their contributions.
//
// Below is an introduction to xsync.MapOf:
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
// and C++'s absl::flat_hash_map (meta memory and SWAR-based
// lookups).
type MapOf[K comparable, V any] struct {
	// Sort fields by access frequency.
	table         unsafe.Pointer // *mapOfTable
	keyHash       hashFunc
	resizeWg      unsafe.Pointer // *sync.WaitGroup
	valEqual      equalFunc
	minTableLen   int
	growOnly      bool
	enablePadding bool
	totalGrowths  int64
	totalShrinks  int64
}

type mapOfTable struct {
	// Sort fields by access frequency.
	buckets []bucketOf
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	size []counterStripe
	seed uintptr
	//lint:ignore U1000 prevents false sharing
	pad [(cacheLineSize - unsafe.Sizeof(struct {
		buckets []bucketOf
		size    []counterStripe
		seed    uint64
	}{})%cacheLineSize) % cacheLineSize]byte
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
		entries [entriesPerMapOfBucket]unsafe.Pointer
		meta    uint64
		next    unsafe.Pointer
		mu      sync.Mutex
	}{})%cacheLineSize) % cacheLineSize]byte

	// Sort fields by access frequency.
	entries [entriesPerMapOfBucket]unsafe.Pointer // *EntryOf
	meta    uint64
	next    unsafe.Pointer // *bucketOf
	mu      sync.Mutex
}

// EntryOf is an immutable map entry.
type EntryOf[K comparable, V any] struct {
	Key   K
	Value V
}

// NewMapOf creates a new MapOf instance. Direct initialization is also supported.
//
// options:
//
//	WithPresize for initial capacity
//	WithGrowOnly to disable shrinking
//	WithPadding enables padding for fields such as size to avoid false sharing
func NewMapOf[K comparable, V any](
	options ...func(*MapConfig),
) *MapOf[K, V] {
	return NewMapOfWithHasher[K, V](nil, nil, options...)
}

// NewMapOfWithHasher creates a MapOf with custom hashing and equality functions
//
// # Allows custom key hashing (keyHash) and value equality (valEqual) functions for compare-and-swap operations
//
// keyHash:
//
//	nil uses the built-in hasher
//
// valEqual:
//
//	nil uses the built-in comparison, but if the value is not of a comparable type, using the Compare series of functions will cause a panic
//
// options:
//
//	WithPresize for initial capacity
//	WithGrowOnly to disable shrinking
//	WithPadding enables padding for fields such as size to avoid false sharing
func NewMapOfWithHasher[K comparable, V any](
	keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig),
) *MapOf[K, V] {
	m := &MapOf[K, V]{}
	m.init(keyHash, valEqual, options...)
	return m
}

func newMapOfTable(minTableLen int, enablePadding bool) *mapOfTable {
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
	if enablePadding {
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

// Init Initialization the MapOf with custom hashing or equality functions
//
// # Allows custom key hasher (keyHash) and value equality (valEqual) functions for compare-and-swap operations
//
// This function is not thread-safe, Even if this Init is not called, the Map will still be initialized automatically.
//
// keyHash:
//
//	nil uses the built-in hasher
//
// valEqual:
//
//	nil uses the built-in comparison, but if the value is not of a comparable type, using the Compare series of functions will cause a panic
//
// options:
//
//	WithPresize for initial capacity
//	WithGrowOnly to disable shrinking
//	WithPadding enables padding for fields such as size to avoid false sharing
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

	tableLen := defaultMinMapTableLen
	if c.sizeHint > defaultMinMapTableLen*entriesPerMapOfBucket {
		tableLen = int(nextPowOf2(uint32((float64(c.sizeHint) / entriesPerMapOfBucket) / mapLoadFactor)))
	}

	table := newMapOfTable(tableLen, c.enablePadding)
	m.table = unsafe.Pointer(table)
	m.minTableLen = tableLen
	m.growOnly = c.growOnly
	m.enablePadding = c.enablePadding
	return table
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
func (m *MapOf[K, V]) Load(key K) (value V, ok bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return
	}
	// Inline findEntry
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	h2val := h2(hash)
	h2w := broadcast(h2val)
	bidx := uint64(len(table.buckets)-1) & h1(hash)

	for b := &table.buckets[bidx]; b != nil; b = (*bucketOf)(atomic.LoadPointer(&b.next)) {
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
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	m.mockSyncMap(table, hash, key, nil, &value, false)
}

// Swap compatible with `sync.Map`
func (m *MapOf[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		table = m.initSlow()
	}
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	return m.mockSyncMap(table, hash, key, nil, &value, false)
}

// LoadOrStore compatible with `sync.Map`
func (m *MapOf[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		table = m.initSlow()
		hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
		return m.mockSyncMap(table, hash, key, nil, &value, true)
	}
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
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
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	m.mockSyncMap(table, hash, key, nil, nil, false)
}

// LoadAndDelete compatible with `sync.Map`
func (m *MapOf[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return *new(V), false
	}
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
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
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
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
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	_, deleted = m.mockSyncMap(table, hash, key, &old, nil, false)
	return
}

func (m *MapOf[K, V]) mockSyncMap(
	table *mapOfTable,
	hash uint64,
	key K,
	cmpValue *V,
	newValue *V,
	loadOrStore bool,
) (result V, ok bool) {
	return m.processEntry(table, hash, key,
		func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if e != nil {
				if loadOrStore {
					return e, e.Value, true
				}
				if cmpValue != nil && !m.valEqual(unsafe.Pointer(&e.Value), noescape(unsafe.Pointer(cmpValue))) {
					return e, e.Value, false
				}
				if newValue == nil {
					// Delete
					return nil, e.Value, true
				}
				// Update
				newe := &EntryOf[K, V]{Value: *newValue}
				return newe, e.Value, true
			}

			if cmpValue != nil || newValue == nil {
				return e, *new(V), false
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
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	return m.processEntry(table, hash, key,
		func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if e != nil {
				return &EntryOf[K, V]{Value: value}, e.Value, true
			}
			return &EntryOf[K, V]{Value: value}, value, false
		},
	)
}

// LoadOrCompute compatible with `xsync.MapOf`.
func (m *MapOf[K, V]) LoadOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		table = m.initSlow()
		hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
		return m.processEntry(table, hash, key,
			func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if e != nil {
					return e, e.Value, true
				}
				newValue := valueFn()
				return &EntryOf[K, V]{Value: newValue}, newValue, false
			},
		)
	}

	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	if e := m.findEntry(table, hash, key); e != nil {
		return e.Value, true
	}
	return m.processEntry(table, hash, key,
		func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if e != nil {
				return e, e.Value, true
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
		hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
		return m.processEntry(table, hash, key,
			func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if e != nil {
					return e, e.Value, true
				}
				newValue, cancel := valueFn()
				if cancel {
					return nil, *new(V), false
				}
				return &EntryOf[K, V]{Value: newValue}, newValue, false
			},
		)
	}

	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	if e := m.findEntry(table, hash, key); e != nil {
		return e.Value, true
	}
	return m.processEntry(table, hash, key,
		func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if e != nil {
				return e, e.Value, true
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
	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	return m.processEntry(table, hash, key,
		func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if e != nil {
				newValue, del := valueFn(e.Value, true)
				if del {
					return nil, e.Value, false
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

// LoadOrProcessEntry loads an existing value or computes a new one using the provided function
func (m *MapOf[K, V]) LoadOrProcessEntry(
	key K,
	fn func() (*EntryOf[K, V], V, bool),
) (result V, ok bool) {

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		table = m.initSlow()
		hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
		return m.processEntry(table, hash, key,
			func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
				if e != nil {
					return e, e.Value, true
				}
				return fn()
			},
		)
	}

	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	if e := m.findEntry(table, hash, key); e != nil {
		return e.Value, true
	}
	return m.processEntry(table, hash, key,
		func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			if e != nil {
				return e, e.Value, true
			}
			return fn()
		},
	)
}

// ProcessEntry processes a value using the provided function, see mockSyncMap for examples
//
// Parameters:
//
//	fn:
//		func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool)
//			e is the loaded entry, e == nil means it doesn't exist,
//			e.Value can retrieve the current value,
//			don't modify e, don't modify e, don't modify e,
//			return value *EntryOf[K, V] == nil means delete, != e means new value
//			return values V, bool are returned as ProcessEntry's result
//
//	Returns:
//
//	(result V, ok bool) are the return values from fn, their meaning is determined by fn
func (m *MapOf[K, V]) ProcessEntry(
	key K,
	fn func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (result V, ok bool) {

	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		table = m.initSlow()
	}

	hash := uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
	return m.processEntry(table, hash, key, fn)
}

func (m *MapOf[K, V]) findEntry(table *mapOfTable, hash uint64, key K) *EntryOf[K, V] {
	h2val := h2(hash)
	h2w := broadcast(h2val)
	bidx := uint64(len(table.buckets)-1) & h1(hash)

	for b := &table.buckets[bidx]; b != nil; b = (*bucketOf)(atomic.LoadPointer(&b.next)) {
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
	hash uint64,
	key K,
	fn func(e *EntryOf[K, V]) (*EntryOf[K, V], V, bool),
) (V, bool) {

	for {
		h2val := h2(hash)
		h2w := broadcast(h2val)
		bidx := uint64(len(table.buckets)-1) & h1(hash)
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
			hash = uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
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
			hash = uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
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
								m.resize(table, mapShrinkHint)
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
		if table.sumSize() > int64(float64(len(table.buckets))*entriesPerMapOfBucket*mapLoadFactor) {
			rootb.mu.Unlock()
			table = m.resize(table, mapGrowHint)
			hash = uint64(m.keyHash(noescape(unsafe.Pointer(&key)), table.seed))
			continue
		}

		// Create new bucket and insert
		atomic.StorePointer(&lastBucket.next, unsafe.Pointer(&bucketOf{
			entries: [entriesPerMapOfBucket]unsafe.Pointer{unsafe.Pointer(newe)},
			meta:    setByte(defaultMeta, h2val, 0),
		}))
		rootb.mu.Unlock()
		table.addSize(bidx, 1)
		return result, ok
	}
}

func (m *MapOf[K, V]) resize(knownTable *mapOfTable, hint mapResizeHint) *mapOfTable {
	knownTableLen := len(knownTable.buckets)
	minTableLen := m.minTableLen
	growOnly := m.growOnly
	// Fast path for shrink attempts.
	if hint == mapShrinkHint {
		if growOnly ||
			minTableLen == knownTableLen ||
			knownTable.sumSize() > int64((knownTableLen*entriesPerMapOfBucket)/mapShrinkFraction) {
			return knownTable
		}
	}

	// Slow path
	// Create a new WaitGroup for the current resize operation
	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Try to set resizeWg, if successful it means we've acquired the "lock"
	if !atomic.CompareAndSwapPointer(&m.resizeWg, nil, unsafe.Pointer(wg)) {
		// Someone else started resize. Wait for it to finish.
		if wg = (*sync.WaitGroup)(atomic.LoadPointer(&m.resizeWg)); wg != nil {
			wg.Wait()
		}
		return (*mapOfTable)(atomic.LoadPointer(&m.table))
	}

	// Although the table is always changed when resizeWg is not nil,
	// it might have been changed before that.
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	tableLen := len(table.buckets)

	var newTable *mapOfTable
	var newTableLen int

	switch hint {
	case mapGrowHint:
		// Grow the table with factor of 2.
		newTableLen = tableLen << 1
		atomic.AddInt64(&m.totalGrowths, 1)
	case mapShrinkHint:
		shrinkThreshold := int64((tableLen * entriesPerMapOfBucket) / mapShrinkFraction)
		if tableLen > minTableLen && table.sumSize() > shrinkThreshold {
			// No need to shrink. Wake up all waiters and give up.
			atomic.StorePointer(&m.resizeWg, nil)
			wg.Done()
			return table
		}
		// Shrink the table with factor of 2.
		newTableLen = tableLen >> 1
		atomic.AddInt64(&m.totalShrinks, 1)
	case mapClearHint:
		newTableLen = minTableLen
	default:
		panic(fmt.Sprintf("unexpected resize hint: %d", hint))
	}

	newTable = newMapOfTable(newTableLen, m.enablePadding)

	// Copy the data only if we're not clearing the map.
	if hint != mapClearHint {
		// Parallel copying is only performed if the table size is at least 256 and there are multiple CPUs.
		// The degree of parallelism is min(number of CPUs, table size / 16),
		// ensuring that each goroutine handles at least 8 buckets.
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
						copied := copyBucketOfThreadSafe[K, V](&table.buckets[i], newTable, m.keyHash)
						if copied > 0 {
							newTable.addSize(uint64(i), copied)
						}
					}
					copyWg.Done()
				}(c*chunkSize, min((c+1)*chunkSize, tableLen))
			}
			copyWg.Wait()
		} else {
			// Serial processing
			for i := 0; i < tableLen; i++ {
				copied := copyBucketOfThreadSafe[K, V](&table.buckets[i], newTable, m.keyHash)
				newTable.addSizePlain(uint64(i), copied)
			}
		}
	}
	// Publish the new table and wake up all waiters.
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	atomic.StorePointer(&m.resizeWg, nil)
	wg.Done()
	return newTable
}

func copyBucketOfThreadSafe[K comparable, V any](
	srcBucket *bucketOf,
	destTable *mapOfTable,
	hasher hashFunc,
) (copied int) {
	b := srcBucket
	// Locking the source buckets will block processEntry, which seems unnecessary,
	// but it actually makes the resize faster.
	srcBucket.mu.Lock()
	for {
		for i := 0; i < entriesPerMapOfBucket; i++ {
			if e := (*EntryOf[K, V])(b.entries[i]); e != nil {
				// It is also possible to store the hash value in the Entry during processEntry,
				// saving the need to recalculate it here, which can speed up the resize process.
				// However, for keys of simple int type, this would actually slow down the load operation.
				// Therefore, it is better to recalculate the hash value.
				hash := uint64(hasher(noescape(unsafe.Pointer(&e.Key)), destTable.seed))
				bidx := uint64(len(destTable.buckets)-1) & h1(hash)
				destb := &destTable.buckets[bidx]
				appendToBucketOfThreadSafe(h2(hash), b.entries[i], destb)
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

func appendToBucketOfThreadSafe(h2 uint8, entryPtr unsafe.Pointer, destBucket *bucketOf) {
	b := destBucket
	// Locking the buckets of the target table is necessary during parallel copying,
	// when copying in a single thread, it's not necessary, but due to the spinning of the mutex,
	// it remains extremely fast.
	destBucket.mu.Lock()
	for {
		for i := 0; i < entriesPerMapOfBucket; i++ {
			if b.entries[i] == nil {
				b.entries[i] = entryPtr
				b.meta = setByte(b.meta, h2, i)
				destBucket.mu.Unlock()
				return
			}
		}
		if b.next == nil {
			b.next = unsafe.Pointer(&bucketOf{
				entries: [entriesPerMapOfBucket]unsafe.Pointer{entryPtr},
				meta:    setByte(defaultMeta, h2, 0),
			})
			destBucket.mu.Unlock()
			return
		}
		b = (*bucketOf)(b.next)
	}
}

// All compatible with `sync.Map`.
func (m *MapOf[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Range compatible with `sync.Map`.
func (m *MapOf[K, V]) Range(f func(key K, value V) bool) {
	table := (*mapOfTable)(atomic.LoadPointer(&m.table))
	if table == nil {
		return
	}

	// Pre-allocate array big enough to fit entries for most hash tables.
	var initialCap int
	if len(table.buckets) > 0 {
		initialCap = min(1024, len(table.buckets)*entriesPerMapOfBucket>>2)
	}
	entries := make([]unsafe.Pointer, 0, initialCap)
	for i := range table.buckets {
		rootb := &table.buckets[i]
		b := rootb
		// Prevent concurrent modifications and copy all entries into
		// the intermediate slice.
		rootb.mu.Lock()
		for {
			for i := 0; i < entriesPerMapOfBucket; i++ {
				if b.entries[i] != nil {
					entries = append(entries, b.entries[i])
				}
			}
			if b.next == nil {
				rootb.mu.Unlock()
				break
			}
			b = (*bucketOf)(b.next)
		}
		// Call the function for all copied entries.
		for j := range entries {
			e := (*EntryOf[K, V])(entries[j])
			if !f(e.Key, e.Value) {
				return
			}
			// Remove the reference to avoid preventing the copied
			// entries from being GCed until this method finishes.
			entries[j] = nil
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
	m.resize(table, mapClearHint)
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
	a := make(map[K]V)
	m.Range(func(k K, v V) bool {
		a[k] = v
		return true
	})
	return a
}

// String Implement the formatting output interface fmt.Print %v
func (m *MapOf[K, V]) String() string {
	return strings.Replace(fmt.Sprint(m.ToMap()), "map[", "MapOf[", 1)
}

// MarshalJSON JSON serialization
func (m *MapOf[K, V]) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.ToMap())
}

// UnmarshalJSON JSON deserialization
func (m *MapOf[K, V]) UnmarshalJSON(data []byte) error {
	var a map[K]V
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	for k, v := range a {
		m.Store(k, v)
	}
	return nil
}

func (table *mapOfTable) addSize(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	atomic.AddInt64(&table.size[cidx].c, int64(delta))
}

func (table *mapOfTable) addSizePlain(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
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

// MapConfig defines configurable Map/MapOf options.
type MapConfig struct {
	sizeHint      int
	growOnly      bool
	enablePadding bool
}

// WithPresize configures new Map/MapOf instance with capacity enough
// to hold sizeHint entries. The capacity is treated as the minimal
// capacity meaning that the underlying hash table will never shrink
// to a smaller capacity. If sizeHint is zero or negative, the value
// is ignored.
func WithPresize(sizeHint int) func(*MapConfig) {
	return func(c *MapConfig) {
		c.sizeHint = sizeHint
	}
}

// WithGrowOnly configures new Map/MapOf instance to be grow-only.
// This means that the underlying hash table grows in capacity when
// new keys are added, but does not shrink when keys are deleted.
// The only exception to this rule is the Clear method which
// shrinks the hash table back to the initial capacity.
func WithGrowOnly() func(*MapConfig) {
	return func(c *MapConfig) {
		c.growOnly = true
	}
}

// WithPadding configures whether a new Map/MapOf instance enables padding to avoid false sharing.
// Enabling padding may improve performance in high-concurrency environments but will increase memory usage.
func WithPadding() func(*MapConfig) {
	return func(c *MapConfig) {
		c.enablePadding = true
	}
}

// MapStats is Map/MapOf statistics.
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
	// Capacity is the Map/MapOf capacity, i.e. the total number of
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

func h1(h uint64) uint64 {
	return h >> 7
}

func h2(h uint64) uint8 {
	return uint8(h & 0x7f)
}

// nextPowOf2 computes the next highest power of 2 of 32-bit v.
// Source: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
func nextPowOf2(v uint32) uint32 {
	if v == 0 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
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
