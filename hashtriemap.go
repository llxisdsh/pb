package pb

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// HashTrieMap is an optimization of the built-in HashTrieMap.
//
// Supports lazy value generation with LoadOrCompute,
// Faster than the built-in HashTrieMap by more than 50%.
// The optimization for the built-in HashTrieMap is:
// Remove inited field, use root instead
// Lazy initialization, only init on write, and use the resulting root for
// subsequent logic to reduce atomic calls
//
// The built-in HashTrieMap is an implementation of a concurrent hash-trie. The implementation
// is designed around frequent loads, but offers decent performance for stores
// and deletes as well, especially if the map is larger. Its primary use-case is
// the unique package, but can be used elsewhere as well.
//
// The zero HashTrieMap is empty and ready to use.
// It must not be copied after first use.
type HashTrieMap[K comparable, V any] struct {
	initMu   sync.Mutex
	root     atomic.Pointer[indirect[K, V]]
	keyHash  hashFunc
	valEqual equalFunc
	seed     uintptr
}

func (ht *HashTrieMap[K, V]) init() *indirect[K, V] {
	if i := ht.root.Load(); i != nil {
		return i
	}
	return ht.initSlow()
}

//go:noinline
func (ht *HashTrieMap[K, V]) initSlow() *indirect[K, V] {
	ht.initMu.Lock()
	i := ht.root.Load()
	if i != nil {
		// Someone got to it while we were waiting.
		ht.initMu.Unlock()
		return i
	}
	ht.keyHash, ht.valEqual = defaultHasherUsingBuiltIn[K, V]()
	ht.seed = uintptr(rand.Uint64())
	i = newIndirectNode[K, V](nil)
	ht.root.Store(i)
	ht.initMu.Unlock()
	return i
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (ht *HashTrieMap[K, V]) Load(key K) (value V, ok bool) {
	i := ht.root.Load()
	if i == nil {
		return
	}

	hash := ht.keyHash(noescape(unsafe.Pointer(&key)), ht.seed)
	hashShift := 8 * ptrSize
	for hashShift != 0 {
		hashShift -= nChildrenLog2

		n := i.children[(hash>>hashShift)&nChildrenMask].Load()
		if n == nil {
			return *new(V), false
		}
		if n.isEntry {
			return n.entry().lookup(key)
		}
		i = n.indirect()
	}
	panic("internal/sync.HashTrieMap: ran out of hash bits while iterating")
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (ht *HashTrieMap[K, V]) LoadOrStore(key K, value V) (result V, loaded bool) {
	return ht.LoadOrStoreFn(key, func() V { return value })
}

// LoadOrStoreFn is similar to LoadOrStore, but uses a generator function for lazy value creation
func (ht *HashTrieMap[K, V]) LoadOrStoreFn(key K, valueFn func() V) (result V, loaded bool) {
	i := ht.init()
	hash := ht.keyHash(noescape(unsafe.Pointer(&key)), ht.seed)
	var hashShift uint
	var slot *atomic.Pointer[node[K, V]]
	var n *node[K, V]
	for ; ; i = ht.root.Load() {
		// Find the key or a candidate location for insertion.
		//i = ht.root.Load()
		hashShift = 8 * ptrSize
		haveInsertPoint := false
		for hashShift != 0 {
			hashShift -= nChildrenLog2

			slot = &i.children[(hash>>hashShift)&nChildrenMask]
			n = slot.Load()
			if n == nil {
				// We found a nil slot which is a candidate for insertion.
				haveInsertPoint = true
				break
			}
			if n.isEntry {
				// We found an existing entry, which is as far as we can go.
				// If it stays this way, we'll have to replace it with an
				// indirect node.
				if v, ok := n.entry().lookup(key); ok {
					return v, true
				}
				haveInsertPoint = true
				break
			}
			i = n.indirect()
		}
		if !haveInsertPoint {
			panic("internal/sync.HashTrieMap: ran out of hash bits while iterating")
		}

		// Grab the lock and double-check what we saw.
		i.mu.Lock()
		n = slot.Load()
		if (n == nil || n.isEntry) && !i.dead.Load() {
			// What we saw is still true, so we can continue with the insert.
			break
		}
		// We have to start over.
		i.mu.Unlock()
	}
	// N.B. This lock is held from when we broke out of the outer loop above.
	// We specifically break this out so that we can use defer here safely.
	// One option is to break this out into a new function instead, but
	// there's so much local iteration state used below that this turns out
	// to be cleaner.
	//defer i.mu.Unlock()

	var oldEntry *entry[K, V]
	if n != nil {
		oldEntry = n.entry()
		if v, ok := oldEntry.lookup(key); ok {
			// Easy case: by loading again, it turns out exactly what we wanted is here!
			i.mu.Unlock()
			return v, true
		}
	}
	result = valueFn()
	newEntry := newEntryNode(hash, key, result)
	if oldEntry == nil {
		// Easy case: create a new entry and store it.
		slot.Store(&newEntry.node)
	} else {
		// We possibly need to expand the entry already there into one or more new nodes.
		//
		// Publish the node last, which will make both oldEntry and newEntry visible. We
		// don't want readers to be able to observe that oldEntry isn't in the tree.
		slot.Store(ht.expand(oldEntry, newEntry, hash, hashShift, i))
	}
	i.mu.Unlock()
	return result, false
}

// expand takes oldEntry and newEntry whose hashes conflict from bit 64 down to hashShift and
// produces a subtree of indirect nodes to hold the two new entries.
func (ht *HashTrieMap[K, V]) expand(oldEntry, newEntry *entry[K, V], newHash uintptr, hashShift uint, parent *indirect[K, V]) *node[K, V] {
	// Check for a hash collision.
	//oldHash := ht.keyHash(oldEntry.key, ht.seed)
	if oldEntry.hash == newHash {
		// Store the old entry in the new entry's overflow list, then store
		// the new entry.
		newEntry.overflow.Store(oldEntry)
		return &newEntry.node
	}
	// We have to add an indirect node. Worse still, we may need to add more than one.
	newIndirect := newIndirectNode(parent)
	top := newIndirect
	for {
		if hashShift == 0 {
			panic("internal/sync.HashTrieMap: ran out of hash bits while inserting")
		}
		hashShift -= nChildrenLog2 // hashShift is for the level parent is at. We need to go deeper.
		oi := (oldEntry.hash >> hashShift) & nChildrenMask
		ni := (newHash >> hashShift) & nChildrenMask
		if oi != ni {
			newIndirect.children[oi].Store(&oldEntry.node)
			newIndirect.children[ni].Store(&newEntry.node)
			break
		}
		nextIndirect := newIndirectNode(newIndirect)
		newIndirect.children[oi].Store(&nextIndirect.node)
		newIndirect = nextIndirect
	}
	return &top.node
}

// Store sets the value for a key.
func (ht *HashTrieMap[K, V]) Store(key K, old V) {
	ht.Swap(key, old)
}

// Swap swaps the value for a key and returns the previous value if any.
// The loaded result reports whether the key was present.
func (ht *HashTrieMap[K, V]) Swap(key K, new V) (previous V, loaded bool) {
	i := ht.init()

	hash := ht.keyHash(noescape(unsafe.Pointer(&key)), ht.seed)
	var hashShift uint
	var slot *atomic.Pointer[node[K, V]]
	var n *node[K, V]
	for ; ; i = ht.root.Load() {
		// Find the key or a candidate location for insertion.
		// i = ht.root.Load()
		hashShift = 8 * ptrSize
		haveInsertPoint := false
		for hashShift != 0 {
			hashShift -= nChildrenLog2

			slot = &i.children[(hash>>hashShift)&nChildrenMask]
			n = slot.Load()
			if n == nil || n.isEntry {
				// We found a nil slot which is a candidate for insertion,
				// or an existing entry that we'll replace.
				haveInsertPoint = true
				break
			}
			i = n.indirect()
		}
		if !haveInsertPoint {
			panic("internal/sync.HashTrieMap: ran out of hash bits while iterating")
		}

		// Grab the lock and double-check what we saw.
		i.mu.Lock()
		n = slot.Load()
		if (n == nil || n.isEntry) && !i.dead.Load() {
			// What we saw is still true, so we can continue with the insert.
			break
		}
		// We have to start over.
		i.mu.Unlock()
	}
	// N.B. This lock is held from when we broke out of the outer loop above.
	// We specifically break this out so that we can use defer here safely.
	// One option is to break this out into a new function instead, but
	// there's so much local iteration state used below that this turns out
	// to be cleaner.
	//defer i.mu.Unlock()

	var zero V
	var oldEntry *entry[K, V]
	if n != nil {
		// Swap if the keys compare.
		oldEntry = n.entry()
		newEntry, old, swapped := oldEntry.swap(hash, key, new)
		if swapped {
			slot.Store(&newEntry.node)
			i.mu.Unlock()
			return old, true
		}
	}
	// The keys didn't compare, so we're doing an insertion.
	newEntry := newEntryNode(hash, key, new)
	if oldEntry == nil {
		// Easy case: create a new entry and store it.
		slot.Store(&newEntry.node)
	} else {
		// We possibly need to expand the entry already there into one or more new nodes.
		//
		// Publish the node last, which will make both oldEntry and newEntry visible. We
		// don't want readers to be able to observe that oldEntry isn't in the tree.
		slot.Store(ht.expand(oldEntry, newEntry, hash, hashShift, i))
	}
	i.mu.Unlock()
	return zero, false
}

// CompareAndSwap swaps the old and new values for key
// if the value stored in the map is equal to old.
// The value type must be of a comparable type, otherwise CompareAndSwap will panic.
func (ht *HashTrieMap[K, V]) CompareAndSwap(key K, old, new V) (swapped bool) {
	i := ht.root.Load()
	if i == nil {
		return
	}
	if ht.valEqual == nil {
		panic("called CompareAndSwap when value is not of comparable type")
	}
	hash := ht.keyHash(noescape(unsafe.Pointer(&key)), ht.seed)

	// Find a node with the key and compare with it. n != nil if we found the node.
	i, _, slot, n := ht.find(i, key, hash, ht.valEqual, old)
	//if i != nil {
	//	defer i.mu.Unlock()
	//}
	if n == nil {
		return false
	}

	// Try to swap the entry.
	e, swapped := n.entry().compareAndSwap(hash, key, old, new, ht.valEqual)
	if !swapped {
		i.mu.Unlock()
		// Nothing was actually swapped, which means the node is no longer there.
		return false
	}
	// Store the entry back because it changed.
	slot.Store(&e.node)
	i.mu.Unlock()
	return true
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (ht *HashTrieMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	i := ht.root.Load()
	if i == nil {
		return
	}
	hash := ht.keyHash(noescape(unsafe.Pointer(&key)), ht.seed)

	// Find a node with the key and compare with it. n != nil if we found the node.
	i, hashShift, slot, n := ht.find(i, key, hash, nil, *new(V))
	if n == nil {
		if i != nil {
			i.mu.Unlock()
		}
		return *new(V), false
	}

	// Try to delete the entry.
	v, e, loaded := n.entry().loadAndDelete(key)
	if !loaded {
		// Nothing was actually deleted, which means the node is no longer there.
		i.mu.Unlock()
		return *new(V), false
	}
	if e != nil {
		// We didn't actually delete the whole entry, just one entry in the chain.
		// Nothing else to do, since the parent is definitely not empty.
		slot.Store(&e.node)
		i.mu.Unlock()
		return v, true
	}
	// Delete the entry.
	slot.Store(nil)

	// Check if the node is now empty (and isn't the root), and delete it if able.
	for i.parent != nil && i.empty() {
		if hashShift == 8*ptrSize {
			panic("internal/sync.HashTrieMap: ran out of hash bits while iterating")
		}
		hashShift += nChildrenLog2

		// Delete the current node in the parent.
		parent := i.parent
		parent.mu.Lock()
		i.dead.Store(true)
		parent.children[(hash>>hashShift)&nChildrenMask].Store(nil)
		i.mu.Unlock()
		i = parent
	}
	i.mu.Unlock()
	return v, true
}

// Delete deletes the value for a key.
func (ht *HashTrieMap[K, V]) Delete(key K) {
	ht.LoadAndDelete(key)
}

// CompareAndDelete deletes the entry for key if its value is equal to old.
// The value type must be comparable, otherwise this CompareAndDelete will panic.
//
// If there is no current value for key in the map, CompareAndDelete returns false
// (even if the old value is the nil interface value).
func (ht *HashTrieMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	i := ht.root.Load()
	if i == nil {
		return
	}
	if ht.valEqual == nil {
		panic("called CompareAndDelete when value is not of comparable type")
	}
	hash := ht.keyHash(noescape(unsafe.Pointer(&key)), ht.seed)

	// Find a node with the key. n != nil if we found the node.
	i, hashShift, slot, n := ht.find(i, key, hash, nil, *new(V))
	if n == nil {
		if i != nil {
			i.mu.Unlock()
		}
		return false
	}

	// Try to delete the entry.
	e, deleted := n.entry().compareAndDelete(key, old, ht.valEqual)
	if !deleted {
		// Nothing was actually deleted, which means the node is no longer there.
		i.mu.Unlock()
		return false
	}
	if e != nil {
		// We didn't actually delete the whole entry, just one entry in the chain.
		// Nothing else to do, since the parent is definitely not empty.
		slot.Store(&e.node)
		i.mu.Unlock()
		return true
	}
	// Delete the entry.
	slot.Store(nil)

	// Check if the node is now empty (and isn't the root), and delete it if able.
	for i.parent != nil && i.empty() {
		if hashShift == 8*ptrSize {
			panic("internal/sync.HashTrieMap: ran out of hash bits while iterating")
		}
		hashShift += nChildrenLog2

		// Delete the current node in the parent.
		parent := i.parent
		parent.mu.Lock()
		i.dead.Store(true)
		parent.children[(hash>>hashShift)&nChildrenMask].Store(nil)
		i.mu.Unlock()
		i = parent
	}
	i.mu.Unlock()
	return true
}

// find searches the tree for a node that contains key (hash must be the hash of key).
// If valEqual != nil, then it will also enforce that the values are equal as well.
//
// Returns a non-nil node, which will always be an entry, if found.
//
// If i != nil then i.mu is locked, and it is the caller's responsibility to unlock it.
func (ht *HashTrieMap[K, V]) find(i0 *indirect[K, V], key K, hash uintptr, valEqual equalFunc, value V) (i *indirect[K, V], hashShift uint, slot *atomic.Pointer[node[K, V]], n *node[K, V]) {
	for i = i0; ; i = ht.root.Load() {
		// Find the key or return if it's not there.
		// i = ht.root.Load()
		hashShift = 8 * ptrSize
		found := false
		for hashShift != 0 {
			hashShift -= nChildrenLog2

			slot = &i.children[(hash>>hashShift)&nChildrenMask]
			n = slot.Load()
			if n == nil {
				// Nothing to compare with. Give up.
				i = nil
				return
			}
			if n.isEntry {
				// We found an entry. Check if it matches.
				if _, ok := n.entry().lookupWithValue(key, value, valEqual); !ok {
					// No match, comparison failed.
					i = nil
					n = nil
					return
				}
				// We've got a match. Prepare to perform an operation on the key.
				found = true
				break
			}
			i = n.indirect()
		}
		if !found {
			panic("internal/sync.HashTrieMap: ran out of hash bits while iterating")
		}

		// Grab the lock and double-check what we saw.
		i.mu.Lock()
		n = slot.Load()
		if !i.dead.Load() && (n == nil || n.isEntry) {
			// Either we've got a valid node or the node is now nil under the lock.
			// In either case, we're done here.
			return
		}
		// We have to start over.
		i.mu.Unlock()
	}
}

// All returns an iterator over each key and value present in the map.
//
// The iterator does not necessarily correspond to any consistent snapshot of the
// HashTrieMap's contents: no key will be visited more than once, but if the value
// for any key is stored or deleted concurrently (including by yield), the iterator
// may reflect any mapping for that key from any point during iteration. The iterator
// does not block other methods on the receiver; even yield itself may call any
// method on the HashTrieMap.
func (ht *HashTrieMap[K, V]) All() func(yield func(K, V) bool) {
	return ht.Range
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// This exists for compatibility with sync.Map; All should be preferred.
// It provides the same guarantees as sync.Map, and All.
func (ht *HashTrieMap[K, V]) Range(yield func(K, V) bool) {
	if i := ht.root.Load(); i != nil {
		ht.iter(i, yield)
	}
}

func (ht *HashTrieMap[K, V]) iter(i *indirect[K, V], yield func(key K, value V) bool) bool {
	for j := range i.children {
		n := i.children[j].Load()
		if n == nil {
			continue
		}
		if !n.isEntry {
			if !ht.iter(n.indirect(), yield) {
				return false
			}
			continue
		}
		e := n.entry()
		for e != nil {
			if !yield(e.key, e.value) {
				return false
			}
			e = e.overflow.Load()
		}
	}
	return true
}

// Clear deletes all the entries, resulting in an empty HashTrieMap.
func (ht *HashTrieMap[K, V]) Clear() {
	if ht.root.Load() == nil {
		return
	}

	// It's sufficient to just drop the root on the floor, but the root
	// must always be non-nil.
	ht.root.Store(newIndirectNode[K, V](nil))
}

// Size returns the number of elements in the map
func (ht *HashTrieMap[K, V]) Size() (size int) {
	ht.Range(func(K, V) bool {
		size += 1
		return true
	})
	return
}

// IsZero checks if the map is empty
func (ht *HashTrieMap[K, V]) IsZero() (isZero bool) {
	isZero = true
	ht.Range(func(K, V) bool {
		isZero = false
		return false
	})
	return
}

// ToMap returns all key-value pairs as a standard map
func (ht *HashTrieMap[K, V]) ToMap() map[K]V {
	m := make(map[K]V)
	ht.Range(func(k K, v V) bool {
		m[k] = v
		return true
	})
	return m
}

// String Implement the formatting output interface for fmt.Print %v
func (ht *HashTrieMap[K, V]) String() string {
	return strings.Replace(fmt.Sprint(ht.ToMap()), "map[", "HashTrieMap[", 1)
}

// MarshalJSON JSON serialization
func (ht *HashTrieMap[K, V]) MarshalJSON() ([]byte, error) {
	return json.Marshal(ht.ToMap())
}

// UnmarshalJSON JSON deserialization
func (ht *HashTrieMap[K, V]) UnmarshalJSON(data []byte) error {
	var m map[K]V
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	for k, v := range m {
		ht.Store(k, v)
	}
	return nil
}

const (
	// 16 children. This seems to be the sweet spot for
	// load performance: any smaller and we lose out on
	// 50% or more in CPU performance. Any larger and the
	// returns are minuscule (~1% improvement for 32 children).
	nChildrenLog2 = 4
	nChildren     = 1 << nChildrenLog2
	nChildrenMask = nChildren - 1

	// ref: goarch.PtrSize
	ptrSize = 4 << (^uintptr(0) >> 63)
)

// indirect is an internal node in the hash-trie.
type indirect[K comparable, V any] struct {
	node[K, V]
	dead     atomic.Bool
	mu       sync.Mutex // Protects mutation to children and any children that are entry nodes.
	parent   *indirect[K, V]
	children [nChildren]atomic.Pointer[node[K, V]]
}

func newIndirectNode[K comparable, V any](parent *indirect[K, V]) *indirect[K, V] {
	return &indirect[K, V]{node: node[K, V]{isEntry: false}, parent: parent}
}

func (i *indirect[K, V]) empty() bool {
	i.mu.TryLock()
	nc := 0
	for j := range i.children {
		if i.children[j].Load() != nil {
			nc++
		}
	}
	return nc == 0
}

// entry is a leaf node in the hash-trie.
type entry[K comparable, V any] struct {
	node[K, V]
	overflow atomic.Pointer[entry[K, V]] // Overflow for hash collisions.
	hash     uintptr
	key      K
	value    V
}

func newEntryNode[K comparable, V any](hash uintptr, key K, value V) *entry[K, V] {
	return &entry[K, V]{
		node:  node[K, V]{isEntry: true},
		hash:  hash,
		key:   key,
		value: value,
	}
}

func (e *entry[K, V]) lookup(key K) (V, bool) {
	for e != nil {
		if e.key == key {
			return e.value, true
		}
		e = e.overflow.Load()
	}
	return *new(V), false
}

func (e *entry[K, V]) lookupWithValue(key K, value V, valEqual equalFunc) (V, bool) {
	for e != nil {
		if e.key == key && (valEqual == nil || valEqual(unsafe.Pointer(&e.value), noescape(unsafe.Pointer(&value)))) {
			return e.value, true
		}
		e = e.overflow.Load()
	}
	return *new(V), false
}

// swap replaces an entry in the overflow chain if keys compare equal. Returns the new entry chain,
// the old value, and whether or not anything was swapped.
//
// swap must be called under the mutex of the indirect node which e is a child of.
func (head *entry[K, V]) swap(hash uintptr, key K, new V) (*entry[K, V], V, bool) {
	if head.key == key {
		// Return the new head of the list.
		e := newEntryNode(hash, key, new)
		if chain := head.overflow.Load(); chain != nil {
			e.overflow.Store(chain)
		}
		return e, head.value, true
	}
	i := &head.overflow
	e := i.Load()
	for e != nil {
		if e.key == key {
			eNew := newEntryNode(hash, key, new)
			eNew.overflow.Store(e.overflow.Load())
			i.Store(eNew)
			return head, e.value, true
		}
		i = &e.overflow
		e = e.overflow.Load()
	}
	var zero V
	return head, zero, false
}

// compareAndSwap replaces an entry in the overflow chain if both the key and value compare
// equal. Returns the new entry chain and whether or not anything was swapped.
//
// compareAndSwap must be called under the mutex of the indirect node which e is a child of.
func (head *entry[K, V]) compareAndSwap(hash uintptr, key K, old, new V, valEqual equalFunc) (*entry[K, V], bool) {
	if head.key == key && valEqual(unsafe.Pointer(&head.value), noescape(unsafe.Pointer(&old))) {
		// Return the new head of the list.
		e := newEntryNode(hash, key, new)
		if chain := head.overflow.Load(); chain != nil {
			e.overflow.Store(chain)
		}
		return e, true
	}
	i := &head.overflow
	e := i.Load()
	for e != nil {
		if e.key == key && valEqual(unsafe.Pointer(&e.value), noescape(unsafe.Pointer(&old))) {
			eNew := newEntryNode(hash, key, new)
			eNew.overflow.Store(e.overflow.Load())
			i.Store(eNew)
			return head, true
		}
		i = &e.overflow
		e = e.overflow.Load()
	}
	return head, false
}

// loadAndDelete deletes an entry in the overflow chain by key. Returns the value for the key, the new
// entry chain and whether or not anything was loaded (and deleted).
//
// loadAndDelete must be called under the mutex of the indirect node which e is a child of.
func (head *entry[K, V]) loadAndDelete(key K) (V, *entry[K, V], bool) {
	if head.key == key {
		// Drop the head of the list.
		return head.value, head.overflow.Load(), true
	}
	i := &head.overflow
	e := i.Load()
	for e != nil {
		if e.key == key {
			i.Store(e.overflow.Load())
			return e.value, head, true
		}
		i = &e.overflow
		e = e.overflow.Load()
	}
	return *new(V), head, false
}

// compareAndDelete deletes an entry in the overflow chain if both the key and value compare
// equal. Returns the new entry chain and whether or not anything was deleted.
//
// compareAndDelete must be called under the mutex of the indirect node which e is a child of.
func (head *entry[K, V]) compareAndDelete(key K, value V, valEqual equalFunc) (*entry[K, V], bool) {
	if head.key == key && valEqual(unsafe.Pointer(&head.value), noescape(unsafe.Pointer(&value))) {
		// Drop the head of the list.
		return head.overflow.Load(), true
	}
	i := &head.overflow
	e := i.Load()
	for e != nil {
		if e.key == key && valEqual(unsafe.Pointer(&e.value), noescape(unsafe.Pointer(&value))) {
			i.Store(e.overflow.Load())
			return head, true
		}
		i = &e.overflow
		e = e.overflow.Load()
	}
	return head, false
}

// node is the header for a node. It's polymorphic and
// is actually either an entry or an indirect.
type node[K comparable, V any] struct {
	isEntry bool
}

func (n *node[K, V]) entry() *entry[K, V] {
	if !n.isEntry {
		panic("called entry on non-entry node")
	}
	return (*entry[K, V])(unsafe.Pointer(n))
}

func (n *node[K, V]) indirect() *indirect[K, V] {
	if n.isEntry {
		panic("called indirect on entry node")
	}
	return (*indirect[K, V])(unsafe.Pointer(n))
}
