package pb

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestHashTrieMapStructSize(t *testing.T) {
	size := unsafe.Sizeof(HashTrieMap[string, int]{})
	t.Log("HashTrieMap[string,int] size:", size)
	structType := reflect.TypeOf(HashTrieMap[string, int]{})
	t.Logf("Struct: %s", structType.Name())

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name
		fieldType := field.Type
		fieldOffset := field.Offset
		fieldSize := fieldType.Size()

		t.Logf("Field: %-10s Type: %-10s Offset: %d Size: %d bytes\n",
			fieldName, fieldType, fieldOffset, fieldSize)
	}

	size = unsafe.Sizeof(indirect[string, int]{})
	t.Log("indirect[string,int] size:", size)
	structType = reflect.TypeOf(indirect[string, int]{})
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name
		fieldType := field.Type
		fieldOffset := field.Offset
		fieldSize := fieldType.Size()

		t.Logf("Field: %-10s Type: %-10s Offset: %d Size: %d bytes\n",
			fieldName, fieldType, fieldOffset, fieldSize)
	}

	size = unsafe.Sizeof(entry[string, int]{})
	t.Log("entry[string,int] size:", size)
	structType = reflect.TypeOf(entry[string, int]{})
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name
		fieldType := field.Type
		fieldOffset := field.Offset
		fieldSize := fieldType.Size()

		t.Logf("Field: %-10s Type: %-10s Offset: %d Size: %d bytes\n",
			fieldName, fieldType, fieldOffset, fieldSize)
	}
}

func TestMyHashTrieMap(t *testing.T) {
	// var a *SyncMap[int, int] = NewSyncMap[int, int]()
	var a, a1, a2, a3, a4 HashTrieMap[int, int]
	var str string
	t.Log(unsafe.Sizeof(HashTrieMap[string, int]{}))
	t.Log(unsafe.Sizeof(indirect[string, int]{}))
	t.Log(unsafe.Sizeof(entry[string, int]{}))
	// t.Log(unsafe.Sizeof(pool[string]{}))
	t.Log(unsafe.Sizeof(indirect[int, int]{}))
	t.Log(unsafe.Sizeof(indirect[int, string]{}))

	t.Log(unsafe.Sizeof(indirect[string, string]{}))
	t.Log(unsafe.Sizeof(str))

	t.Log(unsafe.Sizeof(node[int, string]{}))
	t.Log(unsafe.Sizeof(entry[int, int]{}))

	t.Log(unsafe.Sizeof(entry[string, string]{}))
	t.Log(unsafe.Sizeof(a))
	t.Log(unsafe.Sizeof(atomic.Pointer[int]{}))
	t.Log(&a)
	s, _ := json.Marshal(&a)
	t.Log(string(s))

	t.Log(a.Size())
	t.Log(a.IsZero())
	t.Log(a.Load(1))
	a.Delete(1)
	a.Clear()
	a.Range(func(i int, i2 int) bool {
		return true
	})

	t.Log(a.LoadAndDelete(1))
	t.Log(a.LoadOrStore(1, 1))
	a1.Store(1, 1)
	t.Log(&a)
	t.Log(a2.Swap(1, 1))
	t.Log(&a2)
	t.Log(a2.LoadAndDelete(1))
	t.Log(&a2)

	err := json.Unmarshal([]byte(`{"1":1}`), &a3)
	if err != nil {
		t.Fatal(err)
		return
	}
	s, err = json.Marshal(&a3)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(s))

	t.Log(&a4)

	var idm HashTrieMap[structKey, int]
	t.Log(idm.LoadOrStore(structKey{1, 1}, 1))
	t.Log(&idm)
	t.Log(idm.LoadAndDelete(structKey{1, 1}))
	t.Log(&idm)
}

// // NewBadHashTrieMap creates a new HashTrieMap for the provided key and value
// // but with an intentionally bad hash function.
func NewBadHashTrieMap[K, V comparable]() *HashTrieMap[K, V] {
	// Stub out the good hash function with a terrible one.
	// Everything should still work as expected.
	var m HashTrieMap[K, V]

	m.keyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
		return 0
	}

	return &m
}

//
// NewTruncHashTrieMap creates a new HashTrieMap for the provided key and value
// but with an intentionally bad hash function.

func NewTruncHashTrieMap[K, V comparable]() *HashTrieMap[K, V] {
	// Stub out the good hash function with a terrible one.
	// Everything should still work as expected.
	var m HashTrieMap[K, V]
	//hasher := defaultHasherUintptr[K]()
	//m.keyHash = func(k K, n uintptr) uintptr {
	//	return hasher(k, n) & ((uintptr(1) << 4) - 1)
	//}
	hasher, _ := defaultHasherUsingBuiltIn[K, V]()
	m.keyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
		return hasher(pointer, u) & ((uintptr(1) << 4) - 1)
	}
	return &m
}

func TestHashTrieMap(t *testing.T) {
	testHashTrieMap(t, func() *HashTrieMap[string, int] {
		return &HashTrieMap[string, int]{}
	})
}

func TestHashTrieMapBadHash(t *testing.T) {
	testHashTrieMap(t, func() *HashTrieMap[string, int] {
		return NewBadHashTrieMap[string, int]()
	})
}

func TestHashTrieMapTruncHash(t *testing.T) {
	testHashTrieMap(t, func() *HashTrieMap[string, int] {
		// Stub out the good hash function with a different terrible one
		// (truncated hash). Everything should still work as expected.
		// This is useful to test independently to catch issues with
		// near collisions, where only the last few bits of the hash differ.
		return NewTruncHashTrieMap[string, int]()
	})
}

func testHashTrieMap(t *testing.T, newMap func() *HashTrieMap[string, int]) {
	t.Run("LoadEmpty", func(t *testing.T) {
		m := newMap()

		for _, s := range testData {
			expectMissing(t, s, 0)(m.Load(s))
		}
	})
	t.Run("LoadOrStore", func(t *testing.T) {
		m := newMap()

		for i, s := range testData {
			expectMissing(t, s, 0)(m.Load(s))
			expectStored(t, s, i)(m.LoadOrStore(s, i))
			expectPresent(t, s, i)(m.Load(s))
			expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
		}
		for i, s := range testData {
			expectPresent(t, s, i)(m.Load(s))
			expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
		}
	})
	t.Run("All", func(t *testing.T) {
		m := newMap()

		testAll(t, m, testDataMap(testData[:]), func(_ string, _ int) bool {
			return true
		})
	})
	t.Run("Clear", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
			}
			m.Clear()
			for _, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
			}
		})
		t.Run("Concurrent", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for _, s := range testData {
						// Try a couple things to interfere with the clear.
						expectNotDeleted(t, s, math.MaxInt)(m.CompareAndDelete(s, math.MaxInt))
						m.CompareAndSwap(s, i, i+1) // May succeed or fail; we don't care.
					}
				}(i)
			}

			// Concurrently clear the map.
			runtime.Gosched()
			m.Clear()

			// Wait for workers to finish.
			wg.Wait()

			// It should all be empty now.
			for _, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
			}
		})
	})
	t.Run("CompareAndDelete", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			m := newMap()

			for range 3 {
				for i, s := range testData {
					expectMissing(t, s, 0)(m.Load(s))
					expectStored(t, s, i)(m.LoadOrStore(s, i))
					expectPresent(t, s, i)(m.Load(s))
					expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
				}
				for i, s := range testData {
					expectPresent(t, s, i)(m.Load(s))
					expectNotDeleted(t, s, math.MaxInt)(m.CompareAndDelete(s, math.MaxInt))
					expectDeleted(t, s, i)(m.CompareAndDelete(s, i))
					expectNotDeleted(t, s, i)(m.CompareAndDelete(s, i))
					expectMissing(t, s, 0)(m.Load(s))
				}
				for _, s := range testData {
					expectMissing(t, s, 0)(m.Load(s))
				}
			}
		})
		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
			}
			expectNotDeleted(t, testData[15], math.MaxInt)(m.CompareAndDelete(testData[15], math.MaxInt))
			expectDeleted(t, testData[15], 15)(m.CompareAndDelete(testData[15], 15))
			expectNotDeleted(t, testData[15], 15)(m.CompareAndDelete(testData[15], 15))
			for i, s := range testData {
				if i == 15 {
					expectMissing(t, s, 0)(m.Load(s))
				} else {
					expectPresent(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectNotDeleted(t, testData[i], math.MaxInt)(m.CompareAndDelete(testData[i], math.MaxInt))
				expectDeleted(t, testData[i], i)(m.CompareAndDelete(testData[i], i))
				expectNotDeleted(t, testData[i], i)(m.CompareAndDelete(testData[i], i))
			}
			for i, s := range testData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectMissing(t, s, 0)(m.Load(s))
				} else {
					expectPresent(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Iterate", func(t *testing.T) {
			m := newMap()

			testAll(t, m, testDataMap(testData[:]), func(s string, i int) bool {
				expectDeleted(t, s, i)(m.CompareAndDelete(s, i))
				return true
			})
			for _, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
			}
		})
		t.Run("ConcurrentUnsharedKeys", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
						expectStored(t, key, id)(m.LoadOrStore(key, id))
						expectPresent(t, key, id)(m.Load(key))
						expectLoaded(t, key, id)(m.LoadOrStore(key, 0))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id)(m.Load(key))
						expectDeleted(t, key, id)(m.CompareAndDelete(key, id))
						expectMissing(t, key, 0)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for i, s := range testData {
						expectNotDeleted(t, s, math.MaxInt)(m.CompareAndDelete(s, math.MaxInt))
						m.CompareAndDelete(s, i)
						expectMissing(t, s, 0)(m.Load(s))
					}
					for _, s := range testData {
						expectMissing(t, s, 0)(m.Load(s))
					}
				}(i)
			}
			wg.Wait()
		})
	})
	t.Run("CompareAndSwap", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
			}
			for j := range 3 {
				for i, s := range testData {
					expectPresent(t, s, i+j)(m.Load(s))
					expectNotSwapped(t, s, math.MaxInt, i+j+1)(m.CompareAndSwap(s, math.MaxInt, i+j+1))
					expectSwapped(t, s, i, i+j+1)(m.CompareAndSwap(s, i+j, i+j+1))
					expectNotSwapped(t, s, i+j, i+j+1)(m.CompareAndSwap(s, i+j, i+j+1))
					expectPresent(t, s, i+j+1)(m.Load(s))
				}
			}
			for i, s := range testData {
				expectPresent(t, s, i+3)(m.Load(s))
			}
		})
		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
			}
			expectNotSwapped(t, testData[15], math.MaxInt, 16)(m.CompareAndSwap(testData[15], math.MaxInt, 16))
			expectSwapped(t, testData[15], 15, 16)(m.CompareAndSwap(testData[15], 15, 16))
			expectNotSwapped(t, testData[15], 15, 16)(m.CompareAndSwap(testData[15], 15, 16))
			for i, s := range testData {
				if i == 15 {
					expectPresent(t, s, 16)(m.Load(s))
				} else {
					expectPresent(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectNotSwapped(t, testData[i], math.MaxInt, i+1)(m.CompareAndSwap(testData[i], math.MaxInt, i+1))
				expectSwapped(t, testData[i], i, i+1)(m.CompareAndSwap(testData[i], i, i+1))
				expectNotSwapped(t, testData[i], i, i+1)(m.CompareAndSwap(testData[i], i, i+1))
			}
			for i, s := range testData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectPresent(t, s, i+1)(m.Load(s))
				} else {
					expectPresent(t, s, i)(m.Load(s))
				}
			}
		})

		t.Run("ConcurrentUnsharedKeys", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
						expectStored(t, key, id)(m.LoadOrStore(key, id))
						expectPresent(t, key, id)(m.Load(key))
						expectLoaded(t, key, id)(m.LoadOrStore(key, 0))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id)(m.Load(key))
						expectSwapped(t, key, id, id+1)(m.CompareAndSwap(key, id, id+1))
						expectPresent(t, key, id+1)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id+1)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentUnsharedKeysWithDelete", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
						expectStored(t, key, id)(m.LoadOrStore(key, id))
						expectPresent(t, key, id)(m.Load(key))
						expectLoaded(t, key, id)(m.LoadOrStore(key, 0))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id)(m.Load(key))
						expectSwapped(t, key, id, id+1)(m.CompareAndSwap(key, id, id+1))
						expectPresent(t, key, id+1)(m.Load(key))
						expectDeleted(t, key, id+1)(m.CompareAndDelete(key, id+1))
						expectNotSwapped(t, key, id+1, id+2)(m.CompareAndSwap(key, id+1, id+2))
						expectNotDeleted(t, key, id+1)(m.CompareAndDelete(key, id+1))
						expectMissing(t, key, 0)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for i, s := range testData {
						expectNotSwapped(t, s, math.MaxInt, i+1)(m.CompareAndSwap(s, math.MaxInt, i+1))
						m.CompareAndSwap(s, i, i+1)
						expectPresent(t, s, i+1)(m.Load(s))
					}
					for i, s := range testData {
						expectPresent(t, s, i+1)(m.Load(s))
					}
				}(i)
			}
			wg.Wait()
		})
	})
	t.Run("Swap", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectNotLoadedFromSwap(t, s, i)(m.Swap(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoadedFromSwap(t, s, i, i)(m.Swap(s, i))
			}
			for j := range 3 {
				for i, s := range testData {
					expectPresent(t, s, i+j)(m.Load(s))
					expectLoadedFromSwap(t, s, i+j, i+j+1)(m.Swap(s, i+j+1))
					expectPresent(t, s, i+j+1)(m.Load(s))
				}
			}
			for i, s := range testData {
				expectLoadedFromSwap(t, s, i+3, i+3)(m.Swap(s, i+3))
			}
		})
		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectNotLoadedFromSwap(t, s, i)(m.Swap(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoadedFromSwap(t, s, i, i)(m.Swap(s, i))
			}
			expectLoadedFromSwap(t, testData[15], 15, 16)(m.Swap(testData[15], 16))
			for i, s := range testData {
				if i == 15 {
					expectPresent(t, s, 16)(m.Load(s))
				} else {
					expectPresent(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectNotLoadedFromSwap(t, s, i)(m.Swap(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoadedFromSwap(t, s, i, i)(m.Swap(s, i))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectLoadedFromSwap(t, testData[i], i, i+1)(m.Swap(testData[i], i+1))
			}
			for i, s := range testData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectPresent(t, s, i+1)(m.Load(s))
				} else {
					expectPresent(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("ConcurrentUnsharedKeys", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
						expectNotLoadedFromSwap(t, key, id)(m.Swap(key, id))
						expectPresent(t, key, id)(m.Load(key))
						expectLoadedFromSwap(t, key, id, id)(m.Swap(key, id))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id)(m.Load(key))
						expectLoadedFromSwap(t, key, id, id+1)(m.Swap(key, id+1))
						expectPresent(t, key, id+1)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id+1)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentUnsharedKeysWithDelete", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
						expectNotLoadedFromSwap(t, key, id)(m.Swap(key, id))
						expectPresent(t, key, id)(m.Load(key))
						expectLoadedFromSwap(t, key, id, id)(m.Swap(key, id))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id)(m.Load(key))
						expectLoadedFromSwap(t, key, id, id+1)(m.Swap(key, id+1))
						expectPresent(t, key, id+1)(m.Load(key))
						expectDeleted(t, key, id+1)(m.CompareAndDelete(key, id+1))
						expectNotLoadedFromSwap(t, key, id+2)(m.Swap(key, id+2))
						expectPresent(t, key, id+2)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id+2)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for i, s := range testData {
						m.Swap(s, i+1)
						expectPresent(t, s, i+1)(m.Load(s))
					}
					for i, s := range testData {
						expectPresent(t, s, i+1)(m.Load(s))
					}
				}(i)
			}
			wg.Wait()
		})
	})
	t.Run("LoadAndDelete", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			m := newMap()

			for range 3 {
				for i, s := range testData {
					expectMissing(t, s, 0)(m.Load(s))
					expectStored(t, s, i)(m.LoadOrStore(s, i))
					expectPresent(t, s, i)(m.Load(s))
					expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
				}
				for i, s := range testData {
					expectPresent(t, s, i)(m.Load(s))
					expectLoadedFromDelete(t, s, i)(m.LoadAndDelete(s))
					expectMissing(t, s, 0)(m.Load(s))
					expectNotLoadedFromDelete(t, s, 0)(m.LoadAndDelete(s))
				}
				for _, s := range testData {
					expectMissing(t, s, 0)(m.Load(s))
				}
			}
		})
		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
			}
			expectPresent(t, testData[15], 15)(m.Load(testData[15]))
			expectLoadedFromDelete(t, testData[15], 15)(m.LoadAndDelete(testData[15]))
			expectMissing(t, testData[15], 0)(m.Load(testData[15]))
			expectNotLoadedFromDelete(t, testData[15], 0)(m.LoadAndDelete(testData[15]))
			for i, s := range testData {
				if i == 15 {
					expectMissing(t, s, 0)(m.Load(s))
				} else {
					expectPresent(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
				expectPresent(t, s, i)(m.Load(s))
				expectLoaded(t, s, i)(m.LoadOrStore(s, 0))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectPresent(t, testData[i], i)(m.Load(testData[i]))
				expectLoadedFromDelete(t, testData[i], i)(m.LoadAndDelete(testData[i]))
				expectMissing(t, testData[i], 0)(m.Load(testData[i]))
				expectNotLoadedFromDelete(t, testData[i], 0)(m.LoadAndDelete(testData[i]))
			}
			for i, s := range testData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectMissing(t, s, 0)(m.Load(s))
				} else {
					expectPresent(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Iterate", func(t *testing.T) {
			m := newMap()

			testAll(t, m, testDataMap(testData[:]), func(s string, i int) bool {
				expectLoadedFromDelete(t, s, i)(m.LoadAndDelete(s))
				return true
			})
			for _, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
			}
		})
		t.Run("ConcurrentUnsharedKeys", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
						expectStored(t, key, id)(m.LoadOrStore(key, id))
						expectPresent(t, key, id)(m.Load(key))
						expectLoaded(t, key, id)(m.LoadOrStore(key, 0))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresent(t, key, id)(m.Load(key))
						expectLoadedFromDelete(t, key, id)(m.LoadAndDelete(key))
						expectMissing(t, key, 0)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissing(t, s, 0)(m.Load(s))
				expectStored(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for _, s := range testData {
						m.LoadAndDelete(s)
						expectMissing(t, s, 0)(m.Load(s))
					}
					for _, s := range testData {
						expectMissing(t, s, 0)(m.Load(s))
					}
				}(i)
			}
			wg.Wait()
		})
	})
}

func testAll[K, V comparable](t *testing.T, m *HashTrieMap[K, V], testData map[K]V, yield func(K, V) bool) {
	for k, v := range testData {
		expectStored(t, k, v)(m.LoadOrStore(k, v))
	}
	visited := make(map[K]int)
	m.All()(func(key K, got V) bool {
		want, ok := testData[key]
		if !ok {
			t.Errorf("unexpected key %v in map", key)
			return false
		}
		if got != want {
			t.Errorf("expected key %v to have value %v, got %v", key, want, got)
			return false
		}
		visited[key]++
		return yield(key, got)
	})
	for key, n := range visited {
		if n > 1 {
			t.Errorf("visited key %v more than once", key)
		}
	}
}

func expectPresent[K, V comparable](t *testing.T, key K, want V) func(got V, ok bool) {
	t.Helper()
	return func(got V, ok bool) {
		t.Helper()

		if !ok {
			t.Errorf("expected key %v to be present in map", key)
		}
		if ok && got != want {
			t.Errorf("expected key %v to have value %v, got %v", key, want, got)
		}
	}
}

func expectMissing[K, V comparable](t *testing.T, key K, want V) func(got V, ok bool) {
	t.Helper()
	if want != *new(V) {
		// This is awkward, but the want argument is necessary to smooth over type inference.
		// Just make sure the want argument always looks the same.
		panic("expectMissing must always have a zero value variable")
	}
	return func(got V, ok bool) {
		t.Helper()

		if ok {
			t.Errorf("expected key %v to be missing from map, got value %v", key, got)
		}
		if !ok && got != want {
			t.Errorf("expected missing key %v to be paired with the zero value; got %v", key, got)
		}
	}
}

func expectLoaded[K, V comparable](t *testing.T, key K, want V) func(got V, loaded bool) {
	t.Helper()
	return func(got V, loaded bool) {
		t.Helper()

		if !loaded {
			t.Errorf("expected key %v to have been loaded, not stored", key)
		}
		if got != want {
			t.Errorf("expected key %v to have value %v, got %v", key, want, got)
		}
	}
}

func expectStored[K, V comparable](t *testing.T, key K, want V) func(got V, loaded bool) {
	t.Helper()
	return func(got V, loaded bool) {
		t.Helper()

		if loaded {
			t.Errorf("expected inserted key %v to have been stored, not loaded", key)
		}
		if got != want {
			t.Errorf("expected inserted key %v to have value %v, got %v", key, want, got)
		}
	}
}

func expectDeleted[K, V comparable](t *testing.T, key K, old V) func(deleted bool) {
	t.Helper()
	return func(deleted bool) {
		t.Helper()

		if !deleted {
			t.Errorf("expected key %v with value %v to be in map and deleted", key, old)
		}
	}
}

func expectNotDeleted[K, V comparable](t *testing.T, key K, old V) func(deleted bool) {
	t.Helper()
	return func(deleted bool) {
		t.Helper()

		if deleted {
			t.Errorf("expected key %v with value %v to not be in map and thus not deleted", key, old)
		}
	}
}

func expectSwapped[K, V comparable](t *testing.T, key K, old, new V) func(swapped bool) {
	t.Helper()
	return func(swapped bool) {
		t.Helper()

		if !swapped {
			t.Errorf("expected key %v with value %v to be in map and swapped for %v", key, old, new)
		}
	}
}

func expectNotSwapped[K, V comparable](t *testing.T, key K, old, new V) func(swapped bool) {
	t.Helper()
	return func(swapped bool) {
		t.Helper()

		if swapped {
			t.Errorf("expected key %v with value %v to not be in map or not swapped for %v", key, old, new)
		}
	}
}

func expectLoadedFromSwap[K, V comparable](t *testing.T, key K, want, new V) func(got V, loaded bool) {
	t.Helper()
	return func(got V, loaded bool) {
		t.Helper()

		if !loaded {
			t.Errorf("expected key %v to be in map and for %v to have been swapped for %v", key, want, new)
		} else if want != got {
			t.Errorf("key %v had its value %v swapped for %v, but expected it to have value %v", key, got, new, want)
		}
	}
}

func expectNotLoadedFromSwap[K, V comparable](t *testing.T, key K, new V) func(old V, loaded bool) {
	t.Helper()
	return func(old V, loaded bool) {
		t.Helper()

		if loaded {
			t.Errorf("expected key %v to not be in map, but found value %v for it", key, old)
		}
	}
}

func expectLoadedFromDelete[K, V comparable](t *testing.T, key K, want V) func(got V, loaded bool) {
	t.Helper()
	return func(got V, loaded bool) {
		t.Helper()

		if !loaded {
			t.Errorf("expected key %v to be in map to be deleted", key)
		} else if want != got {
			t.Errorf("key %v was deleted with value %v, but expected it to have value %v", key, got, want)
		}
	}
}

func expectNotLoadedFromDelete[K, V comparable](t *testing.T, key K, _ V) func(old V, loaded bool) {
	t.Helper()
	return func(old V, loaded bool) {
		t.Helper()

		if loaded {
			t.Errorf("expected key %v to not be in map, but found value %v for it", key, old)
		}
	}
}

func testDataMap(data []string) map[string]int {
	m := make(map[string]int)
	for i, s := range data {
		m[s] = i
	}
	return m
}

//
//// TestConcurrentCache tests HashTrieMap in a scenario where it is used as
//// the basis of a memory-efficient concurrent cache. We're specifically
//// looking to make sure that CompareAndSwap and CompareAndDelete are
//// atomic with respect to one another. When competing for the same
//// key-value pair, they must not both succeed.
////
//// This test is a regression test for issue #70970.
//func TestConcurrentCache(t *testing.T) {
//	type dummy [32]byte
//
//	var m HashTrieMap[int, weak.Pointer[dummy]]
//
//	type cleanupArg struct {
//		key   int
//		value weak.Pointer[dummy]
//	}
//	cleanup := func(arg cleanupArg) {
//		m.CompareAndDelete(arg.key, arg.value)
//	}
//	get := func(m *HashTrieMap[int, weak.Pointer[dummy]], key int) *dummy {
//		nv := new(dummy)
//		nw := weak.Make(nv)
//		for {
//			w, loaded := m.LoadOrStore(key, nw)
//			if !loaded {
//				runtime.AddCleanup(nv, cleanup, cleanupArg{key, nw})
//				return nv
//			}
//			if v := w.Value(); v != nil {
//				return v
//			}
//
//			// Weak pointer was reclaimed, try to replace it with nw.
//			if m.CompareAndSwap(key, w, nw) {
//				runtime.AddCleanup(nv, cleanup, cleanupArg{key, nw})
//				return nv
//			}
//		}
//	}
//
//	const N = 100_000
//	const P = 5_000
//
//	var wg sync.WaitGroup
//	wg.Add(N)
//	for i := range N {
//		go func() {
//			defer wg.Done()
//			a := get(&m, i%P)
//			b := get(&m, i%P)
//			if a != b {
//				t.Errorf("consecutive cache reads returned different values: a != b (%p vs %p)\n", a, b)
//			}
//		}()
//	}
//	wg.Wait()
//}

// TestHashTrieMapCompareAndSwap tests the CompareAndSwap function}

// TestHashTrieMapLoadAndDelete tests LoadAndDelete with various scenarios
func TestHashTrieMapLoadAndDelete(t *testing.T) {
	t.Run("LoadAndDeleteExisting", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("key1", 10)
		m.Store("key2", 20)

		value, loaded := m.LoadAndDelete("key1")
		if !loaded {
			t.Fatal("Expected key1 to be loaded")
		}
		if value != 10 {
			t.Fatalf("Expected value 10, got %d", value)
		}

		// Verify key1 is deleted
		_, found := m.Load("key1")
		if found {
			t.Fatal("Expected key1 to be deleted")
		}

		// Verify key2 still exists
		value2, found := m.Load("key2")
		if !found || value2 != 20 {
			t.Fatal("Expected key2 to still exist with value 20")
		}
	})

	t.Run("LoadAndDeleteNonExisting", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("key1", 10)

		value, loaded := m.LoadAndDelete("nonexistent")
		if loaded {
			t.Fatal("Expected nonexistent key to not be loaded")
		}
		if value != 0 {
			t.Fatalf("Expected zero value, got %d", value)
		}
	})

	t.Run("LoadAndDeleteEmptyMap", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}

		value, loaded := m.LoadAndDelete("key")
		if loaded {
			t.Fatal("Expected empty map to not load anything")
		}
		if value != 0 {
			t.Fatalf("Expected zero value, got %d", value)
		}
	})

	t.Run("LoadAndDeleteOverflowChain", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		// Create multiple entries that might hash to same bucket
		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("key%d", i), i*10)
		}

		// Delete from middle of potential chain
		value, loaded := m.LoadAndDelete("key5")
		if !loaded || value != 50 {
			t.Fatalf("Expected to load and delete key5 with value 50, got loaded=%v, value=%d", loaded, value)
		}

		// Verify other keys still exist
		for i := 0; i < 10; i++ {
			if i == 5 {
				continue
			}
			key := fmt.Sprintf("key%d", i)
			value, found := m.Load(key)
			if !found || value != i*10 {
				t.Fatalf("Expected %s to exist with value %d, got found=%v, value=%d", key, i*10, found, value)
			}
		}
	})
}

// TestHashTrieMapSwap tests Swap with various scenarios
func TestHashTrieMapSwap(t *testing.T) {
	t.Run("SwapExisting", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("key1", 10)

		oldValue, loaded := m.Swap("key1", 20)
		if !loaded {
			t.Fatal("Expected key1 to be loaded")
		}
		if oldValue != 10 {
			t.Fatalf("Expected old value 10, got %d", oldValue)
		}

		// Verify new value
		newValue, found := m.Load("key1")
		if !found || newValue != 20 {
			t.Fatalf("Expected new value 20, got found=%v, value=%d", found, newValue)
		}
	})

	t.Run("SwapNonExisting", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}

		oldValue, loaded := m.Swap("newkey", 30)
		if loaded {
			t.Fatal("Expected newkey to not be loaded")
		}
		if oldValue != 0 {
			t.Fatalf("Expected zero old value, got %d", oldValue)
		}

		// Verify key was created with new value (Swap creates if not exists)
		value, found := m.Load("newkey")
		if !found || value != 30 {
			t.Fatalf("Expected newkey to be created with value 30, got found=%v, value=%d", found, value)
		}
	})

	t.Run("SwapOverflowChain", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		// Create multiple entries
		for i := 0; i < 5; i++ {
			m.Store(fmt.Sprintf("key%d", i), i*10)
		}

		// Swap middle entry
		oldValue, loaded := m.Swap("key2", 999)
		if !loaded || oldValue != 20 {
			t.Fatalf("Expected to swap key2 with old value 20, got loaded=%v, oldValue=%d", loaded, oldValue)
		}

		// Verify swap worked
		newValue, found := m.Load("key2")
		if !found || newValue != 999 {
			t.Fatalf("Expected key2 to have new value 999, got found=%v, value=%d", found, newValue)
		}
	})
}

// TestHashTrieMapOverflowChainOperations tests operations on overflow chains
func TestHashTrieMapOverflowChainOperations(t *testing.T) {
	t.Run("LoadAndDeleteOverflowChain", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		// Create entries that will likely hash to same bucket
		keys := []string{"a", "aa", "aaa", "aaaa", "aaaaa"}
		for i, key := range keys {
			m.Store(key, i*10)
		}

		// Test loadAndDelete on head of chain
		value, loaded := m.LoadAndDelete("a")
		if !loaded || value != 0 {
			t.Fatalf("Expected to load and delete 'a' with value 0, got loaded=%v, value=%d", loaded, value)
		}

		// Test loadAndDelete on middle of chain
		value, loaded = m.LoadAndDelete("aaa")
		if !loaded || value != 20 {
			t.Fatalf("Expected to load and delete 'aaa' with value 20, got loaded=%v, value=%d", loaded, value)
		}

		// Test loadAndDelete on non-existing key
		value, loaded = m.LoadAndDelete("nonexist")
		if loaded || value != 0 {
			t.Fatalf("Expected not to load non-existing key, got loaded=%v, value=%d", loaded, value)
		}
	})

	t.Run("CompareAndSwapOverflowChain", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		keys := []string{"b", "bb", "bbb"}
		for i, key := range keys {
			m.Store(key, i*5)
		}

		// Test compareAndSwap on head with correct old value
		swapped := m.CompareAndSwap("b", 0, 100)
		if !swapped {
			t.Fatal("Expected to swap 'b' from 0 to 100")
		}

		// Test compareAndSwap on middle with wrong old value
		swapped = m.CompareAndSwap("bb", 999, 200)
		if swapped {
			t.Fatal("Expected not to swap 'bb' with wrong old value")
		}

		// Test compareAndSwap on middle with correct old value
		swapped = m.CompareAndSwap("bb", 5, 200)
		if !swapped {
			t.Fatal("Expected to swap 'bb' from 5 to 200")
		}

		// Verify values
		if val, _ := m.Load("b"); val != 100 {
			t.Fatalf("Expected 'b' to be 100, got %d", val)
		}
		if val, _ := m.Load("bb"); val != 200 {
			t.Fatalf("Expected 'bb' to be 200, got %d", val)
		}
	})

	t.Run("CompareAndDeleteOverflowChain", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		keys := []string{"c", "cc", "ccc"}
		for i, key := range keys {
			m.Store(key, i*3)
		}

		// Test compareAndDelete on head with wrong value
		deleted := m.CompareAndDelete("c", 999)
		if deleted {
			t.Fatal("Expected not to delete 'c' with wrong value")
		}

		// Test compareAndDelete on head with correct value
		deleted = m.CompareAndDelete("c", 0)
		if !deleted {
			t.Fatal("Expected to delete 'c' with correct value")
		}

		// Test compareAndDelete on middle with correct value
		deleted = m.CompareAndDelete("cc", 3)
		if !deleted {
			t.Fatal("Expected to delete 'cc' with correct value")
		}

		// Verify deletions
		if _, found := m.Load("c"); found {
			t.Fatal("Expected 'c' to be deleted")
		}
		if _, found := m.Load("cc"); found {
			t.Fatal("Expected 'cc' to be deleted")
		}
		if val, found := m.Load("ccc"); !found || val != 6 {
			t.Fatalf("Expected 'ccc' to remain with value 6, got found=%v, val=%d", found, val)
		}
	})

	t.Run("SwapOverflowChainEdgeCases", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		keys := []string{"d", "dd", "ddd"}
		for i, key := range keys {
			m.Store(key, i*7)
		}

		// Test swap on tail of chain
		oldVal, loaded := m.Swap("ddd", 999)
		if !loaded || oldVal != 14 {
			t.Fatalf("Expected to swap 'ddd' from 14, got loaded=%v, oldVal=%d", loaded, oldVal)
		}

		// Test swap on non-existing key (should create new entry)
		oldVal, loaded = m.Swap("dddd", 888)
		if loaded || oldVal != 0 {
			t.Fatalf("Expected not to load non-existing 'dddd', got loaded=%v, oldVal=%d", loaded, oldVal)
		}

		// Verify new entry was created
		if val, found := m.Load("dddd"); !found || val != 888 {
			t.Fatalf("Expected 'dddd' to be created with value 888, got found=%v, val=%d", found, val)
		}
	})
}

// TestHashTrieMapInternalFunctionCoverage tests internal functions for better coverage
func TestHashTrieMapInternalFunctionCoverage(t *testing.T) {
	t.Run("LoadAndDeleteEmptyChain", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		// Test loadAndDelete on empty map
		value, loaded := m.LoadAndDelete("nonexistent")
		if loaded || value != 0 {
			t.Fatalf("Expected not to load from empty map, got loaded=%v, value=%d", loaded, value)
		}
	})

	t.Run("LoadAndDeleteSingleEntry", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("single", 42)

		// Test loadAndDelete on single entry (head of chain)
		value, loaded := m.LoadAndDelete("single")
		if !loaded || value != 42 {
			t.Fatalf("Expected to load and delete single entry, got loaded=%v, value=%d", loaded, value)
		}

		// Verify it's gone
		if _, found := m.Load("single"); found {
			t.Fatal("Expected entry to be deleted")
		}
	})

	t.Run("LoadAndDeleteTailOfChain", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		// Create a chain by using keys that likely hash to same bucket
		keys := []string{"x", "xx", "xxx", "xxxx"}
		for i, key := range keys {
			m.Store(key, i*11)
		}

		// Delete from tail of chain
		value, loaded := m.LoadAndDelete("xxxx")
		if !loaded || value != 33 {
			t.Fatalf("Expected to load and delete tail entry, got loaded=%v, value=%d", loaded, value)
		}

		// Verify others still exist
		for i, key := range keys[:3] {
			if val, found := m.Load(key); !found || val != i*11 {
				t.Fatalf("Expected key %s to still exist with value %d, got found=%v, val=%d", key, i*11, found, val)
			}
		}
	})

	t.Run("CompareAndSwapNonMatchingValue", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("test", 100)

		// Try to swap with wrong old value
		swapped := m.CompareAndSwap("test", 999, 200)
		if swapped {
			t.Fatal("Expected not to swap with wrong old value")
		}

		// Verify original value unchanged
		if val, _ := m.Load("test"); val != 100 {
			t.Fatalf("Expected original value 100, got %d", val)
		}
	})

	t.Run("CompareAndDeleteNonMatchingValue", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("test", 100)

		// Try to delete with wrong value
		deleted := m.CompareAndDelete("test", 999)
		if deleted {
			t.Fatal("Expected not to delete with wrong value")
		}

		// Verify entry still exists
		if val, found := m.Load("test"); !found || val != 100 {
			t.Fatalf("Expected entry to still exist with value 100, got found=%v, val=%d", found, val)
		}
	})

	t.Run("SwapNonExistentInChain", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		// Create a chain
		keys := []string{"y", "yy", "yyy"}
		for i, key := range keys {
			m.Store(key, i*13)
		}

		// Try to swap non-existent key in same hash bucket
		oldVal, loaded := m.Swap("yyyy", 999)
		if loaded || oldVal != 0 {
			t.Fatalf("Expected not to load non-existent key, got loaded=%v, oldVal=%d", loaded, oldVal)
		}

		// Verify new entry was added
		if val, found := m.Load("yyyy"); !found || val != 999 {
			t.Fatalf("Expected new entry to be created, got found=%v, val=%d", found, val)
		}
	})

	t.Run("LoadOrStoreFnWithExistingKey", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("existing", 50)

		// LoadOrStoreFn should return existing value without calling fn
		fnCalled := false
		value, loaded := m.LoadOrStoreFn("existing", func() int {
			fnCalled = true
			return 999
		})

		if !loaded || value != 50 {
			t.Fatalf("Expected to load existing value 50, got loaded=%v, value=%d", loaded, value)
		}
		if fnCalled {
			t.Fatal("Expected function not to be called for existing key")
		}
	})

	t.Run("LoadOrStoreFnWithNewKey", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}

		// LoadOrStoreFn should call fn and store result
		fnCalled := false
		value, loaded := m.LoadOrStoreFn("new", func() int {
			fnCalled = true
			return 777
		})

		if loaded || value != 777 {
			t.Fatalf("Expected to store new value 777, got loaded=%v, value=%d", loaded, value)
		}
		if !fnCalled {
			t.Fatal("Expected function to be called for new key")
		}

		// Verify value was stored
		if val, found := m.Load("new"); !found || val != 777 {
			t.Fatalf("Expected stored value 777, got found=%v, val=%d", found, val)
		}
	})
}

// TestHashTrieMapNodeCleanupCoverage tests node cleanup and edge cases
func TestHashTrieMapNodeCleanupCoverage(t *testing.T) {
	t.Run("LoadAndDeleteWithNodeCleanup", func(t *testing.T) {
		m := &HashTrieMap[int, string]{}

		// Add many entries to force tree expansion
		for i := 0; i < 100; i++ {
			m.Store(i, fmt.Sprintf("value%d", i))
		}

		// Delete entries to trigger node cleanup
		for i := 0; i < 50; i++ {
			value, loaded := m.LoadAndDelete(i)
			if !loaded || value != fmt.Sprintf("value%d", i) {
				t.Fatalf("Expected to delete entry %d, got loaded=%v, value=%s", i, loaded, value)
			}
		}

		// Verify remaining entries
		for i := 50; i < 100; i++ {
			if val, found := m.Load(i); !found || val != fmt.Sprintf("value%d", i) {
				t.Fatalf("Expected entry %d to remain, got found=%v, val=%s", i, found, val)
			}
		}
	})

	t.Run("LoadAndDeleteEmptyAfterDeletion", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("temp", 123)

		// Delete the only entry
		value, loaded := m.LoadAndDelete("temp")
		if !loaded || value != 123 {
			t.Fatalf("Expected to delete temp entry, got loaded=%v, value=%d", loaded, value)
		}

		// Try to delete again (should not find anything)
		value, loaded = m.LoadAndDelete("temp")
		if loaded || value != 0 {
			t.Fatalf("Expected not to find deleted entry, got loaded=%v, value=%d", loaded, value)
		}
	})

	t.Run("LoadAndDeleteNonExistentAfterExpansion", func(t *testing.T) {
		m := &HashTrieMap[int, string]{}

		// Force expansion by adding many entries
		for i := 0; i < 64; i++ {
			m.Store(i, fmt.Sprintf("val%d", i))
		}

		// Try to delete non-existent key
		value, loaded := m.LoadAndDelete(999)
		if loaded || value != "" {
			t.Fatalf("Expected not to find non-existent key, got loaded=%v, value=%s", loaded, value)
		}
	})

	t.Run("SwapInExpandedTree", func(t *testing.T) {
		m := &HashTrieMap[int, string]{}

		// Force tree expansion
		for i := 0; i < 32; i++ {
			m.Store(i, fmt.Sprintf("initial%d", i))
		}

		// Test swap in expanded tree
		oldVal, loaded := m.Swap(15, "swapped15")
		if !loaded || oldVal != "initial15" {
			t.Fatalf("Expected to swap key 15, got loaded=%v, oldVal=%s", loaded, oldVal)
		}

		// Verify swap worked
		if val, found := m.Load(15); !found || val != "swapped15" {
			t.Fatalf("Expected swapped value, got found=%v, val=%s", found, val)
		}
	})

	t.Run("CompareAndSwapInExpandedTree", func(t *testing.T) {
		m := &HashTrieMap[int, string]{}

		// Force tree expansion
		for i := 0; i < 32; i++ {
			m.Store(i, fmt.Sprintf("orig%d", i))
		}

		// Test compareAndSwap with correct old value
		swapped := m.CompareAndSwap(20, "orig20", "new20")
		if !swapped {
			t.Fatal("Expected to swap with correct old value")
		}

		// Test compareAndSwap with wrong old value
		swapped = m.CompareAndSwap(21, "wrong", "new21")
		if swapped {
			t.Fatal("Expected not to swap with wrong old value")
		}
	})

	t.Run("CompareAndDeleteInExpandedTree", func(t *testing.T) {
		m := &HashTrieMap[int, string]{}

		// Force tree expansion
		for i := 0; i < 32; i++ {
			m.Store(i, fmt.Sprintf("data%d", i))
		}

		// Test compareAndDelete with correct value
		deleted := m.CompareAndDelete(25, "data25")
		if !deleted {
			t.Fatal("Expected to delete with correct value")
		}

		// Verify deletion
		if _, found := m.Load(25); found {
			t.Fatal("Expected entry to be deleted")
		}

		// Test compareAndDelete with wrong value
		deleted = m.CompareAndDelete(26, "wrong")
		if deleted {
			t.Fatal("Expected not to delete with wrong value")
		}
	})

	t.Run("LoadOrStoreFnInExpandedTree", func(t *testing.T) {
		m := &HashTrieMap[int, string]{}

		// Force tree expansion
		for i := 0; i < 32; i++ {
			m.Store(i, fmt.Sprintf("existing%d", i))
		}

		// Test LoadOrStoreFn with existing key
		fnCalled := false
		value, loaded := m.LoadOrStoreFn(10, func() string {
			fnCalled = true
			return "should_not_be_called"
		})

		if !loaded || value != "existing10" {
			t.Fatalf("Expected to load existing value, got loaded=%v, value=%s", loaded, value)
		}
		if fnCalled {
			t.Fatal("Expected function not to be called for existing key")
		}

		// Test LoadOrStoreFn with new key
		fnCalled = false
		value, loaded = m.LoadOrStoreFn(100, func() string {
			fnCalled = true
			return "new_value"
		})

		if loaded || value != "new_value" {
			t.Fatalf("Expected to store new value, got loaded=%v, value=%s", loaded, value)
		}
		if !fnCalled {
			t.Fatal("Expected function to be called for new key")
		}
	})
}

// TestHashTrieMapIsZero tests IsZero edge cases
func TestHashTrieMapIsZero(t *testing.T) {
	t.Run("UninitializedMap", func(t *testing.T) {
		var m HashTrieMap[string, int]
		if !m.IsZero() {
			t.Fatal("Expected uninitialized map to be zero")
		}
	})

	t.Run("EmptyInitializedMap", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		if !m.IsZero() {
			t.Fatal("Expected empty initialized map to be zero")
		}
	})

	t.Run("MapWithData", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("key", 1)
		if m.IsZero() {
			t.Fatal("Expected map with data to not be zero")
		}
	})

	t.Run("MapAfterClear", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		m.Store("key", 1)
		m.Clear()
		if !m.IsZero() {
			t.Fatal("Expected cleared map to be zero")
		}
	})
}

// TestHashTrieMapUnmarshalJSON tests JSON unmarshaling edge cases
func TestHashTrieMapUnmarshalJSON(t *testing.T) {
	t.Run("EmptyJSON", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		err := m.UnmarshalJSON([]byte("{}"))
		if err != nil {
			t.Fatalf("Expected no error for empty JSON, got %v", err)
		}
		if m.Size() != 0 {
			t.Fatal("Expected empty map after unmarshaling empty JSON")
		}
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		err := m.UnmarshalJSON([]byte("invalid json"))
		if err == nil {
			t.Fatal("Expected error for invalid JSON")
		}
	})

	t.Run("ValidJSON", func(t *testing.T) {
		m := &HashTrieMap[string, int]{}
		err := m.UnmarshalJSON([]byte(`{"key1":10,"key2":20}`))
		if err != nil {
			t.Fatalf("Expected no error for valid JSON, got %v", err)
		}

		if m.Size() != 2 {
			t.Fatalf("Expected size 2, got %d", m.Size())
		}

		value1, found1 := m.Load("key1")
		if !found1 || value1 != 10 {
			t.Fatalf("Expected key1=10, got found=%v, value=%d", found1, value1)
		}

		value2, found2 := m.Load("key2")
		if !found2 || value2 != 20 {
			t.Fatalf("Expected key2=20, got found=%v, value=%d", found2, value2)
		}
	})
}

func TestHashTrieMapCompareAndSwap(t *testing.T) {
	// Test with comparable values
	t.Run("ComparableValues", func(t *testing.T) {
		var m HashTrieMap[string, int]
		m.Store("key1", 100)

		// Successful swap
		if !m.CompareAndSwap("key1", 100, 200) {
			t.Fatal("CompareAndSwap should succeed when old value matches")
		}
		expectPresent(t, "key1", 200)(m.Load("key1"))

		// Failed swap - wrong old value
		if m.CompareAndSwap("key1", 100, 300) {
			t.Fatal("CompareAndSwap should fail when old value doesn't match")
		}
		expectPresent(t, "key1", 200)(m.Load("key1"))

		// Failed swap - non-existent key
		if m.CompareAndSwap("nonexistent", 100, 300) {
			t.Fatal("CompareAndSwap should fail for non-existent key")
		}

		// Swap with same value (should succeed)
		if !m.CompareAndSwap("key1", 200, 200) {
			t.Fatal("CompareAndSwap should succeed when swapping to same value")
		}
		expectPresent(t, "key1", 200)(m.Load("key1"))
	})

	// Test with non-comparable values (should panic)
	t.Run("NonComparableValues", func(t *testing.T) {
		var m HashTrieMap[string, []int] // slice is not comparable
		m.Store("key1", []int{1, 2, 3})

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("CompareAndSwap should panic for non-comparable values")
			} else if !strings.Contains(fmt.Sprint(r), "not of comparable type") {
				t.Fatalf("Unexpected panic message: %v", r)
			}
		}()

		m.CompareAndSwap("key1", []int{1, 2, 3}, []int{4, 5, 6})
	})

	// Test on empty map
	t.Run("EmptyMap", func(t *testing.T) {
		var m HashTrieMap[string, int]
		if m.CompareAndSwap("key1", 100, 200) {
			t.Fatal("CompareAndSwap should fail on empty map")
		}
	})
}

// TestHashTrieMapCompareAndDelete tests the CompareAndDelete function
func TestHashTrieMapCompareAndDelete(t *testing.T) {
	// Test with comparable values
	t.Run("ComparableValues", func(t *testing.T) {
		var m HashTrieMap[string, int]
		m.Store("key1", 100)
		m.Store("key2", 200)

		// Successful delete
		if !m.CompareAndDelete("key1", 100) {
			t.Fatal("CompareAndDelete should succeed when value matches")
		}
		expectMissing(t, "key1", 0)(m.Load("key1"))

		// Failed delete - wrong value
		if m.CompareAndDelete("key2", 100) {
			t.Fatal("CompareAndDelete should fail when value doesn't match")
		}
		expectPresent(t, "key2", 200)(m.Load("key2"))

		// Failed delete - non-existent key
		if m.CompareAndDelete("nonexistent", 100) {
			t.Fatal("CompareAndDelete should fail for non-existent key")
		}
	})

	// Test with non-comparable values (should panic)
	t.Run("NonComparableValues", func(t *testing.T) {
		var m HashTrieMap[string, []int] // slice is not comparable
		m.Store("key1", []int{1, 2, 3})

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("CompareAndDelete should panic for non-comparable values")
			} else if !strings.Contains(fmt.Sprint(r), "not of comparable type") {
				t.Fatalf("Unexpected panic message: %v", r)
			}
		}()

		m.CompareAndDelete("key1", []int{1, 2, 3})
	})

	// Test on empty map
	t.Run("EmptyMap", func(t *testing.T) {
		var m HashTrieMap[string, int]
		if m.CompareAndDelete("key1", 100) {
			t.Fatal("CompareAndDelete should fail on empty map")
		}
	})
}

// TestHashTrieMapEdgeCases tests various edge cases
func TestHashTrieMapEdgeCases(t *testing.T) {
	t.Run("ZeroValues", func(t *testing.T) {
		var m HashTrieMap[string, int]

		// Store zero value
		m.Store("zero", 0)
		expectPresent(t, "zero", 0)(m.Load("zero"))

		// LoadOrStore with zero value
		val, loaded := m.LoadOrStore("zero", 42)
		if !loaded || val != 0 {
			t.Fatalf("LoadOrStore should return existing zero value: got %d, loaded=%v", val, loaded)
		}

		// CompareAndSwap with zero values
		if !m.CompareAndSwap("zero", 0, 1) {
			t.Fatal("CompareAndSwap should work with zero values")
		}
		expectPresent(t, "zero", 1)(m.Load("zero"))

		// CompareAndDelete with zero value
		m.Store("zero2", 0)
		if !m.CompareAndDelete("zero2", 0) {
			t.Fatal("CompareAndDelete should work with zero values")
		}
		expectMissing(t, "zero2", 0)(m.Load("zero2"))
	})

	t.Run("EmptyStringKeys", func(t *testing.T) {
		var m HashTrieMap[string, int]

		// Store with empty string key
		m.Store("", 42)
		expectPresent(t, "", 42)(m.Load(""))

		// Delete empty string key
		m.Delete("")
		expectMissing(t, "", 0)(m.Load(""))
	})

	t.Run("LargeKeys", func(t *testing.T) {
		var m HashTrieMap[string, int]

		// Very long key
		longKey := strings.Repeat("a", 1000)
		m.Store(longKey, 999)
		expectPresent(t, longKey, 999)(m.Load(longKey))

		// Delete long key
		m.Delete(longKey)
		expectMissing(t, longKey, 0)(m.Load(longKey))
	})

	t.Run("ManyOperationsOnSameKey", func(t *testing.T) {
		var m HashTrieMap[string, int]
		key := "test"

		// Multiple stores
		for i := 0; i < 100; i++ {
			m.Store(key, i)
			expectPresent(t, key, i)(m.Load(key))
		}

		// Multiple LoadOrStore operations
		for i := 100; i < 200; i++ {
			val, loaded := m.LoadOrStore(key, i)
			if !loaded || val != 99 {
				t.Fatalf("LoadOrStore iteration %d: expected loaded=true, val=99, got loaded=%v, val=%d", i, loaded, val)
			}
		}

		// Multiple CompareAndSwap operations
		for i := 0; i < 10; i++ {
			oldVal := 99 + i
			newVal := oldVal + 1
			if !m.CompareAndSwap(key, oldVal, newVal) {
				t.Fatalf("CompareAndSwap iteration %d should succeed", i)
			}
		}

		expectPresent(t, key, 109)(m.Load(key))
	})
}

// TestHashTrieMapConcurrentOperations tests concurrent operations
func TestHashTrieMapConcurrentOperations(t *testing.T) {
	t.Run("ConcurrentStoreLoad", func(t *testing.T) {
		var m HashTrieMap[int, int]
		const numGoroutines = 10
		const numOperations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines * 2)

		// Concurrent stores
		for i := 0; i < numGoroutines; i++ {
			go func(base int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := base*numOperations + j
					m.Store(key, key*2)
				}
			}(i)
		}

		// Concurrent loads
		for i := 0; i < numGoroutines; i++ {
			go func(base int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := base*numOperations + j
					// Load might not find the value due to timing, but shouldn't panic
					m.Load(key)
				}
			}(i)
		}

		wg.Wait()

		// Verify some data exists
		if m.Size() == 0 {
			t.Fatal("Map should not be empty after concurrent operations")
		}
	})

	t.Run("ConcurrentCompareAndSwap", func(t *testing.T) {
		var m HashTrieMap[string, int]
		m.Store("counter", 0)

		const numGoroutines = 10
		const incrementsPerGoroutine = 10

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < incrementsPerGoroutine; j++ {
					for {
						val, ok := m.Load("counter")
						if !ok {
							continue
						}
						if m.CompareAndSwap("counter", val, val+1) {
							break
						}
					}
				}
			}()
		}

		wg.Wait()

		expectedValue := numGoroutines * incrementsPerGoroutine
		actualValue, ok := m.Load("counter")
		if !ok {
			t.Fatal("Counter should exist")
		}
		if actualValue != expectedValue {
			t.Fatalf("Expected counter value %d, got %d", expectedValue, actualValue)
		}
	})
}
