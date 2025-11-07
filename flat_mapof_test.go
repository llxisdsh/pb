package pb

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestFlatMapOf_BucketOfStructSize(t *testing.T) {
	t.Logf("CacheLineSize : %d", CacheLineSize)
	t.Logf("entriesPerBucket : %d", entriesPerBucket)

	size := unsafe.Sizeof(FlatMapOf[string, int]{})
	t.Log("FlatMapOf size:", size)
	if size != CacheLineSize {
		t.Fatalf("FlatMapOf doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(flatRebuildState[string, int]{})
	t.Log("flatRebuildState size:", size)
	if size != CacheLineSize {
		t.Fatalf("flatRebuildState doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(flatTable[string, int]{})
	t.Log("flatTable size:", size)
	//if size != CacheLineSize {
	//	t.Fatalf("flatTable doesn't meet CacheLineSize: %d", size)
	//}

	size = unsafe.Sizeof(flatBucket[string, int]{})
	t.Log("flatBucket size:", size)
	//if size != CacheLineSize {
	//	t.Fatalf("flatBucket doesn't meet CacheLineSize: %d", size)
	//}
}

// TestFlatMapOf_BasicOperations tests basic Load and Process operations
func TestFlatMapOf_BasicOperations(t *testing.T) {
	var m FlatMapOf[string, int]

	// Test empty map
	if val, ok := m.Load("nonexistent"); ok {
		t.Errorf("Expected not found, got value %v", val)
	}

	// Test insert
	actual, ok := m.Process(
		"key1",
		func(old int, loaded bool) (int, ComputeOp, int, bool) {
			if loaded {
				t.Error("Expected not loaded for new key")
			}
			return 42, UpdateOp, 42, true
		},
	)
	if !ok || actual != 42 {
		t.Errorf("Expected (42, true), got (%v, %v)", actual, ok)
	}

	// Test load after insert
	if val, loaded := m.Load("key1"); !loaded || val != 42 {
		t.Errorf("Expected (42, true), got (%v, %v)", val, ok)
	}

	// Test update
	actual, ok = m.Process(
		"key1",
		func(old int, loaded bool) (int, ComputeOp, int, bool) {
			if !loaded || old != 42 {
				t.Errorf(
					"Expected loaded=true, old=42, got loaded=%v, old=%v",
					loaded,
					old,
				)
			}
			return old + 10, UpdateOp, old + 10, true
		},
	)
	if !ok || actual != 52 {
		t.Errorf("Expected (52, true), got (%v, %v)", actual, ok)
	}

	// Test load after update
	if val, loaded := m.Load("key1"); !loaded || val != 52 {
		t.Errorf("Expected (52, true), got (%v, %v)", val, loaded)
	}

	// Test delete
	_, ok = m.Process(
		"key1",
		func(old int, loaded bool) (int, ComputeOp, int, bool) {
			if !loaded || old != 52 {
				t.Errorf(
					"Expected loaded=true, old=52, got loaded=%v, old=%v",
					loaded,
					old,
				)
			}
			return 0, DeleteOp, old, false
		},
	)
	if ok {
		t.Errorf("Expected ok=false after delete, got ok=%v", ok)
	}

	// Test load after delete
	if val, loaded := m.Load("key1"); loaded {
		t.Errorf("Expected not found after delete, got (%v, %v)", val, loaded)
	}

	// Test cancel operation
	m.Process("key2", func(old int, loaded bool) (int, ComputeOp, int, bool) {
		return 100, UpdateOp, 100, true
	})
	actual, ok = m.Process(
		"key2",
		func(old int, loaded bool) (int, ComputeOp, int, bool) {
			return 999, CancelOp, old, loaded
		},
	)
	if !ok || actual != 100 {
		t.Errorf("Expected (100, true) after cancel, got (%v, %v)", actual, ok)
	}
}

// TestFlatMapOf_EdgeCases tests edge cases and error conditions
func TestFlatMapOf_EdgeCases(t *testing.T) {
	var m FlatMapOf[string, *string]

	// Test with empty string key
	m.Process(
		"",
		func(old *string, loaded bool) (*string, ComputeOp, *string, bool) {
			newV := "empty_key_value"
			return &newV, UpdateOp, &newV, true
		},
	)
	if val, ok := m.Load(""); !ok || *val != "empty_key_value" {
		t.Errorf("Empty key test failed: got (%v, %v)", *val, ok)
	}

	// Test with very long key
	longKey := string(make([]byte, 1000))
	for i := range longKey {
		longKey = longKey[:i] + "a" + longKey[i+1:]
	}
	m.Process(
		longKey,
		func(old *string, loaded bool) (*string, ComputeOp, *string, bool) {
			newV := "long_key_value"
			return &newV, UpdateOp, &newV, true
		},
	)
	if val, ok := m.Load(longKey); !ok || *val != "long_key_value" {
		t.Errorf("Long key test failed: got (%v, %v)", *val, ok)
	}

	// Verify data still intact
	if val, ok := m.Load(""); !ok || *val != "empty_key_value" {
		t.Errorf(
			"After invalid grow, empty key test failed: got (%v, %v)",
			*val,
			ok,
		)
	}
}

// TestFlatMapOf_MultipleKeys tests operations with multiple keys
func TestFlatMapOf_MultipleKeys(t *testing.T) {
	var m FlatMapOf[int, *string]

	// Insert multiple keys
	for i := range 100 {
		m.Process(
			i,
			func(old *string, loaded bool) (*string, ComputeOp, *string, bool) {
				newV := fmt.Sprintf("value_%d", i)
				return &newV, UpdateOp, &newV, true
			},
		)
	}

	// Verify all keys
	for i := range 100 {
		expected := fmt.Sprintf("value_%d", i)
		if val, ok := m.Load(i); !ok || *val != expected {
			t.Errorf(
				"Key %d: expected (%s, true), got (%v, %v)",
				i,
				expected,
				val,
				ok,
			)
		}
	}

	// Delete even keys
	for i := 0; i < 100; i += 2 {
		m.Process(
			i,
			func(old *string, loaded bool) (*string, ComputeOp, *string, bool) {
				newV := ""
				return &newV, DeleteOp, &newV, false
			},
		)
	}

	// Verify deletions
	for i := range 100 {
		val, ok := m.Load(i)
		if i%2 == 0 {
			// Even keys should be deleted
			if ok {
				t.Errorf(
					"Key %d should be deleted, but got (%v, %v)",
					i,
					val,
					ok,
				)
			}
		} else {
			// Odd keys should remain
			expected := fmt.Sprintf("value_%d", i)
			if !ok || *val != expected {
				t.Errorf("Key %d: expected (%s, true), got (%v, %v)", i, expected, val, ok)
			}
		}
	}
}

// TestFlatMapOf_Store tests the Store method
func TestFlatMapOf_Store(t *testing.T) {
	var m FlatMapOf[string, int]

	// Test store new key
	m.Store("key1", 100)
	if val, ok := m.Load("key1"); !ok || val != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", val, ok)
	}

	// Test store existing key (update)
	m.Store("key1", 200)
	if val, ok := m.Load("key1"); !ok || val != 200 {
		t.Errorf("Expected (200, true), got (%v, %v)", val, ok)
	}
}

// TestFlatMapOf_LoadOrStore tests the LoadOrStore method
func TestFlatMapOf_LoadOrStore(t *testing.T) {
	var m FlatMapOf[string, int]

	// Test store new key
	actual, loaded := m.LoadOrStore("key1", 100)
	if loaded || actual != 100 {
		t.Errorf("Expected (0, false), got (%v, %v)", actual, loaded)
	}

	// Test load existing key
	actual, loaded = m.LoadOrStore("key1", 200)
	if !loaded || actual != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", actual, loaded)
	}

	// Verify value wasn't changed
	if val, ok := m.Load("key1"); !ok || val != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", val, ok)
	}
}

// TestFlatMapOf_LoadOrStoreFn tests the LoadOrStoreFn method
func TestFlatMapOf_LoadOrStoreFn(t *testing.T) {
	var m FlatMapOf[string, int]

	// Test store new key
	actual, loaded := m.LoadOrStoreFn("key1", func() int { return 100 })
	if loaded || actual != 100 {
		t.Errorf("Expected (0, false), got (%v, %v)", actual, loaded)
	}

	// Test load existing key
	actual, loaded = m.LoadOrStoreFn("key1", func() int { return 200 })
	if !loaded || actual != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", actual, loaded)
	}

	// Verify value wasn't changed
	if val, ok := m.Load("key1"); !ok || val != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", val, ok)
	}
}

// TestFlatMapOf_Delete tests the Delete method
func TestFlatMapOf_Delete(t *testing.T) {
	var m FlatMapOf[string, int]

	// Store a key
	m.Store("key1", 100)
	if val, ok := m.Load("key1"); !ok || val != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", val, ok)
	}

	// Delete the key
	m.Delete("key1")
	if val, ok := m.Load("key1"); ok {
		t.Errorf("Expected key to be deleted, but got (%v, %v)", val, ok)
	}

	// Delete non-existent key (should not panic)
	m.Delete("nonexistent")
}

func TestFlatMapOf_Swap(t *testing.T) {
	var m FlatMapOf[string, int]

	// Swap on empty: returns zero, loaded=false, sets value
	prev, loaded := m.Swap("a", 1)
	if loaded || prev != 0 {
		t.Errorf("first swap got (%d,%v), want (0,false)", prev, loaded)
	}
	if v, ok := m.Load("a"); !ok || v != 1 {
		t.Errorf("post swap got (%d,%v), want (1,true)", v, ok)
	}

	// Swap on existing: returns previous, loaded=true, updates value
	prev, loaded = m.Swap("a", 2)
	if !loaded || prev != 1 {
		t.Errorf("second swap got (%d,%v), want (1,true)", prev, loaded)
	}
	if v, ok := m.Load("a"); !ok || v != 2 {
		t.Errorf("final value got (%d,%v), want (2,true)", v, ok)
	}
}

func TestFlatMapOf_LoadAndDelete(t *testing.T) {
	var m FlatMapOf[string, int]

	m.Store("x", 100)
	prev, loaded := m.LoadAndDelete("x")
	if !loaded || prev != 100 {
		t.Errorf("load+del got (%d,%v), want (100,true)", prev, loaded)
	}
	if v, ok := m.Load("x"); ok {
		t.Errorf("after delete found key: (%d,%v)", v, ok)
	}

	// Deleting missing key: zero, false
	prev, loaded = m.LoadAndDelete("x")
	if loaded || prev != 0 {
		t.Errorf("delete missing got (%d,%v), want (0,false)", prev, loaded)
	}
	if v, ok := m.Load("y"); ok {
		t.Errorf("missing key y found: (%d,%v)", v, ok)
	}
}

func TestFlatMapOf_LoadAndUpdate(t *testing.T) {
	var m FlatMapOf[string, int]

	m.Store("k", 7)
	prev, loaded := m.LoadAndUpdate("k", 9)
	if !loaded || prev != 7 {
		t.Errorf("load+update got (%d,%v), want (7,true)", prev, loaded)
	}
	if v, ok := m.Load("k"); !ok || v != 9 {
		t.Errorf("updated value got (%d,%v), want (9,true)", v, ok)
	}

	// Missing key: should not insert, returns zero, false
	prev, loaded = m.LoadAndUpdate("missing", 1)
	if loaded || prev != 0 {
		t.Errorf("update missing got (%d,%v), want (0,false)", prev, loaded)
	}
	if _, ok := m.Load("missing"); ok {
		t.Errorf("missing inserted by update unexpectedly")
	}
}

// TestFlatMapOf_Concurrent tests concurrent operations
func TestFlatMapOf_Concurrent(t *testing.T) {
	var m FlatMapOf[int, int]
	const numGoroutines = 10
	const numOpsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writers
	for g := range numGoroutines {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 1; i <= numOpsPerGoroutine; i++ {
				key := goroutineID*numOpsPerGoroutine + i
				m.Process(
					key,
					func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return key * 2, UpdateOp, key * 2, true
					},
				)
			}
		}(g)
	}

	// Concurrent readers
	for g := range numGoroutines {
		go func(goroutineID int) {
			for i := 1; i <= numOpsPerGoroutine; i++ {
				key := goroutineID*numOpsPerGoroutine + i
				// May or may not find the key depending on timing
				m.Load(key)
			}
		}(g)
	}

	wg.Wait()

	// Verify final state
	for g := range numGoroutines {
		for i := 1; i <= numOpsPerGoroutine; i++ {
			key := g*numOpsPerGoroutine + i
			expected := key * 2
			if val, ok := m.Load(key); !ok || val != expected {
				t.Errorf(
					"Key %d: expected (%d, true), got (%v, %v)",
					key,
					expected,
					val,
					ok,
				)
			}
		}
	}
}

// TestFlatMapOf_ConcurrentReadWrite tests heavy concurrent read/write load
func TestFlatMapOf_ConcurrentReadWrite(t *testing.T) {
	var m FlatMapOf[int, int]

	// Reduce test duration and concurrency for coverage mode
	var duration time.Duration
	var numReaders, numWriters int
	if testing.CoverMode() != "" {
		duration = 500 * time.Millisecond
		numReaders = 4
		numWriters = 1
	} else {
		duration = 2 * time.Second
		numReaders = 8
		numWriters = 2
	}

	// Pre-populate with some data
	for i := range 1000 {
		m.Process(i, func(old int, loaded bool) (int, ComputeOp, int, bool) {
			return i, UpdateOp, i, true
		})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Start readers
	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(1000)
					m.Load(key)
				}
			}
		}()
	}

	// Start writers
	for range numWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(1000)
					m.Process(
						key,
						func(old int, loaded bool) (int, ComputeOp, int, bool) {
							newV := rand.IntN(10000)
							return newV, UpdateOp, newV, true
						},
					)
				}
			}
		}()
	}

	// Run for specified duration
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// t.Log("Concurrent read/write test completed successfully")
}

// TestFlatMapOf_Range tests the Range method
func TestFlatMapOf_Range(t *testing.T) {
	var m FlatMapOf[int, *string]

	// Test empty map
	count := 0
	m.Range(func(k int, v *string) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("Expected 0 iterations on empty map, got %d", count)
	}

	// Add some data
	expected := make(map[int]string)
	for i := range 10 {
		value := fmt.Sprintf("value_%d", i)
		m.Store(i, &value)
		expected[i] = value
	}

	// Test full iteration
	found := make(map[int]string)
	m.Range(func(k int, v *string) bool {
		found[k] = *v
		return true
	})

	if len(found) != len(expected) {
		t.Errorf("Expected %d items, got %d", len(expected), len(found))
	}

	for k, v := range expected {
		if foundV, ok := found[k]; !ok || foundV != v {
			t.Errorf("Key %d: expected %s, got %s (ok=%v)", k, v, foundV, ok)
		}
	}

	// Test early termination
	count = 0
	m.Range(func(k int, v *string) bool {
		count++
		return count < 5 // Stop after 5 iterations
	})
	if count != 5 {
		t.Errorf("Expected 5 iterations with early termination, got %d", count)
	}
}

// TestFlatMapOf_Size tests the Size method
func TestFlatMapOf_Size(t *testing.T) {
	var m FlatMapOf[int, *string]

	// Test empty map
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 for empty map, got %d", size)
	}

	// Add items and check size
	for i := range 10 {
		value := fmt.Sprintf("value_%d", i)
		m.Store(i, &value)
		expectedSize := i + 1
		if size := m.Size(); size != expectedSize {
			t.Errorf(
				"After storing %d items, expected size %d, got %d",
				expectedSize,
				expectedSize,
				size,
			)
		}
	}

	// Delete items and check size
	for i := range 5 {
		m.Delete(i)
		expectedSize := 10 - i - 1
		if size := m.Size(); size != expectedSize {
			t.Errorf(
				"After deleting %d items, expected size %d, got %d",
				i+1,
				expectedSize,
				size,
			)
		}
	}

	// Test Clear and verify size becomes 0
	m.Clear()
	if size := m.Size(); size != 0 {
		t.Errorf("After Clear(), expected size 0, got %d", size)
	}

	// Add items again after Clear to verify functionality
	for i := range 3 {
		value := fmt.Sprintf("after_clear_%d", i)
		m.Store(i+100, &value)
	}
	if size := m.Size(); size != 3 {
		t.Errorf("After adding 3 items post-Clear, expected size 3, got %d", size)
	}
}

// TestFlatMapOf_IsZero tests the IsZero method
func TestFlatMapOf_IsZero(t *testing.T) {
	var m FlatMapOf[string, int]

	// Test empty map
	if !m.IsZero() {
		t.Error("Expected IsZero() to return true for empty map")
	}

	// Add an item
	m.Store("key1", 100)
	if m.IsZero() {
		t.Error("Expected IsZero() to return false for non-empty map")
	}

	// Delete the item
	m.Delete("key1")
	if !m.IsZero() {
		t.Error("Expected IsZero() to return true after deleting all items")
	}

	// Add multiple items
	for i := 1; i <= 10; i++ {
		m.Store(fmt.Sprintf("key_%d", i), i)
	}
	if m.IsZero() {
		t.Error("Expected IsZero() to return false for map with multiple items")
	}

	// Test Clear as an alternative to individual deletes
	m.Clear()
	if !m.IsZero() {
		t.Error("Expected IsZero() to return true after Clear()")
	}

	// Verify Clear is equivalent to individual deletes
	for i := 1; i <= 5; i++ {
		m.Store(fmt.Sprintf("test_%d", i), i*100)
	}
	if m.IsZero() {
		t.Error("Expected IsZero() to return false after adding items")
	}

	// Delete all items individually for comparison
	for i := 1; i <= 5; i++ {
		m.Delete(fmt.Sprintf("test_%d", i))
	}
	if !m.IsZero() {
		t.Error("Expected IsZero() to return true after deleting all items individually")
	}
}

// ---------------- Additional boundary/concurrency tests ----------------

func TestFlatMapOf_LoadOrStoreFn_OnceUnderRace(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	var called int32
	var wg sync.WaitGroup
	workers := max(2, runtime.GOMAXPROCS(0)) // Reduce Concurrency
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			_, _ = m.LoadOrStoreFn(999, func() int {
				// widen race window
				atomic.AddInt32(&called, 1)
				time.Sleep(1 * time.Millisecond)
				return 777
			})
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&called); got != 1 {
		t.Fatalf("LoadOrStoreFn invoked %d times; want 1", got)
	}
	if v, ok := m.Load(999); !ok || v != 777 {
		t.Fatalf("post state: got (%v,%v), want (777,true)", v, ok)
	}
}

// TestFlatMapOf_DoubleBufferConsistency tests the double buffer mechanism
func TestFlatMapOf_DoubleBufferConsistency(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	const numKeys = 100
	const numUpdates = 50

	// Insert initial data
	for i := 1; i <= numKeys; i++ {
		m.Process(i, func(old int, loaded bool) (int, ComputeOp, int, bool) {
			return i, UpdateOp, i, true
		})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Continuous readers to stress test the double buffer
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := 1; i <= numKeys; i++ {
						val, ok := m.Load(i)
						if !ok {
							t.Errorf("Key %d should exist", i)
							return
						}
						// Value should be consistent (either old or new, but
						// not corrupted)
						if val < 0 {
							t.Errorf("Corrupted value %d for key %d", val, i)
							return
						}
					}
				}
			}
		}()
	}

	// Writer that updates all keys multiple times
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range numUpdates {
			for i := 1; i <= numKeys; i++ {
				m.Process(
					i,
					func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return old + 1000, UpdateOp, old + 1000, true
					},
				)
			}
			runtime.Gosched() // Give readers a chance
		}
	}()

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	// t.Log("Double buffer consistency test completed")
}

// TestFlatMapOf_DoubleBufferConsistency_StressABA stresses rapid A/B flips on
// the same key and verifies that readers never observe torn values (i.e.,
// reading half old, half new). This detects correctness of the double-buffer
// protocol under extreme contention.
func TestFlatMapOf_DoubleBufferConsistency_StressABA(t *testing.T) {
	type pair struct{ X, Y uint16 }
	m := NewFlatMapOf[int, pair]()

	// Initialize key 0
	m.Store(0, pair{X: 0, Y: ^uint16(0)})

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		seq  uint32
	)

	// Start multiple writers to maximize flip frequency on the same slot
	writerN := 4
	for range writerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s := atomic.AddUint32(&seq, 1)
					val := pair{X: uint16(s), Y: ^uint16(s)}
					m.Process(
						0,
						func(old pair, loaded bool) (pair, ComputeOp, pair, bool) {
							return val, UpdateOp, val, true
						},
					)
				}
			}
		}()
	}

	// Start readers to continuously validate that values are not torn
	readerN := 8
	for range readerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v, ok := m.Load(0)
					if !ok {
						t.Errorf("key missing while stressed")
						return
					}
					if v.Y != ^v.X {
						t.Errorf("torn read detected: %+v", v)
						return
					}
				}
			}
		}()
	}

	// Run for a short duration to stress concurrent flips
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// Stress ABA-like rapid flips on the same key and ensure seqlock prevents torn
// reads.
func TestFlatMapOf_SeqlockConsistency_StressABA(t *testing.T) {
	type pair struct{ X, Y uint64 }
	m := NewFlatMapOf[int, pair]()

	m.Store(0, pair{X: 0, Y: ^uint64(0)})

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		seq  uint32
	)

	writerN := 4 // Reduce Concurrency
	for range writerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s := atomic.AddUint32(&seq, 1)
					val := pair{X: uint64(s), Y: ^uint64(s)}
					m.Process(
						0,
						func(old pair, loaded bool) (pair, ComputeOp, pair, bool) {
							return val, UpdateOp, val, true
						},
					)
				}
			}
		}()
	}

	readerN := 4 // Reduce Concurrency
	for range readerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v, ok := m.Load(0)
					if !ok {
						t.Errorf("key missing while stressed")
						return
					}
					if v.Y != ^v.X {
						t.Errorf("torn read detected: %+v", v)
						return
					}
				}
			}
		}()
	}

	time.Sleep(150 * time.Millisecond) // Reduce Duration
	close(stop)
	wg.Wait()
}

// Load/Delete race semantics: under seqlock, readers should not observe
// ok==true with zero value.
func TestFlatMapOf_LoadDeleteRace_Semantics(t *testing.T) {
	m := NewFlatMapOf[string, uint32]()
	const key = "k"
	const insertedVal uint32 = 1

	var (
		anom uint64
		stop uint32
	)

	readers := max(2, runtime.GOMAXPROCS(0)) // Reduce Concurrency
	dur := 500 * time.Millisecond            // Reduce Duration

	var wg sync.WaitGroup
	wg.Add(readers + 1)

	go func() {
		defer wg.Done()
		deadline := time.Now().Add(dur)
		for time.Now().Before(deadline) {
			m.Store(key, insertedVal)
			runtime.Gosched()
			m.Delete(key)
			runtime.Gosched()
		}
		atomic.StoreUint32(&stop, 1)
	}()

	for range readers {
		go func() {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				v, ok := m.Load(key)
				if ok && v == 0 {
					atomic.AddUint64(&anom, 1)
				}
			}
		}()
	}

	wg.Wait()
	if anom > 0 {
		t.Fatalf(
			"Load/Delete race: observed ok==true && value==0 %d times",
			anom,
		)
	}
}

func TestFlatMapOf_KeyTornRead_Stress(t *testing.T) {
	// This test stresses concurrent delete/re-insert cycles to try to expose
	// torn reads on keys when K is larger than machine word size.
	// It also serves as a good target for `go test -race` which should report
	// a data race if key memory is read while being concurrently cleared.

	type bigKey struct{ A, B uint64 }

	m := NewFlatMapOf[bigKey, int]()

	const N = 1024
	keys := make([]bigKey, N)
	for i := range N {
		// Maintain invariant: B == ^A
		ai := uint64(i*2147483647 + 123456789)
		keys[i] = bigKey{A: ai, B: ^ai}
		m.Store(keys[i], i)
	}

	var (
		wg           sync.WaitGroup
		stop         = make(chan struct{})
		foundTornKey atomic.Bool
	)

	// Readers: continuously range and validate key invariant
	readerN := 8
	wg.Add(readerN)
	for range readerN {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m.Range(func(k bigKey, _ int) bool {
						if k.B != ^k.A {
							t.Logf("torn key: %v", k)
							foundTornKey.Store(true)
							return false
						}
						return true
					})
				}
			}
		}()
	}

	// Additional readers: hammer Load to exercise key comparisons
	loadN := 4
	wg.Add(loadN)
	for r := range loadN {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := id; i < N; i += loadN {
						m.Load(keys[i])
					}
				}
			}
		}(r)
	}

	// Writers: repeatedly delete and re-insert the same keys to trigger
	// meta-clearing followed by key memory clearing in current implementation.
	writerN := 2
	wg.Add(writerN)
	for w := range writerN {
		go func(offset int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := offset; i < N; i += writerN {
						k := keys[i]
						// Delete
						m.Process(
							k,
							func(old int, loaded bool) (int, ComputeOp, int, bool) {
								return 0, DeleteOp, 0, false
							},
						)
						// Re-insert
						m.Process(
							k,
							func(old int, loaded bool) (int, ComputeOp, int, bool) {
								return i, UpdateOp, i, true
							},
						)
					}
					runtime.Gosched()
				}
			}
		}(w)
	}

	// Run for a short duration
	dur := 1500 * time.Millisecond
	timer := time.NewTimer(dur)
	<-timer.C
	close(stop)
	wg.Wait()

	if foundTornKey.Load() {
		t.Fatalf(
			"detected possible torn read of key: invariant k.B == ^k.A violated under concurrency",
		)
	}
}

// Key torn-read stress: delete/re-insert big keys under load and ensure key
// invariants hold.
func TestFlatMapOf_KeyTornRead_Stress_Heavy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy stress in -short mode")
	}

	type bigKey struct{ A, B uint64 }
	m := NewFlatMapOf[bigKey, int]()

	const N = 4096
	keys := make([]bigKey, N)
	for i := range N {
		ai := uint64(i*2147483647 + 987654321)
		keys[i] = bigKey{A: ai, B: ^ai}
		m.Store(keys[i], i)
	}

	var (
		wg           sync.WaitGroup
		stop         = make(chan struct{})
		foundTornKey atomic.Bool
	)

	readerN := max(8, runtime.GOMAXPROCS(0)*2)
	wg.Add(readerN)
	for range readerN {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m.Range(func(k bigKey, _ int) bool {
						if k.B != ^k.A {
							foundTornKey.Store(true)
							return false
						}
						return true
					})
				}
			}
		}()
	}

	loadN := max(4, runtime.GOMAXPROCS(0))
	wg.Add(loadN)
	for r := range loadN {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := id; i < N; i += loadN {
						m.Load(keys[i])
					}
				}
			}
		}(r)
	}

	writerN := max(8, runtime.GOMAXPROCS(0)*2)
	wg.Add(writerN)
	for w := range writerN {
		go func(offset int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := offset; i < N; i += writerN {
						k := keys[i]
						m.Process(
							k,
							func(old int, loaded bool) (int, ComputeOp, int, bool) { return 0, DeleteOp, 0, false },
						)
						m.Process(
							k,
							func(old int, loaded bool) (int, ComputeOp, int, bool) { return i, UpdateOp, i, true },
						)
					}
					runtime.Gosched()
				}
			}
		}(w)
	}

	dur := 2 * time.Second
	timer := time.NewTimer(dur)
	<-timer.C
	close(stop)
	wg.Wait()

	if foundTornKey.Load() {
		t.Fatalf(
			"detected possible torn read of key: invariant k.B == ^k.A violated under concurrency (heavy)",
		)
	}
}

func TestFlatMapOf_Range_NoDuplicateVisit_Heavy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy stress in -short mode")
	}

	m := NewFlatMapOf[int, int]()
	const N = 2048 * 50 // ~100K keys to cover chains
	for i := range N {
		m.Store(i, i)
	}

	var stop uint32
	writerN := max(4, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	wg.Add(writerN)
	for w := range writerN {
		go func(offset int) {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				for i := offset; i < N; i += writerN {
					m.Process(
						i,
						func(old int, loaded bool) (int, ComputeOp, int, bool) {
							return old + 1, UpdateOp, old + 1, true
						},
					)
				}
				runtime.Gosched()
			}
		}(w)
	}

	// Multiple Range passes; each pass must not yield duplicates
	rounds := 20
	for range rounds {
		seen := make([]uint8, N) // compact bitset
		m.Range(func(k, v int) bool {
			if k >= 0 && k < N {
				if seen[k] != 0 {
					t.Fatalf(
						"Range yielded duplicate key within a single pass (heavy): %d",
						k,
					)
				}
				seen[k] = 1
			}
			return true
		})
	}

	atomic.StoreUint32(&stop, 1)
	wg.Wait()
}

// New test: verify a single Range call never yields duplicate keys, even under
// concurrent writers causing seqlock retries.
func TestFlatMapOf_Range_NoDuplicateVisit(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	const N = 2048 * 100 // cover multiple buckets and chains
	for i := range N {
		m.Store(i, i)
	}

	var stop uint32
	writerN := max(2, runtime.GOMAXPROCS(0)/2) // Reduce Concurrency
	var wg sync.WaitGroup
	wg.Add(writerN)
	for w := range writerN {
		go func(offset int) {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				for i := offset; i < N; i += writerN {
					// mutate value to force seq changes
					m.Process(
						i,
						func(old int, loaded bool) (int, ComputeOp, int, bool) {
							return old + 1, UpdateOp, old + 1, true
						},
					)
				}
				runtime.Gosched()
			}
		}(w)
	}

	// Run several Range passes under writer churn; each pass must not see
	// duplicates
	rounds := 10
	for range rounds {
		seen := make(map[int]struct{}, N)
		m.Range(func(k, v int) bool {
			if _, dup := seen[k]; dup {
				t.Fatalf(
					"Range yielded duplicate key within a single pass: %d",
					k,
				)
			}
			seen[k] = struct{}{}
			return true
		})
	}

	atomic.StoreUint32(&stop, 1)
	wg.Wait()
}

func TestFlatMapOf_RangeProcess_Basic(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	const N = 1024
	for i := range N {
		m.Store(i, i)
	}

	// Delete evens, add +100 to odds, cancel others (none)
	m.RangeProcess(func(k, v int) (int, ComputeOp) {
		if k%2 == 0 {
			return 0, DeleteOp
		}
		return v + 100, UpdateOp
	})

	for i := range N {
		v, ok := m.Load(i)
		if i%2 == 0 {
			if ok {
				t.Fatalf("key %d should be deleted", i)
			}
		} else {
			if !ok || v != i+100 {
				t.Fatalf("key %d got (%v,%v), want (%d,true)", i, v, ok, i+100)
			}
		}
	}
}

func TestFlatMapOf_RangeProcess_CancelAndEarlyStop(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	for i := range 100 {
		m.Store(i, i)
	}
	count := 0
	m.RangeProcess(func(k, v int) (int, ComputeOp) {
		count++
		if count >= 10 {
			return v, CancelOp // no change
		}
		return v + 1, UpdateOp
	})
	// we cannot assert exact count, but state should be consistent
	// First 9 updated, others unchanged
	for i := range 100 {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("missing key %d", i)
		}
		if i < 9 {
			if v != i+1 {
				t.Fatalf("key %d expected %d got %d", i, i+1, v)
			}
		} else if v != i {
			t.Fatalf("key %d expected %d got %d", i, i, v)
		}
	}
}

// Concurrency smoke: interleave RangeProcess with Process to ensure no panics
// and state convergence.
func TestFlatMapOf_RangeProcess_Concurrent(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	const N = 512
	for i := range N {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(3)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.RangeProcess(func(k, v int) (int, ComputeOp) {
					if k%7 == 0 {
						return 0, DeleteOp
					}
					return v + 1, UpdateOp
				})
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := range N {
					m.Process(
						i,
						func(old int, loaded bool) (int, ComputeOp, int, bool) {
							if !loaded {
								return i, UpdateOp, i, false
							}
							return old + 1, UpdateOp, old + 1, true
						},
					)
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := range N {
					m.Load(i)
				}
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestFlatMapOf_ConcurrentShrink(t *testing.T) {
	m := NewFlatMapOf[int, int](WithShrinkEnabled())
	const numEntries = 10000
	const numGoroutines = 8
	const numOperations = 1000

	// Pre-populate the map to create a large table
	for i := range numEntries {
		m.Store(i, i*2)
	}

	// Delete most entries to trigger potential shrink
	for i := range numEntries - 100 {
		m.Delete(i)
	}

	var wg sync.WaitGroup
	var errors int64

	// Concurrent readers during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				key := numEntries - 100 + (base*numOperations+i)%100
				if val, ok := m.Load(key); ok && val != key*2 {
					atomic.AddInt64(&errors, 1)
				}
				runtime.Gosched()
			}
		}(g)
	}

	// Concurrent writers during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				key := numEntries + base*numOperations + i
				m.Store(key, key*2)
				if val, ok := m.Load(key); !ok || val != key*2 {
					atomic.AddInt64(&errors, 1)
				}
				m.Delete(key)
				runtime.Gosched()
			}
		}(g)
	}

	// Concurrent deleters during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				key := numEntries + 100000 + base*numOperations + i
				m.Store(key, key*2)
				m.Delete(key)
				if _, ok := m.Load(key); ok {
					atomic.AddInt64(&errors, 1)
				}
				runtime.Gosched()
			}
		}(g)
	}

	wg.Wait()

	if errors > 0 {
		t.Errorf("Found %d data consistency errors during concurrent shrink", errors)
	}

	// Verify remaining data integrity
	for i := numEntries - 100; i < numEntries; i++ {
		if val, ok := m.Load(i); !ok || val != i*2 {
			t.Errorf("Data corruption after shrink: key=%d, expected=%d, actual=%d, ok=%v",
				i, i*2, val, ok)
		}
	}
}

// TestFlatMapOf_ShrinkDataIntegrity tests data integrity during shrink operations
func TestFlatMapOf_ShrinkDataIntegrity(t *testing.T) {
	m := NewFlatMapOf[string, int](WithShrinkEnabled())
	const numEntries = 5000

	// Create test data
	for i := range numEntries {
		key := fmt.Sprintf("key_%d", i)
		value := i * 3
		m.Store(key, value)
	}

	// Delete most entries to trigger shrink (delete first numEntries-200 keys)
	deletedKeys := make(map[string]bool)
	for i := range numEntries - 200 {
		key := fmt.Sprintf("key_%d", i)
		m.Delete(key)
		deletedKeys[key] = true
	}

	// Verify remaining data integrity after shrink
	for i := numEntries - 200; i < numEntries; i++ {
		key := fmt.Sprintf("key_%d", i)
		expectedValue := i * 3
		if actualValue, ok := m.Load(key); !ok || actualValue != expectedValue {
			t.Errorf("Data corruption after shrink: key=%s, expected=%d, actual=%d, ok=%v",
				key, expectedValue, actualValue, ok)
		}
	}

	// Verify deleted keys are actually gone
	for key := range deletedKeys {
		if _, ok := m.Load(key); ok {
			t.Errorf("Deleted key still present after shrink: %s", key)
		}
	}
}

// TestFlatMapOf_ConcurrentShrinkWithRangeProcess tests RangeProcess during shrink
func TestFlatMapOf_ConcurrentShrinkWithRangeProcess(t *testing.T) {
	m := NewFlatMapOf[int, int](WithShrinkEnabled())
	const numEntries = 8000
	const numGoroutines = 4

	// Pre-populate
	for i := range numEntries {
		m.Store(i, i*5)
	}

	// Delete most entries to trigger shrink
	for i := range numEntries - 500 {
		m.Delete(i)
	}

	var wg sync.WaitGroup
	var rangeErrors int64
	var processErrors int64

	// Concurrent Range operations during shrink
	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range 100 {
				count := 0
				m.Range(func(k, v int) bool {
					count++
					if v != k*5 {
						atomic.AddInt64(&rangeErrors, 1)
					}
					return true
				})
				if count == 0 {
					atomic.AddInt64(&rangeErrors, 1)
				}
				runtime.Gosched()
			}
		}()
	}

	// Concurrent RangeProcess operations during shrink
	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range 50 {
				processCount := 0
				m.RangeProcess(func(key int, value int) (int, ComputeOp) {
					processCount++
					if value != key*5 {
						atomic.AddInt64(&processErrors, 1)
					}
					return value, CancelOp // Don't modify during test
				})
				if processCount == 0 {
					atomic.AddInt64(&processErrors, 1)
				}
				runtime.Gosched()
			}
		}()
	}

	// Concurrent operations during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range 200 {
				key := numEntries + base*200 + i
				m.Store(key, key*5)
				if val, ok := m.Load(key); !ok || val != key*5 {
					atomic.AddInt64(&processErrors, 1)
				}
				m.Delete(key)
				runtime.Gosched()
			}
		}(g)
	}

	wg.Wait()

	if rangeErrors > 0 {
		t.Errorf("Found %d Range errors during concurrent shrink", rangeErrors)
	}
	if processErrors > 0 {
		t.Errorf("Found %d RangeProcess errors during concurrent shrink", processErrors)
	}
}

func TestFlatMapOf_ShrinkStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	m := NewFlatMapOf[int, int](WithShrinkEnabled())
	const cycles = 10
	const entriesPerCycle = 5000
	const numGoroutines = 6

	for cycle := range cycles {
		var wg sync.WaitGroup
		var errors int64

		// Fill the map
		for i := range entriesPerCycle {
			m.Store(i, i*7)
		}

		// Concurrent operations while triggering shrink
		wg.Add(numGoroutines)
		for g := range numGoroutines {
			go func(base int) {
				defer wg.Done()

				// Delete entries to trigger shrink
				for i := base; i < entriesPerCycle-100; i += numGoroutines {
					m.Delete(i)
				}

				// Perform reads/writes during shrink
				for i := range 500 {
					key := entriesPerCycle + base*500 + i
					m.Store(key, key*7)
					if val, ok := m.Load(key); !ok || val != key*7 {
						atomic.AddInt64(&errors, 1)
					}
					m.Delete(key)
					runtime.Gosched()
				}
			}(g)
		}

		wg.Wait()

		if errors > 0 {
			t.Errorf("Cycle %d: Found %d errors during shrink stress test", cycle, errors)
		}

		// Verify remaining data
		for i := entriesPerCycle - 100; i < entriesPerCycle; i++ {
			if val, ok := m.Load(i); !ok || val != i*7 {
				t.Errorf("Cycle %d: Data corruption: key=%d, expected=%d, actual=%d, ok=%v",
					cycle, i, i*7, val, ok)
			}
		}

		// Clean up for next cycle
		for i := entriesPerCycle - 100; i < entriesPerCycle; i++ {
			m.Delete(i)
		}
	}
}

func TestFlatMapOf_ShrinkSeqlockConsistency(t *testing.T) {
	m := NewFlatMapOf[int, int](WithShrinkEnabled())
	const numEntries = 6000
	const numGoroutines = 6
	const numOperations = 800

	// Pre-populate
	for i := range numEntries {
		m.Store(i, i*11)
	}

	// Delete most entries to trigger shrink
	for i := range numEntries - 300 {
		m.Delete(i)
	}

	var wg sync.WaitGroup
	var seqlockErrors int64

	// Concurrent readers testing seqlock consistency during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				// Test remaining keys
				key := numEntries - 300 + (base*numOperations+i)%300
				val, ok := m.Load(key)
				if ok && val != key*11 {
					atomic.AddInt64(&seqlockErrors, 1)
				}

				// Test Range consistency
				count := 0
				m.Range(func(k, v int) bool {
					count++
					if v != k*11 {
						atomic.AddInt64(&seqlockErrors, 1)
					}
					return count < 50 // Early termination to reduce test time
				})

				runtime.Gosched()
			}
		}(g)
	}

	// Concurrent writers during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				key := numEntries + base*numOperations + i
				m.Store(key, key*11)

				// Immediate read to test consistency
				if val, ok := m.Load(key); !ok || val != key*11 {
					atomic.AddInt64(&seqlockErrors, 1)
				}

				m.Delete(key)
				runtime.Gosched()
			}
		}(g)
	}

	wg.Wait()

	if seqlockErrors > 0 {
		t.Errorf("Found %d seqlock consistency errors during concurrent shrink", seqlockErrors)
	}

	// Final verification of remaining data
	for i := numEntries - 300; i < numEntries; i++ {
		if val, ok := m.Load(i); !ok || val != i*11 {
			t.Errorf("Final data corruption: key=%d, expected=%d, actual=%d, ok=%v",
				i, i*11, val, ok)
		}
	}
}

// TestFlatMapOf_RangeProcess_DuringResize tests RangeProcess during table
// resize operations
func TestFlatMapOf_RangeProcess_DuringResize(t *testing.T) {
	m := NewFlatMapOf[int, int]()

	// Pre-populate to trigger potential resize
	for i := range 50 {
		m.Store(i, i*2)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Goroutine that continuously adds/removes items to trigger resizes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; ; i++ {
			select {
			case <-stop:
				return
			default:
				m.Store(i, i*2)
				if i%10 == 0 {
					// Delete some keys to potentially trigger shrink
					for j := i - 20; j < i-10 && j >= 50; j++ {
						m.Delete(j)
					}
				}
				runtime.Gosched()
			}
		}
	}()

	// Goroutines that continuously call RangeProcess during resize
	const numRangeProcessors = 4
	processCounts := make([]int64, numRangeProcessors)

	for g := range numRangeProcessors {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			var count int64
			for {
				select {
				case <-stop:
					processCounts[goroutineID] = count
					return
				default:
					m.RangeProcess(
						func(key int, value int) (int, ComputeOp) {
							count++
							// Occasionally update values
							if count%100 == 0 {
								return value + 1, UpdateOp
							}
							return value, CancelOp
						},
					)
					runtime.Gosched()
				}
			}
		}(g)
	}

	// Run for a short duration
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify that RangeProcess operations completed successfully
	totalProcessed := int64(0)
	for i, count := range processCounts {
		if count == 0 {
			t.Errorf("Goroutine %d processed 0 items, expected > 0", i)
		}
		totalProcessed += count
	}

	if totalProcessed == 0 {
		t.Error("No items were processed during resize stress test")
	}

	// Verify map is still consistent
	m.RangeProcess(func(key int, value int) (int, ComputeOp) {
		if value < 0 {
			t.Errorf("Found negative value %d for key %d", value, key)
		}
		return value, CancelOp
	})
}

// TestFlatMapOf_RangeProcess_EarlyTermination tests early termination scenarios
func TestFlatMapOf_RangeProcess_EarlyTermination(t *testing.T) {
	m := NewFlatMapOf[int, int]()

	// Add data
	for i := range 20 {
		m.Store(i, i*5)
	}

	// Test that RangeProcess can handle panics gracefully (if any)
	// and that partial processing doesn't corrupt the map
	processedCount := 0
	m.RangeProcess(func(key int, value int) (int, ComputeOp) {
		processedCount++
		if key == 10 {
			// Simulate early termination by returning without processing
			// In real scenarios, this might be due to context cancellation
			return value, CancelOp
		}
		return value + 1, UpdateOp
	})

	// Verify that the map is still consistent
	consistentCount := 0
	m.RangeProcess(func(key int, value int) (int, ComputeOp) {
		consistentCount++
		if value < 0 {
			t.Errorf("Found inconsistent value %d for key %d", value, key)
		}
		return value, CancelOp
	})

	if consistentCount != 20 {
		t.Errorf("Expected 20 consistent entries, got %d", consistentCount)
	}

	if processedCount == 0 {
		t.Error("Expected some entries to be processed")
	}
}

// TestFlatMapOf_Clear tests the Clear method
func TestFlatMapOf_Clear(t *testing.T) {
	var m FlatMapOf[string, int]

	// Test clear on empty map
	m.Clear()
	if !m.IsZero() {
		t.Error("Expected map to be empty after clearing empty map")
	}
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clearing empty map, got %d", size)
	}

	// Add some data
	for i := range 10 {
		key := fmt.Sprintf("key_%d", i)
		m.Store(key, i*10)
	}

	// Verify data exists
	if size := m.Size(); size != 10 {
		t.Errorf("Expected size 10 before clear, got %d", size)
	}
	if m.IsZero() {
		t.Error("Expected map not to be empty before clear")
	}

	// Clear the map
	m.Clear()

	// Verify map is empty
	if !m.IsZero() {
		t.Error("Expected map to be empty after clear")
	}
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}

	// Verify all keys are gone
	for i := range 10 {
		key := fmt.Sprintf("key_%d", i)
		if val, ok := m.Load(key); ok {
			t.Errorf("Key %s should be gone after clear, but got (%v, %v)", key, val, ok)
		}
	}

	// Verify Range returns no entries
	count := 0
	m.Range(func(k string, v int) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("Expected 0 entries in Range after clear, got %d", count)
	}
}

// TestFlatMapOf_ClearAndReinsert tests clearing and then reinserting data
func TestFlatMapOf_ClearAndReinsert(t *testing.T) {
	m := NewFlatMapOf[int, string]()

	// Insert initial data
	for i := range 20 {
		value := fmt.Sprintf("initial_%d", i)
		m.Store(i, value)
	}
	if size := m.Size(); size != 20 {
		t.Errorf("Expected size 20 after initial insert, got %d", size)
	}

	// Clear the map
	m.Clear()
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}

	// Reinsert different data
	for i := range 15 {
		key := i + 100 // Different keys
		value := fmt.Sprintf("new_%d", i)
		m.Store(key, value)
	}
	if size := m.Size(); size != 15 {
		t.Errorf("Expected size 15 after reinsert, got %d", size)
	}

	// Verify new data is accessible
	for i := range 15 {
		key := i + 100
		expected := fmt.Sprintf("new_%d", i)
		if val, ok := m.Load(key); !ok || val != expected {
			t.Errorf("Key %d: expected (%s, true), got (%v, %v)", key, expected, val, ok)
		}
	}

	// Verify old data is gone
	for i := range 20 {
		if val, ok := m.Load(i); ok {
			t.Errorf("Old key %d should be gone, but got (%v, %v)", i, val, ok)
		}
	}
}

// TestFlatMapOf_ClearLargeMap tests clearing a large map
func TestFlatMapOf_ClearLargeMap(t *testing.T) {
	m := NewFlatMapOf[int, int](WithPresize(2048))

	// Insert a large amount of data
	const N = 1000
	for i := range N {
		m.Store(i, i*2)
	}
	if size := m.Size(); size != N {
		t.Errorf("Expected size %d after insert, got %d", N, size)
	}

	// Clear the map
	m.Clear()

	// Verify it's empty
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}
	if !m.IsZero() {
		t.Error("Expected map to be empty after clear")
	}

	// Spot check that data is gone
	testKeys := []int{0, N / 4, N / 2, 3 * N / 4, N - 1}
	for _, key := range testKeys {
		if val, ok := m.Load(key); ok {
			t.Errorf("Key %d should be gone after clear, but got (%v, %v)", key, val, ok)
		}
	}
}

// TestFlatMapOf_ClearConcurrent tests concurrent access during clear
func TestFlatMapOf_ClearConcurrent(t *testing.T) {
	m := NewFlatMapOf[int, int]()

	// Insert initial data
	const N = 100
	for i := range N {
		m.Store(i, i*2)
	}
	if size := m.Size(); size != N {
		t.Errorf("Expected size %d after insert, got %d", N, size)
	}

	var wg sync.WaitGroup
	var clearDone atomic.Bool

	// Start a goroutine that clears the map
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Let readers start
		m.Clear()
		clearDone.Store(true)
	}()

	// Start readers that try to access data during clear
	numReaders := 4
	for r := range numReaders {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := range 200 {
				key := (readerID*50 + i) % N
				m.Load(key) // May or may not find the key
				if clearDone.Load() {
					break
				}
				time.Sleep(time.Microsecond)
			}
		}(r)
	}

	wg.Wait()

	// Verify map is empty after clear
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after concurrent clear, got %d", size)
	}
	if !m.IsZero() {
		t.Error("Expected map to be empty after concurrent clear")
	}
}

// TestFlatMapOf_ClearConcurrentOperations tests clear with concurrent writes
func TestFlatMapOf_ClearConcurrentOperations(t *testing.T) {
	m := NewFlatMapOf[int, int]()

	// Insert initial data
	const N = 50
	for i := range N {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Start a goroutine that clears the map
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		m.Clear()
		close(stop)
	}()

	// Start writers that try to insert during clear
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 150; i++ {
			select {
			case <-stop:
				return
			default:
				m.Store(i, i*3)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Start readers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range N {
			select {
			case <-stop:
				return
			default:
				m.Load(i)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Wait()

	// The map should be in a consistent state
	// Either completely clear or with some new insertions from the writer
	finalSize := m.Size()

	// Verify consistency - no old entries should remain
	foundOldEntries := false
	for i := range N {
		if _, ok := m.Load(i); ok {
			foundOldEntries = true
			break
		}
	}

	// Old entries should be gone after clear
	if foundOldEntries {
		t.Error("Found old entries that should have been cleared")
	}

	// Any remaining entries should be from the concurrent writer (100-149 range)
	if finalSize > 0 {
		validEntries := 0
		for i := 100; i < 150; i++ {
			if _, ok := m.Load(i); ok {
				validEntries++
			}
		}
		if validEntries != finalSize {
			t.Errorf("Expected all remaining entries to be from concurrent writer, got %d valid out of %d total", validEntries, finalSize)
		}
	}
}

// TestFlatMapOf_ClearMultipleTimes tests clearing multiple times
func TestFlatMapOf_ClearMultipleTimes(t *testing.T) {
	m := NewFlatMapOf[int, int]()

	for round := range 5 {
		// Insert data
		for i := range 20 {
			key := i + round*100
			m.Store(key, i*round)
		}
		if size := m.Size(); size != 20 {
			t.Errorf("Round %d: expected size 20 after insert, got %d", round, size)
		}

		// Clear
		m.Clear()
		if size := m.Size(); size != 0 {
			t.Errorf("Round %d: expected size 0 after clear, got %d", round, size)
		}
		if !m.IsZero() {
			t.Errorf("Round %d: expected map to be empty after clear", round)
		}
	}

	// Final verification
	if size := m.Size(); size != 0 {
		t.Errorf("Expected final size 0, got %d", size)
	}
	if !m.IsZero() {
		t.Error("Expected map to be empty at the end")
	}
}

// TestFlatMapOf_ClearWithShrinkEnabled tests clear with shrink enabled
func TestFlatMapOf_ClearWithShrinkEnabled(t *testing.T) {
	m := NewFlatMapOf[int, int](WithPresize(1024), WithShrinkEnabled())

	// Insert data to grow the map
	const N = 800
	for i := range N {
		m.Store(i, i)
	}
	if size := m.Size(); size != N {
		t.Errorf("Expected size %d after insert, got %d", N, size)
	}

	// Clear the map
	m.Clear()

	// Verify it's empty
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}
	if !m.IsZero() {
		t.Error("Expected map to be empty after clear")
	}

	// The map should still be functional after clear
	for i := range 10 {
		m.Store(i, i*10)
	}
	if size := m.Size(); size != 10 {
		t.Errorf("Expected size 10 after reinsert, got %d", size)
	}

	// Verify data is accessible
	for i := range 10 {
		if val, ok := m.Load(i); !ok || val != i*10 {
			t.Errorf("Key %d: expected (%d, true), got (%v, %v)", i, i*10, val, ok)
		}
	}
}

// TestFlatMapOf_RangeProcess_BlockWriters_Strict tests RangeProcess with
// blockWritersOpt=true under heavy concurrent load to ensure writers are
// properly blocked and no torn reads occur.
func TestFlatMapOf_RangeProcess_BlockWriters_Strict(t *testing.T) {
	type testValue struct {
		X, Y    uint64 // Invariant: Y == ^X
		Counter uint32
	}

	m := NewFlatMapOf[int, testValue]()
	const N = 512

	// Initialize with values maintaining invariant
	for i := range N {
		val := testValue{
			X:       uint64(i * 123456789),
			Y:       ^uint64(i * 123456789),
			Counter: 0,
		}
		m.Store(i, val)
	}

	var (
		wg               sync.WaitGroup
		stop             = make(chan struct{})
		tornReads        atomic.Uint64
		blockedWrites    atomic.Uint64
		rangeProcessRuns atomic.Uint64
	)

	// RangeProcess goroutine with blockWritersOpt=true
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.RangeProcess(func(k int, v testValue) (testValue, ComputeOp) {
					// Verify invariant during processing
					if v.Y != ^v.X {
						tornReads.Add(1)
						t.Errorf("Torn read detected in RangeProcess: key=%d, X=%x, Y=%x", k, v.X, v.Y)
					}
					// Update counter while maintaining invariant
					newV := testValue{
						X:       v.X,
						Y:       v.Y,
						Counter: v.Counter + 1,
					}
					return newV, UpdateOp
				}, BlockWriters) // policyOpt = BlockWriters
				rangeProcessRuns.Add(1)
				runtime.Gosched()
			}
		}
	}()

	// Concurrent writers that should be blocked during RangeProcess
	writerN := 4
	for i := range writerN {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					startTime := time.Now()
					m.Process(key, func(old testValue, loaded bool) (testValue, ComputeOp, testValue, bool) {
						if !loaded {
							return testValue{}, CancelOp, testValue{}, false
						}
						// Check if we were blocked for a significant time
						if time.Since(startTime) > 10*time.Millisecond {
							blockedWrites.Add(1)
						}
						// Maintain invariant while updating
						newV := testValue{
							X:       old.X + uint64(writerID),
							Y:       ^(old.X + uint64(writerID)),
							Counter: old.Counter,
						}
						return newV, UpdateOp, newV, true
					})
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Concurrent readers to detect torn reads
	readerN := 6
	for range readerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					if val, ok := m.Load(key); ok {
						if val.Y != ^val.X {
							tornReads.Add(1)
							t.Errorf("Torn read detected in Load: key=%d, X=%x, Y=%x", key, val.X, val.Y)
						}
					}
					runtime.Gosched()
				}
			}
		}()
	}

	// Run test for a reasonable duration
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify no torn reads occurred
	if torn := tornReads.Load(); torn > 0 {
		t.Fatalf("Detected %d torn reads during test", torn)
	}

	// Verify RangeProcess ran multiple times
	if runs := rangeProcessRuns.Load(); runs == 0 {
		t.Fatal("RangeProcess should have run at least once")
	}

	t.Logf("Test completed: RangeProcess runs=%d, blocked writes=%d",
		rangeProcessRuns.Load(), blockedWrites.Load())
}

// TestFlatMapOf_RangeProcess_AllowWriters_Concurrent tests RangeProcess with
// blockWritersOpt=false to verify concurrent writers are allowed and data
// consistency is maintained.
func TestFlatMapOf_RangeProcess_AllowWriters_Concurrent(t *testing.T) {
	type testValue struct {
		A, B uint64 // Invariant: B == ^A
		Seq  uint32
	}

	m := NewFlatMapOf[int, testValue]()
	const N = 256

	// Initialize map
	for i := range N {
		val := testValue{
			A:   uint64(i * 987654321),
			B:   ^uint64(i * 987654321),
			Seq: 0,
		}
		m.Store(i, val)
	}

	var (
		wg               sync.WaitGroup
		stop             = make(chan struct{})
		tornReads        atomic.Uint64
		concurrentWrites atomic.Uint64
		rangeProcessRuns atomic.Uint64
	)

	// RangeProcess with blockWritersOpt=false (allow concurrent writes)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.RangeProcess(func(k int, v testValue) (testValue, ComputeOp) {
					// Verify invariant
					if v.B != ^v.A {
						tornReads.Add(1)
						t.Errorf("Torn read in RangeProcess: key=%d, A=%x, B=%x", k, v.A, v.B)
					}
					// Increment sequence while maintaining invariant
					newV := testValue{
						A:   v.A,
						B:   v.B,
						Seq: v.Seq + 1,
					}
					return newV, UpdateOp
				}, AllowWriters) // policyOpt = AllowWriters
				rangeProcessRuns.Add(1)
				runtime.Gosched()
			}
		}
	}()

	// Concurrent writers (should NOT be blocked)
	writerN := 6
	for i := range writerN {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					m.Process(key, func(old testValue, loaded bool) (testValue, ComputeOp, testValue, bool) {
						if !loaded {
							return testValue{}, CancelOp, testValue{}, false
						}
						concurrentWrites.Add(1)
						// Update while maintaining invariant
						newA := old.A + uint64(writerID*1000)
						newV := testValue{
							A:   newA,
							B:   ^newA,
							Seq: old.Seq,
						}
						return newV, UpdateOp, newV, true
					})
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Concurrent readers
	readerN := 4
	for range readerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					if val, ok := m.Load(key); ok {
						if val.B != ^val.A {
							tornReads.Add(1)
							t.Errorf("Torn read in Load: key=%d, A=%x, B=%x", key, val.A, val.B)
						}
					}
					runtime.Gosched()
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify no torn reads
	if torn := tornReads.Load(); torn > 0 {
		t.Fatalf("Detected %d torn reads during test", torn)
	}

	// Verify concurrent writes occurred (writers were not blocked)
	if writes := concurrentWrites.Load(); writes == 0 {
		t.Fatal("Expected concurrent writes to occur when blockWritersOpt=false")
	}

	t.Logf("Test completed: RangeProcess runs=%d, concurrent writes=%d",
		rangeProcessRuns.Load(), concurrentWrites.Load())
}

// TestFlatMapOf_RangeProcess_TornReadDetection_Stress performs intensive
// stress testing to detect any torn reads during RangeProcess operations.
func TestFlatMapOf_RangeProcess_TornReadDetection_Stress(t *testing.T) {
	type complexValue struct {
		ID       uint64
		Checksum uint64 // Should equal ^ID
		Data     [4]uint64
		Tail     uint64 // Should equal ID
	}

	m := NewFlatMapOf[int, complexValue]()
	const N = 1024

	// Initialize with complex values maintaining multiple invariants
	for i := range N {
		id := uint64(i * 0x123456789ABCDEF)
		val := complexValue{
			ID:       id,
			Checksum: ^id,
			Data:     [4]uint64{id, id + 1, id + 2, id + 3},
			Tail:     id,
		}
		m.Store(i, val)
	}

	var (
		wg               sync.WaitGroup
		stop             = make(chan struct{})
		tornReads        atomic.Uint64
		validationErrors atomic.Uint64
	)

	// Validation function to check all invariants
	validateValue := func(k int, v complexValue, context string) {
		if v.Checksum != ^v.ID {
			tornReads.Add(1)
			t.Errorf("Torn read in %s: key=%d, ID=%x, Checksum=%x", context, k, v.ID, v.Checksum)
		}
		if v.Tail != v.ID {
			tornReads.Add(1)
			t.Errorf("Torn read in %s: key=%d, ID=%x, Tail=%x", context, k, v.ID, v.Tail)
		}
		for i, d := range v.Data {
			if d != v.ID+uint64(i) {
				validationErrors.Add(1)
				t.Errorf("Data corruption in %s: key=%d, Data[%d]=%x, expected=%x",
					context, k, i, d, v.ID+uint64(i))
			}
		}
	}

	// RangeProcess with blockWritersOpt=true for maximum stress
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.RangeProcess(func(k int, v complexValue) (complexValue, ComputeOp) {
					validateValue(k, v, "RangeProcess")

					// Modify while maintaining invariants
					newID := v.ID + 0x1000
					newV := complexValue{
						ID:       newID,
						Checksum: ^newID,
						Data:     [4]uint64{newID, newID + 1, newID + 2, newID + 3},
						Tail:     newID,
					}
					return newV, UpdateOp
				}, BlockWriters) // policyOpt = BlockWriters
				runtime.Gosched()
			}
		}
	}()

	// Heavy concurrent readers using different access patterns
	readerN := 8
	for r := range readerN {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					switch readerID % 3 {
					case 0:
						// Sequential access
						for i := range N {
							select {
							case <-stop:
								return
							default:
								if val, ok := m.Load(i); ok {
									validateValue(i, val, "Load-Sequential")
								}
							}
						}
					case 1:
						// Random access
						key := rand.IntN(N)
						if val, ok := m.Load(key); ok {
							validateValue(key, val, "Load-Random")
						}
					case 2:
						// Range access
						m.Range(func(k int, v complexValue) bool {
							validateValue(k, v, "Range")
							return true
						})
					}
					runtime.Gosched()
				}
			}
		}(r)
	}

	// Concurrent writers (should be blocked by RangeProcess)
	writerN := 2
	for i := range writerN {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					m.Process(key, func(old complexValue, loaded bool) (complexValue, ComputeOp, complexValue, bool) {
						if !loaded {
							return complexValue{}, CancelOp, complexValue{}, false
						}

						// Create new value maintaining invariants
						newID := old.ID + uint64(writerID*0x10000)
						newV := complexValue{
							ID:       newID,
							Checksum: ^newID,
							Data:     [4]uint64{newID, newID + 1, newID + 2, newID + 3},
							Tail:     newID,
						}
						return newV, UpdateOp, newV, true
					})
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Run stress test
	time.Sleep(1 * time.Second)
	close(stop)
	wg.Wait()

	// Final validation
	if torn := tornReads.Load(); torn > 0 {
		t.Fatalf("Detected %d torn reads during stress test", torn)
	}

	if errors := validationErrors.Load(); errors > 0 {
		t.Fatalf("Detected %d validation errors during stress test", errors)
	}

	t.Log("Stress test completed successfully - no torn reads or validation errors detected")
}

// TestFlatMapOf_RangeProcess_WriterBlocking_Verification verifies that when
// blockWritersOpt=true, writers are actually blocked and cannot proceed
// during RangeProcess execution.
func TestFlatMapOf_RangeProcess_WriterBlocking_Verification(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	const N = 100

	// Initialize map
	for i := range N {
		m.Store(i, i)
	}

	var (
		wg                  sync.WaitGroup
		rangeProcessStarted = make(chan struct{})
		rangeProcessDone    = make(chan struct{})
		writerBlocked       atomic.Bool
		writerCompleted     atomic.Bool
	)

	// RangeProcess that takes some time and blocks writers
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(rangeProcessStarted)

		m.RangeProcess(func(k, v int) (int, ComputeOp) {
			// Simulate some processing time
			time.Sleep(10 * time.Millisecond)
			return v + 1, UpdateOp
		}, BlockWriters) // policyOpt = BlockWriters

		close(rangeProcessDone)
	}()

	// Writer that should be blocked
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for RangeProcess to start
		<-rangeProcessStarted
		time.Sleep(1 * time.Millisecond) // Ensure RangeProcess is running

		startTime := time.Now()
		writerBlocked.Store(true)

		// This should be blocked until RangeProcess completes
		m.Store(0, 999)

		duration := time.Since(startTime)
		writerCompleted.Store(true)

		// Verify we were blocked for a reasonable time
		if duration < 50*time.Millisecond {
			t.Errorf("Writer was not blocked long enough: %v", duration)
		}
	}()

	// Verify writer is blocked while RangeProcess runs
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-rangeProcessStarted
		time.Sleep(5 * time.Millisecond)

		// At this point, writer should be blocked
		if !writerBlocked.Load() {
			t.Error("Writer should be blocked at this point")
		}

		if writerCompleted.Load() {
			t.Error("Writer should not have completed while RangeProcess is running")
		}

		// Wait for RangeProcess to complete
		<-rangeProcessDone

		// Give writer a chance to complete
		time.Sleep(5 * time.Millisecond)

		if !writerCompleted.Load() {
			t.Error("Writer should have completed after RangeProcess finished")
		}
	}()

	wg.Wait()

	// Verify final state
	if val, ok := m.Load(0); !ok || val != 999 {
		t.Errorf("Expected final value 999, got %v (ok=%v)", val, ok)
	}
}
