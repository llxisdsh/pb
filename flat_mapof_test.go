package pb

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestFlatMapOf_BasicOperations tests basic Load and ProcessEntry operations
func TestFlatMapOf_BasicOperations(t *testing.T) {
	m := NewFlatMapOf[string, int]()

	// Test empty map
	if val, ok := m.Load("nonexistent"); ok {
		t.Errorf("Expected not found, got value %v", val)
	}

	// Test insert
	actual, ok := m.ProcessEntry("key1", func(old int, loaded bool) (int, ComputeOp, int, bool) {
		if loaded {
			t.Error("Expected not loaded for new key")
		}
		return 42, UpdateOp, 42, true
	})
	if !ok || actual != 42 {
		t.Errorf("Expected (42, true), got (%v, %v)", actual, ok)
	}

	// Test load after insert
	if val, ok := m.Load("key1"); !ok || val != 42 {
		t.Errorf("Expected (42, true), got (%v, %v)", val, ok)
	}

	// Test update
	actual, ok = m.ProcessEntry("key1", func(old int, loaded bool) (int, ComputeOp, int, bool) {
		if !loaded || old != 42 {
			t.Errorf("Expected loaded=true, old=42, got loaded=%v, old=%v", loaded, old)
		}
		return old + 10, UpdateOp, old + 10, true
	})
	if !ok || actual != 52 {
		t.Errorf("Expected (52, true), got (%v, %v)", actual, ok)
	}

	// Test load after update
	if val, ok := m.Load("key1"); !ok || val != 52 {
		t.Errorf("Expected (52, true), got (%v, %v)", val, ok)
	}

	// Test delete
	actual, ok = m.ProcessEntry("key1", func(old int, loaded bool) (int, ComputeOp, int, bool) {
		if !loaded || old != 52 {
			t.Errorf("Expected loaded=true, old=52, got loaded=%v, old=%v", loaded, old)
		}
		return 0, DeleteOp, old, false
	})
	if ok {
		t.Errorf("Expected ok=false after delete, got ok=%v", ok)
	}

	// Test load after delete
	if val, ok := m.Load("key1"); ok {
		t.Errorf("Expected not found after delete, got (%v, %v)", val, ok)
	}

	// Test cancel operation
	m.ProcessEntry("key2", func(old int, loaded bool) (int, ComputeOp, int, bool) {
		return 100, UpdateOp, 100, true
	})
	actual, ok = m.ProcessEntry("key2", func(old int, loaded bool) (int, ComputeOp, int, bool) {
		return 999, CancelOp, old, loaded
	})
	if !ok || actual != 100 {
		t.Errorf("Expected (100, true) after cancel, got (%v, %v)", actual, ok)
	}
}

// TestFlatMapOf_MultipleKeys tests operations with multiple keys
func TestFlatMapOf_MultipleKeys(t *testing.T) {
	m := NewFlatMapOf[int, string]()

	// Insert multiple keys
	for i := 0; i < 100; i++ {
		m.ProcessEntry(i, func(old string, loaded bool) (string, ComputeOp, string, bool) {
			newV := fmt.Sprintf("value_%d", i)
			return newV, UpdateOp, newV, true
		})
	}

	// Verify all keys
	for i := 0; i < 100; i++ {
		expected := fmt.Sprintf("value_%d", i)
		if val, ok := m.Load(i); !ok || val != expected {
			t.Errorf("Key %d: expected (%s, true), got (%v, %v)", i, expected, val, ok)
		}
	}

	// Delete even keys
	for i := 0; i < 100; i += 2 {
		m.ProcessEntry(i, func(old string, loaded bool) (string, ComputeOp, string, bool) {
			return "", DeleteOp, "", false
		})
	}

	// Verify deletions
	for i := 0; i < 100; i++ {
		val, ok := m.Load(i)
		if i%2 == 0 {
			// Even keys should be deleted
			if ok {
				t.Errorf("Key %d should be deleted, but got (%v, %v)", i, val, ok)
			}
		} else {
			// Odd keys should remain
			expected := fmt.Sprintf("value_%d", i)
			if !ok || val != expected {
				t.Errorf("Key %d: expected (%s, true), got (%v, %v)", i, expected, val, ok)
			}
		}
	}
}

// TestFlatMapOf_Concurrent tests concurrent operations
func TestFlatMapOf_Concurrent(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	const numGoroutines = 10
	const numOpsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writers
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < numOpsPerGoroutine; i++ {
				key := goroutineID*numOpsPerGoroutine + i
				m.ProcessEntry(key, func(old int, loaded bool) (int, ComputeOp, int, bool) {
					return key * 2, UpdateOp, key * 2, true
				})
			}
		}(g)
	}

	// Concurrent readers
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < numOpsPerGoroutine; i++ {
				key := goroutineID*numOpsPerGoroutine + i
				// May or may not find the key depending on timing
				m.Load(key)
			}
		}(g)
	}

	wg.Wait()

	// Verify final state
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < numOpsPerGoroutine; i++ {
			key := g*numOpsPerGoroutine + i
			expected := key * 2
			if val, ok := m.Load(key); !ok || val != expected {
				t.Errorf("Key %d: expected (%d, true), got (%v, %v)", key, expected, val, ok)
			}
		}
	}
}

// TestFlatMapOf_ConcurrentReadWrite tests heavy concurrent read/write load
func TestFlatMapOf_ConcurrentReadWrite(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	const duration = 2 * time.Second
	const numReaders = 8
	const numWriters = 2

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		m.ProcessEntry(i, func(old int, loaded bool) (int, ComputeOp, int, bool) {
			return i, UpdateOp, i, true
		})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.Intn(1000)
					m.Load(key)
				}
			}
		}()
	}

	// Start writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.Intn(1000)
					m.ProcessEntry(key, func(old int, loaded bool) (int, ComputeOp, int, bool) {
						newV := rand.Intn(10000)
						return newV, UpdateOp, newV, true
					})
				}
			}
		}()
	}

	// Run for specified duration
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	t.Log("Concurrent read/write test completed successfully")
}

// TestFlatMapOf_DoubleBufferConsistency tests the double buffer mechanism
func TestFlatMapOf_DoubleBufferConsistency(t *testing.T) {
	m := NewFlatMapOf[int, int]()
	const numKeys = 100
	const numUpdates = 50

	// Insert initial data
	for i := 0; i < numKeys; i++ {
		m.ProcessEntry(i, func(old int, loaded bool) (int, ComputeOp, int, bool) {
			return i, UpdateOp, i, true
		})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Continuous readers to stress test the double buffer
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := 0; i < numKeys; i++ {
						val, ok := m.Load(i)
						if !ok {
							t.Errorf("Key %d should exist", i)
							return
						}
						// Value should be consistent (either old or new, but not corrupted)
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
		for update := 0; update < numUpdates; update++ {
			for i := 0; i < numKeys; i++ {
				m.ProcessEntry(i, func(old int, loaded bool) (int, ComputeOp, int, bool) {
					return old + 1000, UpdateOp, old + 1000, true
				})
			}
			runtime.Gosched() // Give readers a chance
		}
	}()

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	t.Log("Double buffer consistency test completed")
}

// TestFlatMapOf_EdgeCases tests edge cases and error conditions
func TestFlatMapOf_EdgeCases(t *testing.T) {
	m := NewFlatMapOf[string, string]()

	// Test with empty string key
	m.ProcessEntry("", func(old string, loaded bool) (string, ComputeOp, string, bool) {
		return "empty_key_value", UpdateOp, "empty_key_value", true
	})
	if val, ok := m.Load(""); !ok || val != "empty_key_value" {
		t.Errorf("Empty key test failed: got (%v, %v)", val, ok)
	}

	// Test with very long key
	longKey := string(make([]byte, 1000))
	for i := range longKey {
		longKey = longKey[:i] + "a" + longKey[i+1:]
	}
	m.ProcessEntry(longKey, func(old string, loaded bool) (string, ComputeOp, string, bool) {
		return "long_key_value", UpdateOp, "long_key_value", true
	})
	if val, ok := m.Load(longKey); !ok || val != "long_key_value" {
		t.Errorf("Long key test failed: got (%v, %v)", val, ok)
	}

	// Verify data still intact
	if val, ok := m.Load(""); !ok || val != "empty_key_value" {
		t.Errorf("After invalid grow, empty key test failed: got (%v, %v)", val, ok)
	}
}

// TestFlatMapOf_Store tests the Store method
func TestFlatMapOf_Store(t *testing.T) {
	m := NewFlatMapOf[string, int]()

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

// TestFlatMapOf_Delete tests the Delete method
func TestFlatMapOf_Delete(t *testing.T) {
	m := NewFlatMapOf[string, int]()

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

// TestFlatMapOf_LoadOrStore tests the LoadOrStore method
func TestFlatMapOf_LoadOrStore(t *testing.T) {
	m := NewFlatMapOf[string, int]()

	// Test store new key
	actual, loaded := m.LoadOrStore("key1", 100)
	if loaded || actual != 0 {
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

// TestFlatMapOf_Range tests the Range method
func TestFlatMapOf_Range(t *testing.T) {
	m := NewFlatMapOf[int, string]()

	// Test empty map
	count := 0
	m.Range(func(k int, v string) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("Expected 0 iterations on empty map, got %d", count)
	}

	// Add some data
	expected := make(map[int]string)
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("value_%d", i)
		m.Store(i, value)
		expected[i] = value
	}

	// Test full iteration
	found := make(map[int]string)
	m.Range(func(k int, v string) bool {
		found[k] = v
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
	m.Range(func(k int, v string) bool {
		count++
		return count < 5 // Stop after 5 iterations
	})
	if count != 5 {
		t.Errorf("Expected 5 iterations with early termination, got %d", count)
	}
}

//// Go 1.23+ iterator support
//// TestFlatMapOf_All tests the All method (iterator)
//func TestFlatMapOf_All(t *testing.T) {
//	m := NewFlatMapOf[int, string]()
//
//	// Add some data
//	expected := make(map[int]string)
//	for i := 0; i < 5; i++ {
//		value := fmt.Sprintf("value_%d", i)
//		m.Store(i, value)
//		expected[i] = value
//	}
//
//	// Test using range-over-func
//	found := make(map[int]string)
//	for k, v := range m.All() {
//		found[k] = v
//	}
//
//	if len(found) != len(expected) {
//		t.Errorf("Expected %d items, got %d", len(expected), len(found))
//	}
//
//	for k, v := range expected {
//		if foundV, ok := found[k]; !ok || foundV != v {
//			t.Errorf("Key %d: expected %s, got %s (ok=%v)", k, v, foundV, ok)
//		}
//	}
//}

// TestFlatMapOf_Size tests the Size method
func TestFlatMapOf_Size(t *testing.T) {
	m := NewFlatMapOf[int, string]()

	// Test empty map
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 for empty map, got %d", size)
	}

	// Add items and check size
	for i := 0; i < 10; i++ {
		m.Store(i, fmt.Sprintf("value_%d", i))
		expectedSize := i + 1
		if size := m.Size(); size != expectedSize {
			t.Errorf("After storing %d items, expected size %d, got %d", expectedSize, expectedSize, size)
		}
	}

	// Delete items and check size
	for i := 0; i < 5; i++ {
		m.Delete(i)
		expectedSize := 10 - i - 1
		if size := m.Size(); size != expectedSize {
			t.Errorf("After deleting %d items, expected size %d, got %d", i+1, expectedSize, size)
		}
	}
}

// TestFlatMapOf_IsZero tests the IsZero method
func TestFlatMapOf_IsZero(t *testing.T) {
	m := NewFlatMapOf[string, int]()

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
	for i := 0; i < 5; i++ {
		m.Store(fmt.Sprintf("key_%d", i), i)
	}
	if m.IsZero() {
		t.Error("Expected IsZero() to return false for map with multiple items")
	}

	// Delete all items
	for i := 0; i < 5; i++ {
		m.Delete(fmt.Sprintf("key_%d", i))
	}
	if !m.IsZero() {
		t.Error("Expected IsZero() to return true after deleting all items")
	}
}
