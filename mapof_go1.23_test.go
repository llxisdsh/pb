//go:build go1.23

package pb

import (
	"sync"
	"testing"
)

// TestMapOf_ProcessAll_Basic tests basic functionality of ProcessAll method
func TestMapOf_ProcessAll_Basic(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)
	m.Store("d", 4)

	// Track processed entries
	processed := make(map[string]int)
	count := 0

	// Process all entries
	for e := range m.ProcessAll() {
		processed[e.Key()] = e.Value()
		count++
	}

	// Verify all entries were processed
	if count != 4 {
		t.Errorf("Expected to process 4 entries, got %d", count)
	}

	expected := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	for k, v := range expected {
		if processed[k] != v {
			t.Errorf("Expected processed[%s] = %d, got %d", k, v, processed[k])
		}
	}
}

// TestMapOf_ProcessAll_EarlyTermination tests early termination of ProcessAll
func TestMapOf_ProcessAll_EarlyTermination(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	for i := 0; i < 10; i++ {
		m.Store(string(rune('a'+i)), i)
	}

	count := 0
	for range m.ProcessAll() {
		count++
		// Stop after processing 3 entries
		if count >= 3 {
			break
		}
	}

	if count != 3 {
		t.Errorf("Expected to process exactly 3 entries, got %d", count)
	}
}

// TestMapOf_ProcessAll_Update tests updating entries during ProcessAll
func TestMapOf_ProcessAll_Update(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)

	// Double all values
	for e := range m.ProcessAll() {
		e.Update(e.Value() * 2)
	}

	// Verify updates
	expected := map[string]int{"a": 2, "b": 4, "c": 6}
	for k, expectedVal := range expected {
		if val, ok := m.Load(k); !ok || val != expectedVal {
			t.Errorf(
				"Expected m[%s] = %d, got %d (ok=%v)",
				k,
				expectedVal,
				val,
				ok,
			)
		}
	}
}

// TestMapOf_ProcessAll_Delete tests deleting entries during ProcessAll
func TestMapOf_ProcessAll_Delete(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)
	m.Store("d", 4)

	// Delete even values
	for e := range m.ProcessAll() {
		if e.Value()%2 == 0 {
			e.Delete()
		}
	}

	// Verify deletions
	if _, ok := m.Load("b"); ok {
		t.Error("Expected 'b' to be deleted")
	}
	if _, ok := m.Load("d"); ok {
		t.Error("Expected 'd' to be deleted")
	}

	// Verify remaining entries
	if val, ok := m.Load("a"); !ok || val != 1 {
		t.Errorf("Expected 'a' to remain with value 1, got %d (ok=%v)", val, ok)
	}
	if val, ok := m.Load("c"); !ok || val != 3 {
		t.Errorf("Expected 'c' to remain with value 3, got %d (ok=%v)", val, ok)
	}
}

// TestMapOf_ProcessAll_Concurrent tests ProcessAll under concurrent access
func TestMapOf_ProcessAll_Concurrent(t *testing.T) {
	m := NewMapOf[int, int]()

	// Setup test data
	for i := 0; i < 100; i++ {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	processed := make([]int, 2)

	// Run two concurrent ProcessAll operations
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			count := 0
			for range m.ProcessAll() {
				count++
			}
			processed[idx] = count
		}(i)
	}

	wg.Wait()

	// Both should process all entries (though some might be deleted by the
	// other)
	for i, count := range processed {
		if count <= 0 {
			t.Errorf(
				"Goroutine %d processed %d entries, expected > 0",
				i,
				count,
			)
		}
	}
}
