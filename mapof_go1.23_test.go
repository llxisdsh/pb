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

// TestMapOf_ProcessAll_OnlyKeys tests ProcessAll with specific keys
func TestMapOf_ProcessAll_OnlyKeys(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)
	m.Store("d", 4)
	m.Store("e", 5)

	// Process only specific keys
	processed := make(map[string]int)
	count := 0

	for e := range m.ProcessAll("a", "c", "e", "nonexistent") {
		processed[e.Key()] = e.Value()
		count++
	}

	// Should only process existing specified keys
	if count != 3 {
		t.Errorf("Expected to process 3 entries, got %d", count)
	}

	expected := map[string]int{"a": 1, "c": 3, "e": 5}
	for k, v := range expected {
		if processed[k] != v {
			t.Errorf("Expected processed[%s] = %d, got %d", k, v, processed[k])
		}
	}

	// Verify non-specified keys were not processed
	if _, exists := processed["b"]; exists {
		t.Error("Key 'b' should not have been processed")
	}
	if _, exists := processed["d"]; exists {
		t.Error("Key 'd' should not have been processed")
	}
}

// TestMapOf_ProcessAll_OnlyKeysUpdate tests updating specific keys
func TestMapOf_ProcessAll_OnlyKeysUpdate(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)
	m.Store("d", 4)

	// Update only specific keys
	for e := range m.ProcessAll("a", "c") {
		e.Update(e.Value() * 10)
	}

	// Verify updates
	if val, ok := m.Load("a"); !ok || val != 10 {
		t.Errorf("Expected a=10, got %d (ok=%v)", val, ok)
	}
	if val, ok := m.Load("c"); !ok || val != 30 {
		t.Errorf("Expected c=30, got %d (ok=%v)", val, ok)
	}

	// Verify non-specified keys remain unchanged
	if val, ok := m.Load("b"); !ok || val != 2 {
		t.Errorf("Expected b=2 (unchanged), got %d (ok=%v)", val, ok)
	}
	if val, ok := m.Load("d"); !ok || val != 4 {
		t.Errorf("Expected d=4 (unchanged), got %d (ok=%v)", val, ok)
	}
}

// TestMapOf_ProcessAll_EmptyMap tests ProcessAll on empty map
func TestMapOf_ProcessAll_EmptyMap(t *testing.T) {
	m := NewMapOf[string, int]()

	count := 0
	for range m.ProcessAll() {
		count++
	}

	if count != 0 {
		t.Errorf("Expected to process 0 entries on empty map, got %d", count)
	}

	// Test with specific keys on empty map
	count = 0
	for range m.ProcessAll("a", "b", "c") {
		count++
	}

	if count != 0 {
		t.Errorf("Expected to process 0 entries with onlyKeys on empty map, got %d", count)
	}
}

// TestMapOf_ProcessAll_SingleEntry tests ProcessAll with single entry
func TestMapOf_ProcessAll_SingleEntry(t *testing.T) {
	m := NewMapOf[string, int]()
	m.Store("single", 42)

	count := 0
	var processedKey string
	var processedValue int

	for e := range m.ProcessAll() {
		processedKey = e.Key()
		processedValue = e.Value()
		count++
	}

	if count != 1 {
		t.Errorf("Expected to process 1 entry, got %d", count)
	}
	if processedKey != "single" {
		t.Errorf("Expected key 'single', got '%s'", processedKey)
	}
	if processedValue != 42 {
		t.Errorf("Expected value 42, got %d", processedValue)
	}
}

// TestMapOf_ProcessAll_NonExistentKeys tests ProcessAll with only non-existent keys
func TestMapOf_ProcessAll_NonExistentKeys(t *testing.T) {
	m := NewMapOf[string, int]()
	m.Store("a", 1)
	m.Store("b", 2)

	count := 0
	for range m.ProcessAll("x", "y", "z") {
		count++
	}

	if count != 0 {
		t.Errorf("Expected to process 0 entries with non-existent keys, got %d", count)
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

// BenchmarkMapOfProcessAll benchmarks ProcessAll with all entries
func BenchmarkMapOfProcessAll(b *testing.B) {
	benchmarkMapOfProcessAll(b, testData[:], nil)
}

// BenchmarkMapOfProcessAllLarge benchmarks ProcessAll with large dataset
func BenchmarkMapOfProcessAllLarge(b *testing.B) {
	benchmarkMapOfProcessAll(b, testDataLarge[:], nil)
}

// BenchmarkMapOfProcessAllOnlyKeys benchmarks ProcessAll with specific keys
func BenchmarkMapOfProcessAllOnlyKeys(b *testing.B) {
	// Use first 10% of keys for onlyKeys parameter
	keys := testData[:len(testData)/10]
	benchmarkMapOfProcessAll(b, testData[:], keys)
}

func benchmarkMapOfProcessAll(b *testing.B, data []string, onlyKeys []string) {
	var m MapOf[string, int]
	// Populate the map
	for i := range data {
		m.LoadOrStore(data[i], i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if onlyKeys != nil {
			for entry := range m.ProcessAll(onlyKeys...) {
				count++
				_ = entry // Prevent optimization
			}
		} else {
			for entry := range m.ProcessAll() {
				count++
				_ = entry // Prevent optimization
			}
		}
	}
}
