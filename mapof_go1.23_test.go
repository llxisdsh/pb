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

// TestMapOf_ProcessSpecified tests ProcessSpecified with specific keys
func TestMapOf_ProcessSpecified(t *testing.T) {
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

	for e := range m.ProcessSpecified("a", "c", "e", "nonexistent") {
		if e.Loaded() {
			processed[e.Key()] = e.Value()
		}
		count++
	}

	// Should process all specified keys (including non-existent)
	if count != 4 {
		t.Errorf("Expected to process 4 entries, got %d", count)
	}

	// Should only have values for existing keys
	if len(processed) != 3 {
		t.Errorf("Expected 3 loaded entries, got %d", len(processed))
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

// TestMapOf_ProcessSpecified_LoadedStatus tests Loaded() status for ProcessSpecified
func TestMapOf_ProcessSpecified_LoadedStatus(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("exists", 42)

	loadedCount := 0
	notLoadedCount := 0

	for e := range m.ProcessSpecified("exists", "notexists1", "notexists2") {
		if e.Loaded() {
			loadedCount++
			if e.Key() != "exists" {
				t.Errorf("Expected loaded key to be 'exists', got '%s'", e.Key())
			}
			if e.Value() != 42 {
				t.Errorf("Expected loaded value to be 42, got %d", e.Value())
			}
		} else {
			notLoadedCount++
			if e.Key() != "notexists1" && e.Key() != "notexists2" {
				t.Errorf("Expected not loaded key to be 'notexists1' or 'notexists2', got '%s'", e.Key())
			}
		}
	}

	if loadedCount != 1 {
		t.Errorf("Expected 1 loaded entry, got %d", loadedCount)
	}
	if notLoadedCount != 2 {
		t.Errorf("Expected 2 not loaded entries, got %d", notLoadedCount)
	}
}

// TestMapOf_ProcessSpecified_OrderGuarantee tests that ProcessSpecified maintains order
func TestMapOf_ProcessSpecified_OrderGuarantee(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("b", 2)
	m.Store("d", 4)

	// Test order preservation
	expectedOrder := []string{"a", "b", "c", "d", "e"}
	actualOrder := make([]string, 0, len(expectedOrder))

	for e := range m.ProcessSpecified(expectedOrder...) {
		actualOrder = append(actualOrder, e.Key())
	}

	if len(actualOrder) != len(expectedOrder) {
		t.Errorf("Expected %d entries, got %d", len(expectedOrder), len(actualOrder))
	}

	for i, expected := range expectedOrder {
		if i >= len(actualOrder) || actualOrder[i] != expected {
			t.Errorf("Expected order[%d] = '%s', got '%s'", i, expected, actualOrder[i])
		}
	}
}

// TestMapOf_ProcessSpecified_InsertNonExistent tests inserting non-existent keys
func TestMapOf_ProcessSpecified_InsertNonExistent(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup existing data
	m.Store("a", 1)

	// Process keys including non-existent ones
	for e := range m.ProcessSpecified("a", "b", "c") {
		if !e.Loaded() {
			// Insert new value for non-existent keys
			e.Update(100)
		} else {
			// Double existing values
			e.Update(e.Value() * 2)
		}
	}

	// Verify results
	if val, ok := m.Load("a"); !ok || val != 2 {
		t.Errorf("Expected a=2, got %d (ok=%v)", val, ok)
	}
	if val, ok := m.Load("b"); !ok || val != 100 {
		t.Errorf("Expected b=100, got %d (ok=%v)", val, ok)
	}
	if val, ok := m.Load("c"); !ok || val != 100 {
		t.Errorf("Expected c=100, got %d (ok=%v)", val, ok)
	}
}

// TestMapOf_ProcessSpecified_DeleteMixed tests deleting both existing and non-existent keys
func TestMapOf_ProcessSpecified_DeleteMixed(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)

	// Delete all specified keys (some exist, some don't)
	for e := range m.ProcessSpecified("a", "b", "nonexistent", "c") {
		e.Delete()
	}

	// Verify all existing keys are deleted
	if _, ok := m.Load("a"); ok {
		t.Error("Expected 'a' to be deleted")
	}
	if _, ok := m.Load("b"); ok {
		t.Error("Expected 'b' to be deleted")
	}
	if _, ok := m.Load("c"); ok {
		t.Error("Expected 'c' to be deleted")
	}
}

// TestMapOf_ProcessSpecified_Update tests updating specific keys
func TestMapOf_ProcessSpecified_Update(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)
	m.Store("d", 4)

	// Update only specific keys
	for e := range m.ProcessSpecified("a", "c") {
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

// TestMapOf_ProcessSpecified_EmptyKeys tests ProcessSpecified with empty key list
func TestMapOf_ProcessSpecified_EmptyKeys(t *testing.T) {
	m := NewMapOf[string, int]()
	m.Store("a", 1)

	count := 0
	for range m.ProcessSpecified() {
		count++
	}

	if count != 0 {
		t.Errorf("Expected to process 0 entries with empty keys, got %d", count)
	}
}

// TestMapOf_ProcessSpecified_DuplicateKeys tests ProcessSpecified with duplicate keys
func TestMapOf_ProcessSpecified_DuplicateKeys(t *testing.T) {
	m := NewMapOf[string, int]()
	m.Store("a", 1)

	processedKeys := make([]string, 0)
	for e := range m.ProcessSpecified("a", "a", "b", "a") {
		processedKeys = append(processedKeys, e.Key())
	}

	// Should process all specified keys including duplicates
	expectedKeys := []string{"a", "a", "b", "a"}
	if len(processedKeys) != len(expectedKeys) {
		t.Errorf("Expected %d processed keys, got %d", len(expectedKeys), len(processedKeys))
	}

	for i, expected := range expectedKeys {
		if i >= len(processedKeys) || processedKeys[i] != expected {
			t.Errorf("Expected processedKeys[%d] = '%s', got '%s'", i, expected, processedKeys[i])
		}
	}
}

// TestMapOf_ProcessSpecified_EarlyTermination tests early termination of ProcessSpecified
func TestMapOf_ProcessSpecified_EarlyTermination(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup test data
	for i := 0; i < 10; i++ {
		m.Store(string(rune('a'+i)), i)
	}

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	count := 0
	for range m.ProcessSpecified(keys...) {
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
	for range m.ProcessSpecified("a", "b", "c") {
		count++
	}

	if count != 3 {
		t.Errorf("Expected to process 3 entries with ProcessSpecified on empty map, got %d", count)
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

// TestMapOf_ProcessSpecified_NonExistentKeys tests ProcessSpecified with only non-existent keys
func TestMapOf_ProcessSpecified_NonExistentKeys(t *testing.T) {
	m := NewMapOf[string, int]()
	m.Store("a", 1)
	m.Store("b", 2)

	count := 0
	loadedCount := 0
	for e := range m.ProcessSpecified("x", "y", "z") {
		count++
		if e.Loaded() {
			loadedCount++
		}
	}

	if count != 3 {
		t.Errorf("Expected to process 3 entries with non-existent keys, got %d", count)
	}
	if loadedCount != 0 {
		t.Errorf("Expected 0 loaded entries with non-existent keys, got %d", loadedCount)
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

// TestMapOf_ProcessSpecified_Concurrent tests ProcessSpecified under concurrent access
func TestMapOf_ProcessSpecified_Concurrent(t *testing.T) {
	m := NewMapOf[int, int]()

	// Setup test data
	for i := 0; i < 100; i++ {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	keys := make([]int, 50)
	for i := range keys {
		keys[i] = i
	}

	processed := make([]int, 2)

	// Run two concurrent ProcessSpecified operations
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			count := 0
			for range m.ProcessSpecified(keys...) {
				count++
			}
			processed[idx] = count
		}(i)
	}

	wg.Wait()

	// Both should process all specified keys
	for i, count := range processed {
		if count != len(keys) {
			t.Errorf(
				"Goroutine %d processed %d entries, expected %d",
				i,
				count,
				len(keys),
			)
		}
	}
}

// BenchmarkMapOfProcessAll benchmarks ProcessAll with all entries
func BenchmarkMapOfProcessAll(b *testing.B) {
	benchmarkMapOfProcessAll(b, testData[:])
}

// BenchmarkMapOfProcessAllLarge benchmarks ProcessAll with large dataset
func BenchmarkMapOfProcessAllLarge(b *testing.B) {
	benchmarkMapOfProcessAll(b, testDataLarge[:])
}

func benchmarkMapOfProcessAll(b *testing.B, data []string) {
	var m MapOf[string, int]
	// Populate the map
	for i := range data {
		m.LoadOrStore(data[i], i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for entry := range m.ProcessAll() {
			count++
			_ = entry // Prevent optimization
		}
	}
}

// BenchmarkMapOfProcessSpecified benchmarks ProcessSpecified with specific keys
func BenchmarkMapOfProcessSpecified(b *testing.B) {
	benchmarkMapOfProcessSpecified(b, testData[:100]) // Use subset for ProcessSpecified
}

// BenchmarkMapOfProcessSpecifiedLarge benchmarks ProcessSpecified with large key set
func BenchmarkMapOfProcessSpecifiedLarge(b *testing.B) {
	benchmarkMapOfProcessSpecified(b, testDataLarge[:1000]) // Use subset for ProcessSpecified
}

func benchmarkMapOfProcessSpecified(b *testing.B, data []string) {
	var m MapOf[string, int]
	// Populate the map
	for i := range data {
		m.LoadOrStore(data[i], i)
	}

	// Use half of the keys for ProcessSpecified (mix of existing and non-existing)
	keys := make([]string, len(data)/2)
	for i := range keys {
		if i%2 == 0 && i/2 < len(data) {
			keys[i] = data[i/2] // existing key
		} else {
			keys[i] = "nonexistent_" + data[i%len(data)] // non-existing key
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for entry := range m.ProcessSpecified(keys...) {
			count++
			_ = entry // Prevent optimization
		}
	}
}
