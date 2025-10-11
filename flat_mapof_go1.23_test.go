//go:build go1.23

package pb

import (
	"fmt"
	"sync"
	"testing"
)

func TestFlatMapOf_All(t *testing.T) {
	t.Run("EmptyMap", func(t *testing.T) {
		m := NewFlatMapOf[int, string]()
		count := 0

		for k, v := range m.All() {
			count++
			t.Errorf("Unexpected iteration on empty map: key=%d, value=%s", k, v)
		}

		if count != 0 {
			t.Errorf("Expected 0 iterations on empty map, got %d", count)
		}
	})

	t.Run("SingleElement", func(t *testing.T) {
		m := NewFlatMapOf[string, int]()
		m.Store("key1", 100)

		found := make(map[string]int)
		for k, v := range m.All() {
			found[k] = v
		}

		if len(found) != 1 {
			t.Errorf("Expected 1 element, got %d", len(found))
		}

		if found["key1"] != 100 {
			t.Errorf("Expected key1=100, got key1=%d", found["key1"])
		}
	})

	t.Run("MultipleElements", func(t *testing.T) {
		m := NewFlatMapOf[int, string]()
		expected := make(map[int]string)

		// Add test data
		for i := 0; i < 50; i++ {
			value := fmt.Sprintf("value_%d", i)
			m.Store(i, value)
			expected[i] = value
		}

		// Iterate using All
		found := make(map[int]string)
		for k, v := range m.All() {
			found[k] = v
		}

		// Verify all elements are found
		if len(found) != len(expected) {
			t.Errorf("Expected %d elements, got %d", len(expected), len(found))
		}

		for k, expectedV := range expected {
			if foundV, ok := found[k]; !ok || foundV != expectedV {
				t.Errorf("Key %d: expected %s, got %s (ok=%v)", k, expectedV, foundV, ok)
			}
		}
	})

	t.Run("EarlyBreak", func(t *testing.T) {
		m := NewFlatMapOf[int, int]()

		// Add test data
		for i := 0; i < 100; i++ {
			m.Store(i, i*2)
		}

		count := 0
		for k, v := range m.All() {
			count++
			if count >= 10 {
				break // Early termination
			}
			// Verify values are correct
			if v != k*2 {
				t.Errorf("Key %d: expected %d, got %d", k, k*2, v)
			}
		}

		if count != 10 {
			t.Errorf("Expected exactly 10 iterations with early break, got %d", count)
		}
	})

	t.Run("ConcurrentIteration", func(t *testing.T) {
		m := NewFlatMapOf[int, string]()

		// Pre-populate map
		for i := 0; i < 100; i++ {
			m.Store(i, fmt.Sprintf("value_%d", i))
		}

		var wg sync.WaitGroup
		const numGoroutines = 5
		results := make([]map[int]string, numGoroutines)

		// Start multiple goroutines iterating concurrently
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				found := make(map[int]string)
				for k, v := range m.All() {
					found[k] = v
				}
				results[idx] = found
			}(i)
		}

		wg.Wait()

		// Verify all goroutines saw consistent data
		for i, result := range results {
			if len(result) == 0 {
				t.Errorf("Goroutine %d found no elements", i)
				continue
			}

			// Check that values are consistent
			for k, v := range result {
				expected := fmt.Sprintf("value_%d", k)
				if v != expected {
					t.Errorf("Goroutine %d: key %d expected %s, got %s", i, k, expected, v)
				}
			}
		}
	})

	t.Run("IterationWithModification", func(t *testing.T) {
		m := NewFlatMapOf[int, int]()

		// Pre-populate map
		for i := 0; i < 20; i++ {
			m.Store(i, i)
		}

		// Iterate and modify concurrently
		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: Iterate
		go func() {
			defer wg.Done()
			count := 0
			for k, v := range m.All() {
				count++
				// Basic sanity check - values should be non-negative
				if k < 0 || v < 0 {
					t.Errorf("Unexpected negative values: k=%d, v=%d", k, v)
				}
			}
			// Should see some elements (exact count may vary due to concurrent modifications)
			if count == 0 {
				t.Error("Iterator saw no elements during concurrent modification")
			}
		}()

		// Goroutine 2: Modify map
		go func() {
			defer wg.Done()
			for i := 20; i < 40; i++ {
				m.Store(i, i*2)
			}
			// Delete some original elements
			for i := 0; i < 10; i += 2 {
				m.Delete(i)
			}
		}()

		wg.Wait()
	})

	t.Run("PointerValues", func(t *testing.T) {
		m := NewFlatMapOf[string, *int]()

		// Add pointer values
		values := make(map[string]*int)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%d", i)
			val := new(int)
			*val = i * 10
			m.Store(key, val)
			values[key] = val
		}

		// Iterate and verify pointer values
		found := make(map[string]*int)
		for k, v := range m.All() {
			found[k] = v
		}

		if len(found) != len(values) {
			t.Errorf("Expected %d elements, got %d", len(values), len(found))
		}

		for k, expectedPtr := range values {
			if foundPtr, ok := found[k]; !ok {
				t.Errorf("Key %s not found", k)
			} else if foundPtr != expectedPtr {
				t.Errorf("Key %s: pointer mismatch", k)
			} else if *foundPtr != *expectedPtr {
				t.Errorf("Key %s: value mismatch, expected %d, got %d", k, *expectedPtr, *foundPtr)
			}
		}
	})

	t.Run("LargeDataset", func(t *testing.T) {
		m := NewFlatMapOf[int, int]()
		const size = 1000

		// Populate large dataset
		for i := 0; i < size; i++ {
			m.Store(i, i*i)
		}

		// Verify all elements through iteration
		found := make(map[int]int)
		for k, v := range m.All() {
			found[k] = v
		}

		if len(found) != size {
			t.Errorf("Expected %d elements, got %d", size, len(found))
		}

		// Spot check some values
		for i := 0; i < size; i += 100 {
			if found[i] != i*i {
				t.Errorf("Key %d: expected %d, got %d", i, i*i, found[i])
			}
		}
	})
}

// TestFlatMapOf_All_Compatibility tests that All behaves identically to Range
func TestFlatMapOf_All_Compatibility(t *testing.T) {
	m := NewFlatMapOf[string, int]()

	// Add test data
	testData := map[string]int{
		"alpha":   1,
		"beta":    2,
		"gamma":   3,
		"delta":   4,
		"epsilon": 5,
	}

	for k, v := range testData {
		m.Store(k, v)
	}

	// Collect results from Range
	rangeResults := make(map[string]int)
	m.Range(func(k string, v int) bool {
		rangeResults[k] = v
		return true
	})

	// Collect results from All
	allResults := make(map[string]int)
	for k, v := range m.All() {
		allResults[k] = v
	}

	// Compare results
	if len(rangeResults) != len(allResults) {
		t.Errorf("Range and All returned different counts: Range=%d, All=%d",
			len(rangeResults), len(allResults))
	}

	for k, rangeV := range rangeResults {
		if allV, ok := allResults[k]; !ok {
			t.Errorf("Key %s found in Range but not in All", k)
		} else if allV != rangeV {
			t.Errorf("Key %s: Range returned %d, All returned %d", k, rangeV, allV)
		}
	}

	for k, allV := range allResults {
		if rangeV, ok := rangeResults[k]; !ok {
			t.Errorf("Key %s found in All but not in Range", k)
		} else if rangeV != allV {
			t.Errorf("Key %s: All returned %d, Range returned %d", k, allV, rangeV)
		}
	}
}
