//go:build go1.23

package pb

import (
	"fmt"
	"math/rand/v2"
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

// TestMapOf_Keys tests the Keys iterator method
func TestMapOf_Keys(t *testing.T) {
	m := NewMapOf[string, int]()

	t.Run("EmptyMap", func(t *testing.T) {
		count := 0
		for key := range m.Keys() {
			count++
			t.Errorf("Unexpected key in empty map: %s", key)
		}
		if count != 0 {
			t.Errorf("Expected 0 keys in empty map, got %d", count)
		}
	})

	t.Run("SingleKey", func(t *testing.T) {
		m.Clear()
		m.Store("single", 42)

		keys := make([]string, 0)
		for key := range m.Keys() {
			keys = append(keys, key)
		}

		if len(keys) != 1 {
			t.Errorf("Expected 1 key, got %d", len(keys))
		}
		if keys[0] != "single" {
			t.Errorf("Expected key 'single', got '%s'", keys[0])
		}
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		m.Clear()
		expectedKeys := map[string]bool{
			"key1": true,
			"key2": true,
			"key3": true,
			"key4": true,
			"key5": true,
		}

		// Store keys with different values
		for key := range expectedKeys {
			m.Store(key, len(key))
		}

		// Collect all keys from iterator
		actualKeys := make(map[string]bool)
		count := 0
		for key := range m.Keys() {
			actualKeys[key] = true
			count++
		}

		if count != len(expectedKeys) {
			t.Errorf("Expected %d keys, got %d", len(expectedKeys), count)
		}

		// Verify all expected keys are present
		for expectedKey := range expectedKeys {
			if !actualKeys[expectedKey] {
				t.Errorf("Expected key '%s' not found in iterator", expectedKey)
			}
		}

		// Verify no unexpected keys
		for actualKey := range actualKeys {
			if !expectedKeys[actualKey] {
				t.Errorf("Unexpected key '%s' found in iterator", actualKey)
			}
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		m.Clear()
		// Store multiple keys
		for i := range 10 {
			m.Store(fmt.Sprintf("key%d", i), i)
		}

		count := 0
		for range m.Keys() {
			count++
			if count >= 3 {
				break // Early termination
			}
		}

		if count != 3 {
			t.Errorf("Expected to process exactly 3 keys before break, got %d", count)
		}
	})

	t.Run("ConcurrentModification", func(t *testing.T) {
		m.Clear()
		// Store initial keys
		for i := range 50 {
			m.Store(fmt.Sprintf("initial_%d", i), i)
		}

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: Iterate over keys
		go func() {
			defer wg.Done()
			count := 0
			for key := range m.Keys() {
				count++
				// Just count, don't fail on specific keys due to concurrent modification
				_ = key
			}
			// We should get at least some keys, but exact count may vary due to concurrency
			if count == 0 {
				t.Error("Expected to iterate over at least some keys")
			}
		}()

		// Goroutine 2: Modify map during iteration
		go func() {
			defer wg.Done()
			for i := range 25 {
				m.Store(fmt.Sprintf("concurrent_%d", i), i+1000)
				if i%5 == 0 {
					m.Delete(fmt.Sprintf("initial_%d", i))
				}
			}
		}()

		wg.Wait()
	})

	t.Run("LargeMap", func(t *testing.T) {
		m.Clear()
		const numKeys = 1000

		// Store many keys
		expectedKeys := make(map[string]bool)
		for i := range numKeys {
			key := fmt.Sprintf("large_%d", i)
			m.Store(key, i)
			expectedKeys[key] = true
		}

		// Iterate and collect all keys
		actualKeys := make(map[string]bool)
		count := 0
		for key := range m.Keys() {
			actualKeys[key] = true
			count++
		}

		if count != numKeys {
			t.Errorf("Expected %d keys, got %d", numKeys, count)
		}

		// Verify all keys are present
		for expectedKey := range expectedKeys {
			if !actualKeys[expectedKey] {
				t.Errorf("Expected key '%s' not found", expectedKey)
			}
		}

		if len(actualKeys) != len(expectedKeys) {
			t.Errorf("Expected %d unique keys, got %d", len(expectedKeys), len(actualKeys))
		}
	})

	t.Run("EmptyStringKeys", func(t *testing.T) {
		m.Clear()
		m.Store("", 0)
		m.Store("non-empty", 1)

		keys := make([]string, 0)
		for key := range m.Keys() {
			keys = append(keys, key)
		}

		if len(keys) != 2 {
			t.Errorf("Expected 2 keys, got %d", len(keys))
		}

		hasEmpty := false
		hasNonEmpty := false
		for _, key := range keys {
			switch key {
			case "":
				hasEmpty = true
			case "non-empty":
				hasNonEmpty = true
			}
		}

		if !hasEmpty {
			t.Error("Expected empty string key to be present")
		}
		if !hasNonEmpty {
			t.Error("Expected non-empty string key to be present")
		}
	})

	t.Run("SpecialCharacterKeys", func(t *testing.T) {
		m.Clear()
		specialKeys := []string{
			"key with spaces",
			"key\twith\ttabs",
			"key\nwith\nnewlines",
			"key\"with\"quotes",
			"key'with'apostrophes",
			"key\\with\\backslashes",
			"key/with/slashes",
			"key.with.dots",
			"key-with-dashes",
			"key_with_underscores",
		}

		// Store all special keys
		for i, key := range specialKeys {
			m.Store(key, i)
		}

		// Collect keys from iterator
		actualKeys := make(map[string]bool)
		for key := range m.Keys() {
			actualKeys[key] = true
		}

		if len(actualKeys) != len(specialKeys) {
			t.Errorf("Expected %d keys, got %d", len(specialKeys), len(actualKeys))
		}

		// Verify all special keys are present
		for _, expectedKey := range specialKeys {
			if !actualKeys[expectedKey] {
				t.Errorf("Expected special key '%s' not found", expectedKey)
			}
		}
	})

	t.Run("UnicodeKeys", func(t *testing.T) {
		m.Clear()
		unicodeKeys := []string{
			"键",        // Chinese
			"キー",       // Japanese
			"키",        // Korean
			"ключ",     // Russian
			"مفتاح",    // Arabic
			"🔑",        // Emoji
			"café",     // Accented characters
			"naïve",    // More accented characters
			"Ελληνικά", // Greek
			"עברית",    // Hebrew
		}

		// Store all Unicode keys
		for i, key := range unicodeKeys {
			m.Store(key, i)
		}

		// Collect keys from iterator
		actualKeys := make(map[string]bool)
		for key := range m.Keys() {
			actualKeys[key] = true
		}

		if len(actualKeys) != len(unicodeKeys) {
			t.Errorf("Expected %d keys, got %d", len(unicodeKeys), len(actualKeys))
		}

		// Verify all Unicode keys are present
		for _, expectedKey := range unicodeKeys {
			if !actualKeys[expectedKey] {
				t.Errorf("Expected unicode key '%s' not found", expectedKey)
			}
		}
	})

	t.Run("StoreDeletePattern", func(t *testing.T) {
		m.Clear()

		// Store some keys
		m.Store("persistent1", 1)
		m.Store("persistent2", 2)
		m.Store("temporary1", 3)
		m.Store("temporary2", 4)

		// Delete some keys
		m.Delete("temporary1")
		m.Delete("temporary2")

		// Collect remaining keys
		actualKeys := make(map[string]bool)
		for key := range m.Keys() {
			actualKeys[key] = true
		}

		if len(actualKeys) != 2 {
			t.Errorf("Expected 2 remaining keys, got %d", len(actualKeys))
		}

		if !actualKeys["persistent1"] {
			t.Error("Expected 'persistent1' to remain")
		}
		if !actualKeys["persistent2"] {
			t.Error("Expected 'persistent2' to remain")
		}
		if actualKeys["temporary1"] {
			t.Error("Expected 'temporary1' to be deleted")
		}
		if actualKeys["temporary2"] {
			t.Error("Expected 'temporary2' to be deleted")
		}
	})

	t.Run("IteratorIndependence", func(t *testing.T) {
		m.Clear()
		// Store test keys
		for i := range 10 {
			m.Store(fmt.Sprintf("key%d", i), i)
		}

		// Create two independent iterators
		iter1 := m.Keys()
		iter2 := m.Keys()

		// Consume first iterator partially
		count1 := 0
		for key := range iter1 {
			count1++
			_ = key
			if count1 >= 5 {
				break
			}
		}

		// Consume second iterator completely
		count2 := 0
		for key := range iter2 {
			count2++
			_ = key
		}

		// Second iterator should see all keys regardless of first iterator state
		if count2 != 10 {
			t.Errorf("Expected second iterator to see all 10 keys, got %d", count2)
		}
	})
}

// TestMapOf_Values tests the Values iterator method
func TestMapOf_Values(t *testing.T) {
	m := NewMapOf[string, int]()

	t.Run("EmptyMap", func(t *testing.T) {
		count := 0
		for value := range m.Values() {
			count++
			t.Errorf("Unexpected value in empty map: %d", value)
		}
		if count != 0 {
			t.Errorf("Expected 0 values in empty map, got %d", count)
		}
	})

	t.Run("SingleValue", func(t *testing.T) {
		m.Clear()
		m.Store("single", 42)

		values := make([]int, 0)
		for value := range m.Values() {
			values = append(values, value)
		}

		if len(values) != 1 {
			t.Errorf("Expected 1 value, got %d", len(values))
		}
		if values[0] != 42 {
			t.Errorf("Expected value 42, got %d", values[0])
		}
	})

	t.Run("MultipleValues", func(t *testing.T) {
		m.Clear()
		expectedValues := map[int]bool{
			10: true,
			20: true,
			30: true,
			40: true,
			50: true,
		}

		// Store values with different keys
		i := 0
		for value := range expectedValues {
			m.Store(fmt.Sprintf("key%d", i), value)
			i++
		}

		// Collect all values from iterator
		actualValues := make(map[int]bool)
		count := 0
		for value := range m.Values() {
			actualValues[value] = true
			count++
		}

		if count != len(expectedValues) {
			t.Errorf("Expected %d values, got %d", len(expectedValues), count)
		}

		// Verify all expected values are present
		for expectedValue := range expectedValues {
			if !actualValues[expectedValue] {
				t.Errorf("Expected value %d not found in iterator", expectedValue)
			}
		}

		// Verify no unexpected values
		for actualValue := range actualValues {
			if !expectedValues[actualValue] {
				t.Errorf("Unexpected value %d found in iterator", actualValue)
			}
		}
	})

	t.Run("DuplicateValues", func(t *testing.T) {
		m.Clear()
		// Store same value with different keys
		m.Store("key1", 100)
		m.Store("key2", 100)
		m.Store("key3", 100)
		m.Store("key4", 200)

		values := make([]int, 0)
		for value := range m.Values() {
			values = append(values, value)
		}

		if len(values) != 4 {
			t.Errorf("Expected 4 values, got %d", len(values))
		}

		// Count occurrences
		count100 := 0
		count200 := 0
		for _, value := range values {
			switch value {
			case 100:
				count100++
			case 200:
				count200++
			default:
				t.Errorf("Unexpected value: %d", value)
			}
		}

		if count100 != 3 {
			t.Errorf("Expected 3 occurrences of value 100, got %d", count100)
		}
		if count200 != 1 {
			t.Errorf("Expected 1 occurrence of value 200, got %d", count200)
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		m.Clear()
		// Store multiple values
		for i := range 10 {
			m.Store(fmt.Sprintf("key%d", i), i*10)
		}

		count := 0
		for value := range m.Values() {
			count++
			_ = value
			if count >= 3 {
				break // Early termination
			}
		}

		if count != 3 {
			t.Errorf("Expected to process exactly 3 values before break, got %d", count)
		}
	})

	t.Run("ConcurrentModification", func(t *testing.T) {
		m.Clear()
		// Store initial values
		for i := range 50 {
			m.Store(fmt.Sprintf("initial_%d", i), i*100)
		}

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: Iterate over values
		go func() {
			defer wg.Done()
			count := 0
			for value := range m.Values() {
				count++
				// Just count, don't fail on specific values due to concurrent modification
				_ = value
			}
			// We should get at least some values, but exact count may vary due to concurrency
			if count == 0 {
				t.Error("Expected to iterate over at least some values")
			}
		}()

		// Goroutine 2: Modify map during iteration
		go func() {
			defer wg.Done()
			for i := range 25 {
				m.Store(fmt.Sprintf("concurrent_%d", i), i+5000)
				if i%5 == 0 {
					m.Delete(fmt.Sprintf("initial_%d", i))
				}
			}
		}()

		wg.Wait()
	})

	t.Run("LargeMap", func(t *testing.T) {
		m.Clear()
		const numValues = 1000

		// Store many values
		expectedValues := make(map[int]bool)
		for i := range numValues {
			value := i * 7 // Use a multiplier to create distinct values
			m.Store(fmt.Sprintf("large_%d", i), value)
			expectedValues[value] = true
		}

		// Iterate and collect all values
		actualValues := make(map[int]bool)
		count := 0
		for value := range m.Values() {
			actualValues[value] = true
			count++
		}

		if count != numValues {
			t.Errorf("Expected %d values, got %d", numValues, count)
		}

		// Verify all values are present
		for expectedValue := range expectedValues {
			if !actualValues[expectedValue] {
				t.Errorf("Expected value %d not found", expectedValue)
			}
		}

		if len(actualValues) != len(expectedValues) {
			t.Errorf("Expected %d unique values, got %d", len(expectedValues), len(actualValues))
		}
	})

	t.Run("ZeroValues", func(t *testing.T) {
		m.Clear()
		m.Store("zero1", 0)
		m.Store("zero2", 0)
		m.Store("nonzero", 42)

		values := make([]int, 0)
		for value := range m.Values() {
			values = append(values, value)
		}

		if len(values) != 3 {
			t.Errorf("Expected 3 values, got %d", len(values))
		}

		zeroCount := 0
		nonZeroCount := 0
		for _, value := range values {
			switch value {
			case 0:
				zeroCount++
			case 42:
				nonZeroCount++
			default:
				t.Errorf("Unexpected value: %d", value)
			}
		}

		if zeroCount != 2 {
			t.Errorf("Expected 2 zero values, got %d", zeroCount)
		}
		if nonZeroCount != 1 {
			t.Errorf("Expected 1 non-zero value, got %d", nonZeroCount)
		}
	})

	t.Run("NegativeValues", func(t *testing.T) {
		m.Clear()
		negativeValues := []int{-100, -50, -1, 0, 1, 50, 100}

		// Store all values
		for i, value := range negativeValues {
			m.Store(fmt.Sprintf("key%d", i), value)
		}

		// Collect values from iterator
		actualValues := make(map[int]bool)
		for value := range m.Values() {
			actualValues[value] = true
		}

		if len(actualValues) != len(negativeValues) {
			t.Errorf("Expected %d values, got %d", len(negativeValues), len(actualValues))
		}

		// Verify all values are present
		for _, expectedValue := range negativeValues {
			if !actualValues[expectedValue] {
				t.Errorf("Expected value %d not found", expectedValue)
			}
		}
	})

	t.Run("LargeValues", func(t *testing.T) {
		m.Clear()
		largeValues := []int{
			1000000,
			2147483647,  // Max int32
			-2147483648, // Min int32
			999999999,
			-999999999,
		}

		// Store all large values
		for i, value := range largeValues {
			m.Store(fmt.Sprintf("large_key%d", i), value)
		}

		// Collect values from iterator
		actualValues := make(map[int]bool)
		for value := range m.Values() {
			actualValues[value] = true
		}

		if len(actualValues) != len(largeValues) {
			t.Errorf("Expected %d values, got %d", len(largeValues), len(actualValues))
		}

		// Verify all large values are present
		for _, expectedValue := range largeValues {
			if !actualValues[expectedValue] {
				t.Errorf("Expected large value %d not found", expectedValue)
			}
		}
	})

	t.Run("StoreDeletePattern", func(t *testing.T) {
		m.Clear()

		// Store some values
		m.Store("persistent1", 111)
		m.Store("persistent2", 222)
		m.Store("temporary1", 333)
		m.Store("temporary2", 444)

		// Delete some entries
		m.Delete("temporary1")
		m.Delete("temporary2")

		// Collect remaining values
		actualValues := make(map[int]bool)
		for value := range m.Values() {
			actualValues[value] = true
		}

		if len(actualValues) != 2 {
			t.Errorf("Expected 2 remaining values, got %d", len(actualValues))
		}

		if !actualValues[111] {
			t.Error("Expected value 111 to remain")
		}
		if !actualValues[222] {
			t.Error("Expected value 222 to remain")
		}
		if actualValues[333] {
			t.Error("Expected value 333 to be deleted")
		}
		if actualValues[444] {
			t.Error("Expected value 444 to be deleted")
		}
	})

	t.Run("ValueUpdatePattern", func(t *testing.T) {
		m.Clear()

		// Store initial values
		m.Store("key1", 100)
		m.Store("key2", 200)
		m.Store("key3", 300)

		// Update some values
		m.Store("key1", 1000) // Update existing
		m.Store("key2", 2000) // Update existing

		// Collect current values
		actualValues := make(map[int]bool)
		for value := range m.Values() {
			actualValues[value] = true
		}

		if len(actualValues) != 3 {
			t.Errorf("Expected 3 values, got %d", len(actualValues))
		}

		// Should have updated values, not original ones
		if !actualValues[1000] {
			t.Error("Expected updated value 1000 to be present")
		}
		if !actualValues[2000] {
			t.Error("Expected updated value 2000 to be present")
		}
		if !actualValues[300] {
			t.Error("Expected unchanged value 300 to be present")
		}
		if actualValues[100] {
			t.Error("Expected original value 100 to be replaced")
		}
		if actualValues[200] {
			t.Error("Expected original value 200 to be replaced")
		}
	})

	t.Run("IteratorIndependence", func(t *testing.T) {
		m.Clear()
		// Store test values
		for i := range 10 {
			m.Store(fmt.Sprintf("key%d", i), i*100)
		}

		// Create two independent iterators
		iter1 := m.Values()
		iter2 := m.Values()

		// Consume first iterator partially
		count1 := 0
		for value := range iter1 {
			count1++
			_ = value
			if count1 >= 5 {
				break
			}
		}

		// Consume second iterator completely
		count2 := 0
		for value := range iter2 {
			count2++
			_ = value
		}

		// Second iterator should see all values regardless of first iterator state
		if count2 != 10 {
			t.Errorf("Expected second iterator to see all 10 values, got %d", count2)
		}
	})

	t.Run("ValueOrderConsistency", func(t *testing.T) {
		m.Clear()
		// Store values in a specific order
		keys := []string{"a", "b", "c", "d", "e"}
		values := []int{10, 20, 30, 40, 50}

		for i, key := range keys {
			m.Store(key, values[i])
		}

		// Collect values multiple times
		var collections [][]int
		for range 3 {
			var currentCollection []int
			for value := range m.Values() {
				currentCollection = append(currentCollection, value)
			}
			collections = append(collections, currentCollection)
		}

		// All collections should have the same length
		for i, collection := range collections {
			if len(collection) != len(values) {
				t.Errorf("Collection %d: expected %d values, got %d", i, len(values), len(collection))
			}
		}

		// While order might not be guaranteed, the same set of values should be present
		for i, collection := range collections {
			collectionMap := make(map[int]bool)
			for _, value := range collection {
				collectionMap[value] = true
			}

			for _, expectedValue := range values {
				if !collectionMap[expectedValue] {
					t.Errorf("Collection %d: expected value %d not found", i, expectedValue)
				}
			}
		}
	})
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
	for i := range 10 {
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
	for i := range 10 {
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
	for i := range 100 {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	processed := make([]int, 2)

	// Run two concurrent ProcessAll operations
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			count := 0
			for range m.ProcessAll(rand.IntN(2) == 0) {
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
	for i := range 100 {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	keys := make([]int, 50)
	for i := range keys {
		keys[i] = i
	}

	processed := make([]int, 2)

	// Run two concurrent ProcessSpecified operations
	for i := range 2 {
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
	for range b.N {
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
	for range b.N {
		count := 0
		for entry := range m.ProcessSpecified(keys...) {
			count++
			_ = entry // Prevent optimization
		}
	}
}
