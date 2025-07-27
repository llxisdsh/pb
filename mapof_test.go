package pb

import (
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

var (
	testDataSmall [8]string
	testData      [128]string
	testDataLarge [128 << 10]string

	testDataIntSmall [8]int
	testDataInt      [128]int
	testDataIntLarge [128 << 10]int
)

func init() {
	for i := range testDataSmall {
		testDataSmall[i] = fmt.Sprintf("%b", i)
	}
	for i := range testData {
		testData[i] = fmt.Sprintf("%b", i)
	}
	for i := range testDataLarge {
		testDataLarge[i] = fmt.Sprintf("%b", i)
	}

	for i := range testDataIntSmall {
		testDataIntSmall[i] = i
	}
	for i := range testData {
		testDataInt[i] = i
	}
	for i := range testDataIntLarge {
		testDataIntLarge[i] = i
	}
}

type structKey struct {
	Service  uint32
	Instance uint64
}

func TestMap_BucketOfStructSize(t *testing.T) {
	t.Logf("CacheLineSize : %d", CacheLineSize)
	t.Logf("entriesPerMapOfBucket : %d", entriesPerMapOfBucket)
	t.Log("resizeState size:", unsafe.Sizeof(resizeState{}))

	size := unsafe.Sizeof(counterStripe{})
	t.Log("counterStripe size:", size)
	if //goland:noinspection GoBoolExpressions
	enablePadding && size != CacheLineSize {
		t.Fatalf("counterStripe doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(bucketOf{})
	t.Log("bucketOf size:", size)
	if size != CacheLineSize {
		t.Fatalf("bucketOf doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(mapOfTable{})
	t.Log("mapOfTable size:", size)
	if size != CacheLineSize {
		t.Fatalf("mapOfTable doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(MapOf[string, int]{})
	t.Log("MapOf size:", size)
	if size != CacheLineSize {
		t.Fatalf("MapOf doesn't meet CacheLineSize: %d", size)
	}

	structType := reflect.TypeOf(bucketOf{})
	t.Logf("Struct bucketOf: %s", structType.Name())
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name
		fieldType := field.Type
		fieldOffset := field.Offset
		fieldSize := fieldType.Size()

		t.Logf("Field: %-10s Type: %-10s Offset: %d Size: %d bytes\n",
			fieldName, fieldType, fieldOffset, fieldSize)
	}

	structType = reflect.TypeOf(mapOfTable{})
	t.Logf("Struct mapOfTable: %s", structType.Name())
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name
		fieldType := field.Type
		fieldOffset := field.Offset
		fieldSize := fieldType.Size()

		t.Logf("Field: %-10s Type: %-10s Offset: %d Size: %d bytes\n",
			fieldName, fieldType, fieldOffset, fieldSize)
	}

	structType = reflect.TypeOf(MapOf[string, int]{})
	t.Logf("Struct MapOf: %s", structType.Name())
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

// TestMapOfStoreLoadLatency tests the latency between Store and Load operations
func TestMapOfStoreLoadLatency(t *testing.T) {
	const (
		iterations   = 100000 // Number of samples
		warmupRounds = 1000   // Warmup iterations
	)

	// Define percentiles to report
	reportPercentiles := []float64{50, 90, 99, 99.9, 99.99, 100}

	m := NewMapOf[string, int64]()

	// Channels for synchronization
	var wg sync.WaitGroup
	startCh := make(chan struct{})
	readyCh := make(chan struct{}, 1) // Buffered to prevent blocking
	doneCh := make(chan struct{})

	// Record latency data
	latencies := make([]time.Duration, 0, iterations)
	var successCount, failureCount int64

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for start signal
		<-startCh

		var lastValue int64
		for {
			select {
			case <-doneCh:
				return
			case <-readyCh:
				// Record start time
				startTime := time.Now()
				success := false

				// Try to read until success or timeout
				timeout := time.After(10 * time.Millisecond)
				for !success {
					select {
					case <-timeout:
						// Timeout, record failure
						atomic.AddInt64(&failureCount, 1)
						success = true // Exit loop
					default:
						value, ok := m.Load("test-key")
						if ok && value > lastValue {
							// Read success and value updated
							latency := time.Since(startTime)
							latencies = append(latencies, latency)
							lastValue = value
							atomic.AddInt64(&successCount, 1)
							success = true
						}
					}
				}
			}
		}
	}()

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(doneCh)

		// Send start signal
		close(startCh)

		// Warmup phase
		for i := 0; i < warmupRounds; i++ {
			m.Store("test-key", int64(i))
			readyCh <- struct{}{}
		}

		// Actual test
		for i := warmupRounds; i < warmupRounds+iterations; i++ {
			// Write new value
			m.Store("test-key", int64(i))

			// Notify reader goroutine
			readyCh <- struct{}{}
		}
	}()

	// Wait for test completion
	wg.Wait()

	// Analyze results
	if len(latencies) == 0 {
		t.Fatal("No latency data collected")
	}

	// Sort latency data for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate statistics
	var sum time.Duration
	for _, latency := range latencies {
		sum += latency
	}
	avgLatency := sum / time.Duration(len(latencies))

	// Calculate standard deviation
	var variance float64
	for _, latency := range latencies {
		diff := float64(latency - avgLatency)
		variance += diff * diff
	}
	variance /= float64(len(latencies))
	stdDev := time.Duration(math.Sqrt(variance))

	// Output results
	t.Logf("Store-Load Latency Statistics (samples: %d):", len(latencies))
	t.Logf("  Success rate: %.2f%% (%d/%d)",
		float64(successCount)*100/float64(successCount+failureCount),
		successCount, successCount+failureCount)
	t.Logf("  Average latency: %v", avgLatency)
	t.Logf("  Standard deviation: %v", stdDev)
	t.Logf("  Min latency: %v", latencies[0])
	t.Logf("  Max latency: %v", latencies[len(latencies)-1])

	// Output percentiles
	for _, p := range reportPercentiles {
		idx := int(float64(len(latencies)-1) * p / 100)
		t.Logf("  %v percentile: %v", p, latencies[idx])
	}

	// Output latency distribution
	buckets := []time.Duration{
		1 * time.Nanosecond,
		10 * time.Nanosecond,
		100 * time.Nanosecond,
		1 * time.Microsecond,
		10 * time.Microsecond,
		100 * time.Microsecond,
		1 * time.Millisecond,
		10 * time.Millisecond,
	}

	counts := make([]int, len(buckets)+1)
	for _, latency := range latencies {
		i := 0
		for ; i < len(buckets); i++ {
			if latency < buckets[i] {
				break
			}
		}
		counts[i]++
	}

	t.Log("Latency distribution:")
	for i := 0; i < len(buckets); i++ {
		var rangeStr string
		if i == 0 {
			rangeStr = fmt.Sprintf("< %v", buckets[i])
		} else {
			rangeStr = fmt.Sprintf("%v - %v", buckets[i-1], buckets[i])
		}
		percentage := float64(counts[i]) * 100 / float64(len(latencies))
		t.Logf("  %s: %d (%.2f%%)", rangeStr, counts[i], percentage)
	}

	if counts[len(counts)-1] > 0 {
		percentage := float64(counts[len(counts)-1]) * 100 / float64(len(latencies))
		t.Logf("  >= %v: %d (%.2f%%)", buckets[len(buckets)-1], counts[len(counts)-1], percentage)
	}
}

// TestMapOfStoreLoadMultiThreadLatency tests Store-Load latency in a multi-threaded environment
func TestMapOfStoreLoadMultiThreadLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-thread latency test in short mode")
	}

	const (
		iterations   = 10000 // Iterations per writer thread
		writerCount  = 4     // Number of writer threads
		readerCount  = 16    // Number of reader threads
		keyCount     = 100   // Number of keys
		warmupRounds = 1000  // Warmup iterations
	)

	// Define percentiles to report
	percentiles := []float64{50, 90, 99, 99.9, 99.99, 100}

	m := NewMapOf[int, int64]()

	// Synchronization variables
	var wg sync.WaitGroup
	startCh := make(chan struct{})
	doneCh := make(chan struct{})

	// Record latency data
	var latencyLock sync.Mutex
	latencies := make([]time.Duration, 0, writerCount*iterations)

	// Track latest values for each key
	latestValues := make([]atomic.Int64, keyCount)

	// Start reader threads
	for r := 0; r < readerCount; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Wait for start signal
			<-startCh

			localLatestValues := make([]int64, keyCount)

			for {
				select {
				case <-doneCh:
					return
				default:
					// Select a key based on reader ID
					keyIdx := readerID % keyCount

					// Read value
					value, ok := m.Load(keyIdx)
					if ok && value > localLatestValues[keyIdx] {
						// Update local record of latest value
						localLatestValues[keyIdx] = value
					}
				}
			}
		}(r)
	}

	// Start writer threads
	for w := 0; w < writerCount; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			// Wait for start signal
			<-startCh

			// Determine key range for this writer
			keysPerWriter := keyCount / writerCount
			startKey := writerID * keysPerWriter
			endKey := (writerID + 1) * keysPerWriter
			if writerID == writerCount-1 {
				endKey = keyCount // Last thread handles remaining keys
			}

			// Warmup phase
			for i := 0; i < warmupRounds; i++ {
				for key := startKey; key < endKey; key++ {
					newValue := int64(i + 1)
					m.Store(key, newValue)
					latestValues[key].Store(newValue)
				}
			}

			// Actual test
			for i := 0; i < iterations; i++ {
				for key := startKey; key < endKey; key++ {
					// Write new value
					startTime := time.Now()
					newValue := latestValues[key].Load() + 1
					m.Store(key, newValue)

					// Update latest value record
					latestValues[key].Store(newValue)

					// Record latency
					latency := time.Since(startTime)
					latencyLock.Lock()
					latencies = append(latencies, latency)
					latencyLock.Unlock()
				}
			}
		}(w)
	}

	// Start test
	close(startCh)

	// Wait for a reasonable time then end test
	time.Sleep(time.Duration(iterations/100) * time.Millisecond)
	close(doneCh)

	// Wait for all threads to complete
	wg.Wait()

	// Analyze results
	latencyLock.Lock()
	defer latencyLock.Unlock()

	if len(latencies) == 0 {
		t.Fatal("No latency data collected")
	}

	// Sort latency data for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate statistics
	var sum time.Duration
	for _, latency := range latencies {
		sum += latency
	}
	avgLatency := sum / time.Duration(len(latencies))

	// Calculate standard deviation
	var variance float64
	for _, latency := range latencies {
		diff := float64(latency - avgLatency)
		variance += diff * diff
	}
	variance /= float64(len(latencies))
	stdDev := time.Duration(math.Sqrt(variance))

	// Output results
	t.Logf("Multi-thread Store-Load Latency Statistics (samples: %d):", len(latencies))
	t.Logf("  Average latency: %v", avgLatency)
	t.Logf("  Standard deviation: %v", stdDev)
	t.Logf("  Min latency: %v", latencies[0])
	t.Logf("  Max latency: %v", latencies[len(latencies)-1])

	// Output percentiles
	for _, p := range percentiles {
		idx := int(float64(len(latencies)-1) * p / 100)
		t.Logf("  %v percentile: %v", p, latencies[idx])
	}
}

//
//func TestMapOfConcurrentInsert(t *testing.T) {
//	const total = 100_000_000
//
//	m := NewMapOf[int, int](WithPresize(total))
//
//	numCPU := runtime.GOMAXPROCS(0)
//
//	var wg sync.WaitGroup
//	wg.Add(numCPU)
//
//	start := time.Now()
//
//	batchSize := total / numCPU
//
//	for i := 0; i < numCPU; i++ {
//		go func(start, end int) {
//			//defer wg.Done()
//
//			for j := start; j < end; j++ {
//				m.Store(j, j)
//			}
//			wg.Done()
//		}(i*batchSize, min((i+1)*batchSize, total))
//	}
//
//	wg.Wait()
//
//	elapsed := time.Since(start)
//
//	size := m.Size()
//	if size != total {
//		t.Errorf("Expected size %d, got %d", total, size)
//	}
//
//	t.Logf("Inserted %d items in %v", total, elapsed)
//	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
//	t.Logf("Throughput: %.2f million ops/sec", float64(total)/(elapsed.Seconds()*1000000))
//
//	// rand check
//	for i := 0; i < 1000; i++ {
//		idx := i * (total / 1000)
//		if val, ok := m.Load(idx); !ok || val != idx {
//			t.Errorf("Expected value %d at key %d, got %d, exists: %v", idx, idx, val, ok)
//		}
//	}
//}

func TestMapOfMisc(t *testing.T) {
	//var a *SyncMap[int, int] = NewSyncMap[int, int]()
	var a, a1, a2, a3, a4 MapOf[int, int]

	t.Log(unsafe.Sizeof(MapOf[string, int]{}))

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

	var idm MapOf[structKey, int]
	t.Log(idm.LoadOrStore(structKey{1, 1}, 1))
	t.Log(&idm)
	t.Log(idm.LoadAndDelete(structKey{1, 1}))
	t.Log(&idm)

	var test int64
	for k := int64(0); k <= 100000; k++ {
		atomic.StoreInt64(&test, k)
		if test != k {
			t.Fatal("sync test fail:", test, k)
		}
	}

	var wg sync.WaitGroup
	for k := int64(0); k <= 100000; k++ {

		wg.Add(1)
		go func(k int64) {
			atomic.StoreInt64(&test, k)
			wg.Done()
		}(k)
		wg.Wait()
		if test != k {
			t.Fatal("async test2 fail:", test, k)
		}
	}
	a.Clear()
	for i := range 32 {
		t.Log(a.LoadOrStore(i, i))
	}

	for i := range 32 {
		t.Log(a.Load(i))
	}
}

// TestMapOfSimpleConcurrentReadWrite test 1 goroutine for store and 1 goroutine for load
func TestMapOfSimpleConcurrentReadWrite(t *testing.T) {
	const iterations = 1000

	m := NewMapOf[string, int]()

	writeDone := make(chan int)
	readDone := make(chan struct{})

	var failures int

	// start reader goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			// wait for writer to complete and send the written value
			expectedValue := <-writeDone

			// read and verify value
			value, ok := m.Load("test-key")
			if !ok {
				t.Logf("Iteration %d: key not found", i)
				failures++
			} else if value != expectedValue {
				t.Logf("Iteration %d: read value %d, expected %d", i, value, expectedValue)
				failures++
			}

			// notify writer to continue
			readDone <- struct{}{}
		}
	}()

	// start writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			// write value
			m.Store("test-key", i)

			// notify reader and pass expected value
			writeDone <- i

			// wait for reader to complete
			<-readDone
		}
	}()

	// wait for all goroutines to complete
	wg.Wait()

	if failures > 0 {
		t.Errorf("Found %d read failures", failures)
	} else {
		t.Logf("All %d reads successful", iterations)
	}
}

// TestMapOfMultiKeyConcurrentReadWrite tests concurrent read/write with multiple keys
func TestMapOfMultiKeyConcurrentReadWrite(t *testing.T) {
	const (
		iterations = 1000
		keyCount   = 100
	)

	m := NewMapOf[int, int]()

	// channels for goroutine communication
	writeDone := make(chan struct{})
	readDone := make(chan struct{})

	// track test results
	var failures int

	// start writer goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			// write a batch of key-value pairs
			for k := 0; k < keyCount; k++ {
				m.Store(k, i)
			}

			// notify reader to start reading
			writeDone <- struct{}{}

			// wait for reader to complete
			<-readDone
		}
	}()

	// start reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			// wait for writer to complete
			<-writeDone

			// read and verify all key-value pairs
			for k := 0; k < keyCount; k++ {
				value, ok := m.Load(k)
				if !ok {
					t.Logf("Iteration %d: key %d not found", i, k)
					failures++
				} else if value != i {
					t.Logf("Iteration %d: key %d has value %d, expected %d", i, k, value, i)
					failures++
				}
			}

			// notify writer to continue
			readDone <- struct{}{}
		}
	}()

	// wait for all goroutines to complete
	wg.Wait()

	if failures > 0 {
		t.Errorf("Found %d read failures", failures)
	} else {
		t.Logf("All %d reads successful", iterations*keyCount)
	}
}

// TestMapOfConcurrentReadWriteStress performs intensive concurrent stress testing
func TestMapOfConcurrentReadWriteStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test")
	}

	const (
		writerCount = 4
		readerCount = 16
		keyCount    = 1000
		iterations  = 10000
	)

	m := NewMapOf[int, int]()
	var wg sync.WaitGroup

	// start writer goroutines
	for w := 0; w < writerCount; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				key := (writerID*iterations + i) % keyCount
				m.Store(key, writerID*10000+i)
			}
		}(w)
	}

	// start reader goroutines
	readErrors := make(chan string, readerCount*iterations)
	for r := 0; r < readerCount; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				for k := 0; k < keyCount; k++ {
					_, _ = m.Load(k) // we only care about crashes or deadlocks
				}
				//time.Sleep(time.Microsecond) // slightly slow down reading
			}
		}()
	}

	// wait for all goroutines to complete
	wg.Wait()
	close(readErrors)

	// check for errors
	errorCount := 0
	for err := range readErrors {
		t.Log(err)
		errorCount++
		if errorCount >= 10 {
			t.Log("Too many errors, stopping display...")
			break
		}
	}

	if errorCount > 0 {
		t.Errorf("Found %d read errors", errorCount)
	}
}

func TestMapOfCalcLen(t *testing.T) {
	var tableLen, growTableLen, sizeLen, parallelism, lastTableLen, lastGrowTableLen, lastSizeLen, lastParallelism int
	cpus := runtime.GOMAXPROCS(0)
	t.Log("runtime.GOMAXPROCS(0),", cpus)
	for i := 0; i < 1000000; i++ {
		tableLen = calcTableLen(i)
		sizeLen = calcSizeLen(i, cpus)
		//const sizeHintFactor = float64(entriesPerMapOfBucket) * mapLoadFactor
		growThreshold := int(float64(tableLen*entriesPerMapOfBucket) * mapLoadFactor)
		growTableLen = calcTableLen(growThreshold)
		_, parallelism = calcParallelism(tableLen, minBucketsPerGoroutine, cpus)
		if tableLen != lastTableLen ||
			growTableLen != lastGrowTableLen ||
			sizeLen != lastSizeLen ||
			parallelism != lastParallelism {
			t.Logf("sizeHint: %v, tableLen: %v, growThreshold: %v, growTableLen: %v, counterLen: %v, parallelism: %v",
				i, tableLen, growThreshold, growTableLen, sizeLen, parallelism)
			lastTableLen, lastGrowTableLen, lastSizeLen, lastParallelism = tableLen, growTableLen, sizeLen, parallelism
		}
	}
}

// // NewBadMapOf creates a new MapOf for the provided key and value
// // but with an intentionally bad hash function.
func NewBadMapOf[K, V comparable]() *MapOf[K, V] {
	// Stub out the good hash function with a terrible one.
	// Everything should still work as expected.
	var m MapOf[K, V]

	m.keyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
		return 0
	}

	return &m
}

//
// NewTruncMapOf creates a new MapOf for the provided key and value
// but with an intentionally bad hash function.

func NewTruncMapOf[K, V comparable]() *MapOf[K, V] {
	// Stub out the good hash function with a terrible one.
	// Everything should still work as expected.
	var m MapOf[K, V]
	hasher, _ := defaultHasherUsingBuiltIn[K, V]()
	m.keyHash = func(pointer unsafe.Pointer, u uintptr) uintptr {
		return hasher(pointer, u) & ((uintptr(1) << 4) - 1)
	}
	return &m
}
func TestMapOf(t *testing.T) {
	testMapOf(t, func() *MapOf[string, int] {
		return &MapOf[string, int]{}
	})
}

func TestMapOfBadHash(t *testing.T) {
	testMapOf(t, func() *MapOf[string, int] {
		return NewBadMapOf[string, int]()
	})
}

func TestMapOfTruncHash(t *testing.T) {
	testMapOf(t, func() *MapOf[string, int] {
		// Stub out the good hash function with a different terrible one
		// (truncated hash). Everything should still work as expected.
		// This is useful to test independently to catch issues with
		// near collisions, where only the last few bits of the hash differ.
		return NewTruncMapOf[string, int]()
	})
}

func testMapOf(t *testing.T, newMap func() *MapOf[string, int]) {
	t.Run("LoadEmpty", func(t *testing.T) {
		m := newMap()

		for _, s := range testData {
			expectMissingMapOf(t, s, 0)(m.Load(s))
		}
	})
	t.Run("LoadOrStore", func(t *testing.T) {
		m := newMap()

		for i, s := range testData {
			expectMissingMapOf(t, s, 0)(m.Load(s))
			expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
			expectPresentMapOf(t, s, i)(m.Load(s))
			expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
		}
		for i, s := range testData {
			expectPresentMapOf(t, s, i)(m.Load(s))
			expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
		}
	})
	t.Run("All", func(t *testing.T) {
		m := newMap()

		testAllMapOf(t, m, testDataMapMapOf(testData[:]), func(_ string, _ int) bool {
			return true
		})
	})
	t.Run("Clear", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
			}
			m.Clear()
			for _, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
			}
		})
		t.Run("Concurrent", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for _, s := range testData {
						// Try a couple things to interfere with the clear.
						expectNotDeletedMapOf(t, s, math.MaxInt)(m.CompareAndDelete(s, math.MaxInt))
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
				expectMissingMapOf(t, s, 0)(m.Load(s))
			}
		})
	})
	t.Run("CompareAndDelete", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			m := newMap()

			for range 3 {
				for i, s := range testData {
					expectMissingMapOf(t, s, 0)(m.Load(s))
					expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
					expectPresentMapOf(t, s, i)(m.Load(s))
					expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
				}
				for i, s := range testData {
					expectPresentMapOf(t, s, i)(m.Load(s))
					expectNotDeletedMapOf(t, s, math.MaxInt)(m.CompareAndDelete(s, math.MaxInt))
					expectDeletedMapOf(t, s, i)(m.CompareAndDelete(s, i))
					expectNotDeletedMapOf(t, s, i)(m.CompareAndDelete(s, i))
					expectMissingMapOf(t, s, 0)(m.Load(s))
				}
				for _, s := range testData {
					expectMissingMapOf(t, s, 0)(m.Load(s))
				}
			}
		})
		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
			}
			expectNotDeletedMapOf(t, testData[15], math.MaxInt)(m.CompareAndDelete(testData[15], math.MaxInt))
			expectDeletedMapOf(t, testData[15], 15)(m.CompareAndDelete(testData[15], 15))
			expectNotDeletedMapOf(t, testData[15], 15)(m.CompareAndDelete(testData[15], 15))
			for i, s := range testData {
				if i == 15 {
					expectMissingMapOf(t, s, 0)(m.Load(s))
				} else {
					expectPresentMapOf(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectNotDeletedMapOf(t, testData[i], math.MaxInt)(m.CompareAndDelete(testData[i], math.MaxInt))
				expectDeletedMapOf(t, testData[i], i)(m.CompareAndDelete(testData[i], i))
				expectNotDeletedMapOf(t, testData[i], i)(m.CompareAndDelete(testData[i], i))
			}
			for i, s := range testData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectMissingMapOf(t, s, 0)(m.Load(s))
				} else {
					expectPresentMapOf(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Iterate", func(t *testing.T) {
			m := newMap()

			testAllMapOf(t, m, testDataMapMapOf(testData[:]), func(s string, i int) bool {
				expectDeletedMapOf(t, s, i)(m.CompareAndDelete(s, i))
				return true
			})
			for _, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
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
						expectMissingMapOf(t, key, 0)(m.Load(key))
						expectStoredMapOf(t, key, id)(m.LoadOrStore(key, id))
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedMapOf(t, key, id)(m.LoadOrStore(key, 0))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectDeletedMapOf(t, key, id)(m.CompareAndDelete(key, id))
						expectMissingMapOf(t, key, 0)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissingMapOf(t, key, 0)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for i, s := range testData {
						expectNotDeletedMapOf(t, s, math.MaxInt)(m.CompareAndDelete(s, math.MaxInt))
						m.CompareAndDelete(s, i)
						expectMissingMapOf(t, s, 0)(m.Load(s))
					}
					for _, s := range testData {
						expectMissingMapOf(t, s, 0)(m.Load(s))
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
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
			}
			for j := range 3 {
				for i, s := range testData {
					expectPresentMapOf(t, s, i+j)(m.Load(s))
					expectNotSwappedMapOf(t, s, math.MaxInt, i+j+1)(m.CompareAndSwap(s, math.MaxInt, i+j+1))
					expectSwappedMapOf(t, s, i, i+j+1)(m.CompareAndSwap(s, i+j, i+j+1))
					expectNotSwappedMapOf(t, s, i+j, i+j+1)(m.CompareAndSwap(s, i+j, i+j+1))
					expectPresentMapOf(t, s, i+j+1)(m.Load(s))
				}
			}
			for i, s := range testData {
				expectPresentMapOf(t, s, i+3)(m.Load(s))
			}
		})
		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
			}
			expectNotSwappedMapOf(t, testData[15], math.MaxInt, 16)(m.CompareAndSwap(testData[15], math.MaxInt, 16))
			expectSwappedMapOf(t, testData[15], 15, 16)(m.CompareAndSwap(testData[15], 15, 16))
			expectNotSwappedMapOf(t, testData[15], 15, 16)(m.CompareAndSwap(testData[15], 15, 16))
			for i, s := range testData {
				if i == 15 {
					expectPresentMapOf(t, s, 16)(m.Load(s))
				} else {
					expectPresentMapOf(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectNotSwappedMapOf(t, testData[i], math.MaxInt, i+1)(m.CompareAndSwap(testData[i], math.MaxInt, i+1))
				expectSwappedMapOf(t, testData[i], i, i+1)(m.CompareAndSwap(testData[i], i, i+1))
				expectNotSwappedMapOf(t, testData[i], i, i+1)(m.CompareAndSwap(testData[i], i, i+1))
			}
			for i, s := range testData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectPresentMapOf(t, s, i+1)(m.Load(s))
				} else {
					expectPresentMapOf(t, s, i)(m.Load(s))
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
						expectMissingMapOf(t, key, 0)(m.Load(key))
						expectStoredMapOf(t, key, id)(m.LoadOrStore(key, id))
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedMapOf(t, key, id)(m.LoadOrStore(key, 0))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectSwappedMapOf(t, key, id, id+1)(m.CompareAndSwap(key, id, id+1))
						expectPresentMapOf(t, key, id+1)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id+1)(m.Load(key))
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
						expectMissingMapOf(t, key, 0)(m.Load(key))
						expectStoredMapOf(t, key, id)(m.LoadOrStore(key, id))
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedMapOf(t, key, id)(m.LoadOrStore(key, 0))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectSwappedMapOf(t, key, id, id+1)(m.CompareAndSwap(key, id, id+1))
						expectPresentMapOf(t, key, id+1)(m.Load(key))
						expectDeletedMapOf(t, key, id+1)(m.CompareAndDelete(key, id+1))
						expectNotSwappedMapOf(t, key, id+1, id+2)(m.CompareAndSwap(key, id+1, id+2))
						expectNotDeletedMapOf(t, key, id+1)(m.CompareAndDelete(key, id+1))
						expectMissingMapOf(t, key, 0)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissingMapOf(t, key, 0)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for i, s := range testData {
						expectNotSwappedMapOf(t, s, math.MaxInt, i+1)(m.CompareAndSwap(s, math.MaxInt, i+1))
						m.CompareAndSwap(s, i, i+1)
						expectPresentMapOf(t, s, i+1)(m.Load(s))
					}
					for i, s := range testData {
						expectPresentMapOf(t, s, i+1)(m.Load(s))
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
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectNotLoadedFromSwapMapOf(t, s, i)(m.Swap(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedFromSwapMapOf(t, s, i, i)(m.Swap(s, i))
			}
			for j := range 3 {
				for i, s := range testData {
					expectPresentMapOf(t, s, i+j)(m.Load(s))
					expectLoadedFromSwapMapOf(t, s, i+j, i+j+1)(m.Swap(s, i+j+1))
					expectPresentMapOf(t, s, i+j+1)(m.Load(s))
				}
			}
			for i, s := range testData {
				expectLoadedFromSwapMapOf(t, s, i+3, i+3)(m.Swap(s, i+3))
			}
		})
		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectNotLoadedFromSwapMapOf(t, s, i)(m.Swap(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedFromSwapMapOf(t, s, i, i)(m.Swap(s, i))
			}
			expectLoadedFromSwapMapOf(t, testData[15], 15, 16)(m.Swap(testData[15], 16))
			for i, s := range testData {
				if i == 15 {
					expectPresentMapOf(t, s, 16)(m.Load(s))
				} else {
					expectPresentMapOf(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectNotLoadedFromSwapMapOf(t, s, i)(m.Swap(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedFromSwapMapOf(t, s, i, i)(m.Swap(s, i))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectLoadedFromSwapMapOf(t, testData[i], i, i+1)(m.Swap(testData[i], i+1))
			}
			for i, s := range testData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectPresentMapOf(t, s, i+1)(m.Load(s))
				} else {
					expectPresentMapOf(t, s, i)(m.Load(s))
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
						expectMissingMapOf(t, key, 0)(m.Load(key))
						expectNotLoadedFromSwapMapOf(t, key, id)(m.Swap(key, id))
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromSwapMapOf(t, key, id, id)(m.Swap(key, id))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromSwapMapOf(t, key, id, id+1)(m.Swap(key, id+1))
						expectPresentMapOf(t, key, id+1)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id+1)(m.Load(key))
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
						expectMissingMapOf(t, key, 0)(m.Load(key))
						expectNotLoadedFromSwapMapOf(t, key, id)(m.Swap(key, id))
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromSwapMapOf(t, key, id, id)(m.Swap(key, id))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromSwapMapOf(t, key, id, id+1)(m.Swap(key, id+1))
						expectPresentMapOf(t, key, id+1)(m.Load(key))
						expectDeletedMapOf(t, key, id+1)(m.CompareAndDelete(key, id+1))
						expectNotLoadedFromSwapMapOf(t, key, id+2)(m.Swap(key, id+2))
						expectPresentMapOf(t, key, id+2)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id+2)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for i, s := range testData {
						m.Swap(s, i+1)
						expectPresentMapOf(t, s, i+1)(m.Load(s))
					}
					for i, s := range testData {
						expectPresentMapOf(t, s, i+1)(m.Load(s))
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
					expectMissingMapOf(t, s, 0)(m.Load(s))
					expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
					expectPresentMapOf(t, s, i)(m.Load(s))
					expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
				}
				for i, s := range testData {
					expectPresentMapOf(t, s, i)(m.Load(s))
					expectLoadedFromDeleteMapOf(t, s, i)(m.LoadAndDelete(s))
					expectMissingMapOf(t, s, 0)(m.Load(s))
					expectNotLoadedFromDeleteMapOf(t, s, 0)(m.LoadAndDelete(s))
				}
				for _, s := range testData {
					expectMissingMapOf(t, s, 0)(m.Load(s))
				}
			}
		})
		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
			}
			expectPresentMapOf(t, testData[15], 15)(m.Load(testData[15]))
			expectLoadedFromDeleteMapOf(t, testData[15], 15)(m.LoadAndDelete(testData[15]))
			expectMissingMapOf(t, testData[15], 0)(m.Load(testData[15]))
			expectNotLoadedFromDeleteMapOf(t, testData[15], 0)(m.LoadAndDelete(testData[15]))
			for i, s := range testData {
				if i == 15 {
					expectMissingMapOf(t, s, 0)(m.Load(s))
				} else {
					expectPresentMapOf(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
				expectPresentMapOf(t, s, i)(m.Load(s))
				expectLoadedMapOf(t, s, i)(m.LoadOrStore(s, 0))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectPresentMapOf(t, testData[i], i)(m.Load(testData[i]))
				expectLoadedFromDeleteMapOf(t, testData[i], i)(m.LoadAndDelete(testData[i]))
				expectMissingMapOf(t, testData[i], 0)(m.Load(testData[i]))
				expectNotLoadedFromDeleteMapOf(t, testData[i], 0)(m.LoadAndDelete(testData[i]))
			}
			for i, s := range testData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectMissingMapOf(t, s, 0)(m.Load(s))
				} else {
					expectPresentMapOf(t, s, i)(m.Load(s))
				}
			}
		})
		t.Run("Iterate", func(t *testing.T) {
			m := newMap()

			testAllMapOf(t, m, testDataMapMapOf(testData[:]), func(s string, i int) bool {
				expectLoadedFromDeleteMapOf(t, s, i)(m.LoadAndDelete(s))
				return true
			})
			for _, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
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
						expectMissingMapOf(t, key, 0)(m.Load(key))
						expectStoredMapOf(t, key, id)(m.LoadOrStore(key, id))
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedMapOf(t, key, id)(m.LoadOrStore(key, 0))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromDeleteMapOf(t, key, id)(m.LoadAndDelete(key))
						expectMissingMapOf(t, key, 0)(m.Load(key))
					}
					for _, s := range testData {
						key := makeKey(s)
						expectMissingMapOf(t, key, 0)(m.Load(key))
					}
				}(i)
			}
			wg.Wait()
		})
		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			// Load up the map.
			for i, s := range testData {
				expectMissingMapOf(t, s, 0)(m.Load(s))
				expectStoredMapOf(t, s, i)(m.LoadOrStore(s, i))
			}
			gmp := runtime.GOMAXPROCS(-1)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for _, s := range testData {
						m.LoadAndDelete(s)
						expectMissingMapOf(t, s, 0)(m.Load(s))
					}
					for _, s := range testData {
						expectMissingMapOf(t, s, 0)(m.Load(s))
					}
				}(i)
			}
			wg.Wait()
		})
	})
}

func testAllMapOf[K, V comparable](t *testing.T, m *MapOf[K, V], testData map[K]V, yield func(K, V) bool) {
	for k, v := range testData {
		expectStoredMapOf(t, k, v)(m.LoadOrStore(k, v))
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

func expectPresentMapOf[K, V comparable](t *testing.T, key K, want V) func(got V, ok bool) {
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

func expectMissingMapOf[K, V comparable](t *testing.T, key K, want V) func(got V, ok bool) {
	t.Helper()
	if want != *new(V) {
		// This is awkward, but the want argument is necessary to smooth over type inference.
		// Just make sure the want argument always looks the same.
		panic("expectMissingMapOf must always have a zero value variable")
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

func expectLoadedMapOf[K, V comparable](t *testing.T, key K, want V) func(got V, loaded bool) {
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

func expectStoredMapOf[K, V comparable](t *testing.T, key K, want V) func(got V, loaded bool) {
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

func expectDeletedMapOf[K, V comparable](t *testing.T, key K, old V) func(deleted bool) {
	t.Helper()
	return func(deleted bool) {
		t.Helper()

		if !deleted {
			t.Errorf("expected key %v with value %v to be in map and deleted", key, old)
		}
	}
}

func expectNotDeletedMapOf[K, V comparable](t *testing.T, key K, old V) func(deleted bool) {
	t.Helper()
	return func(deleted bool) {
		t.Helper()

		if deleted {
			t.Errorf("expected key %v with value %v to not be in map and thus not deleted", key, old)
		}
	}
}

func expectSwappedMapOf[K, V comparable](t *testing.T, key K, old, new V) func(swapped bool) {
	t.Helper()
	return func(swapped bool) {
		t.Helper()

		if !swapped {
			t.Errorf("expected key %v with value %v to be in map and swapped for %v", key, old, new)
		}
	}
}

func expectNotSwappedMapOf[K, V comparable](t *testing.T, key K, old, new V) func(swapped bool) {
	t.Helper()
	return func(swapped bool) {
		t.Helper()

		if swapped {
			t.Errorf("expected key %v with value %v to not be in map or not swapped for %v", key, old, new)
		}
	}
}

func expectLoadedFromSwapMapOf[K, V comparable](t *testing.T, key K, want, new V) func(got V, loaded bool) {
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

func expectNotLoadedFromSwapMapOf[K, V comparable](t *testing.T, key K, new V) func(old V, loaded bool) {
	t.Helper()
	return func(old V, loaded bool) {
		t.Helper()

		if loaded {
			t.Errorf("expected key %v to not be in map, but found value %v for it", key, old)
		}
	}
}

func expectLoadedFromDeleteMapOf[K, V comparable](t *testing.T, key K, want V) func(got V, loaded bool) {
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

func expectNotLoadedFromDeleteMapOf[K, V comparable](t *testing.T, key K, _ V) func(old V, loaded bool) {
	t.Helper()
	return func(old V, loaded bool) {
		t.Helper()

		if loaded {
			t.Errorf("expected key %v to not be in map, but found value %v for it", key, old)
		}
	}
}

func testDataMapMapOf(data []string) map[string]int {
	m := make(map[string]int)
	for i, s := range data {
		m[s] = i
	}
	return m
}

//
//var (
//	testDataSmall [8]string
//	testData      [128]string
//	testDataLarge [128 << 10]string
//
//	testDataIntSmall [8]int
//	testDataInt      [128]int
//	testDataIntLarge [128 << 10]int
//)
//
//func init() {
//	for i := range testDataSmall {
//		testDataSmall[i] = fmt.Sprintf("%b", i)
//	}
//	for i := range testData {
//		testData[i] = fmt.Sprintf("%b", i)
//	}
//	for i := range testDataLarge {
//		testDataLarge[i] = fmt.Sprintf("%b", i)
//	}
//
//	for i := range testDataIntSmall {
//		testDataIntSmall[i] = i
//	}
//	for i := range testData {
//		testDataInt[i] = i
//	}
//	for i := range testDataIntLarge {
//		testDataIntLarge[i] = i
//	}
//}
//
//// TestConcurrentCacheMapOf tests MapOf in a scenario where it is used as
//// the basis of a memory-efficient concurrent cache. We're specifically
//// looking to make sure that CompareAndSwap and CompareAndDelete are
//// atomic with respect to one another. When competing for the same
//// key-value pair, they must not both succeed.
////
//// This test is a regression test for issue #70970.
//func TestConcurrentCacheMapOf(t *testing.T) {
//	type dummy [32]byte
//
//	var m MapOf[int, weak.Pointer[dummy]]
//
//	type cleanupArg struct {
//		key   int
//		value weak.Pointer[dummy]
//	}
//	cleanup := func(arg cleanupArg) {
//		m.CompareAndDelete(arg.key, arg.value)
//	}
//	get := func(m *MapOf[int, weak.Pointer[dummy]], key int) *dummy {
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

// ------------------------------------------------------

type point struct {
	x int32
	y int32
}

func TestMapOf_MissingEntry(t *testing.T) {
	m := NewMapOf[string, string]()
	v, ok := m.Load("foo")
	if ok {
		t.Fatalf("value was not expected: %v", v)
	}
	if deleted, loaded := m.LoadAndDelete("foo"); loaded {
		t.Fatalf("value was not expected %v", deleted)
	}
	if actual, loaded := m.LoadOrStore("foo", "bar"); loaded {
		t.Fatalf("value was not expected %v", actual)
	}
}

func TestMapOf_EmptyStringKey(t *testing.T) {
	m := NewMapOf[string, string]()
	m.Store("", "foobar")
	v, ok := m.Load("")
	if !ok {
		t.Fatal("value was expected")
	}
	if v != "foobar" {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestMapOfStore_NilValue(t *testing.T) {
	m := NewMapOf[string, *struct{}]()
	m.Store("foo", nil)
	v, ok := m.Load("foo")
	if !ok {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestMapOfLoadOrStore_NilValue(t *testing.T) {
	m := NewMapOf[string, *struct{}]()
	m.LoadOrStore("foo", nil)
	v, loaded := m.LoadOrStore("foo", nil)
	if !loaded {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestMapOfLoadOrStore_NonNilValue(t *testing.T) {
	type foo struct{}
	m := NewMapOf[string, *foo]()
	newv := &foo{}
	v, loaded := m.LoadOrStore("foo", newv)
	if loaded {
		t.Fatal("no value was expected")
	}
	if v != newv {
		t.Fatalf("value does not match: %v", v)
	}
	newv2 := &foo{}
	v, loaded = m.LoadOrStore("foo", newv2)
	if !loaded {
		t.Fatal("value was expected")
	}
	if v != newv {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestMapOfLoadAndStore_NilValue(t *testing.T) {
	m := NewMapOf[string, *struct{}]()
	m.LoadAndStore("foo", nil)
	v, loaded := m.LoadAndStore("foo", nil)
	if !loaded {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
	v, loaded = m.Load("foo")
	if !loaded {
		t.Fatal("nil value was expected")
	}
	if v != nil {
		t.Fatalf("value was not nil: %v", v)
	}
}

func TestMapOfLoadAndStore_NonNilValue(t *testing.T) {
	m := NewMapOf[string, int]()
	v1 := 1
	v, loaded := m.LoadAndStore("foo", v1)
	if loaded {
		t.Fatal("no value was expected")
	}
	if v != v1 {
		t.Fatalf("value does not match: %v", v)
	}
	v2 := 2
	v, loaded = m.LoadAndStore("foo", v2)
	if !loaded {
		t.Fatal("value was expected")
	}
	if v != v1 {
		t.Fatalf("value does not match: %v", v)
	}
	v, loaded = m.Load("foo")
	if !loaded {
		t.Fatal("value was expected")
	}
	if v != v2 {
		t.Fatalf("value does not match: %v", v)
	}
}

func TestMapOfRange(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	met := make(map[string]int)
	m.Range(func(key string, value int) bool {
		if key != strconv.Itoa(value) {
			t.Fatalf("got unexpected key/value for iteration %d: %v/%v", iters, key, value)
			return false
		}
		met[key] += 1
		iters++
		return true
	})
	if iters != numEntries {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
	for i := 0; i < numEntries; i++ {
		if c := met[strconv.Itoa(i)]; c != 1 {
			t.Fatalf("range did not iterate correctly over %d: %d", i, c)
		}
	}
}

func TestMapOfRange_FalseReturned(t *testing.T) {
	m := NewMapOf[string, int]()
	for i := 0; i < 100; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	iters := 0
	m.Range(func(key string, value int) bool {
		iters++
		return iters != 13
	})
	if iters != 13 {
		t.Fatalf("got unexpected number of iterations: %d", iters)
	}
}

func TestMapOfRange_NestedDelete(t *testing.T) {
	const numEntries = 256
	m := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	m.Range(func(key string, value int) bool {
		m.Delete(key)
		return true
	})
	for i := 0; i < numEntries; i++ {
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value found for %d", i)
		}
	}
}

func TestMapOfStringStore(t *testing.T) {
	const numEntries = 128
	m := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapOfIntStore(t *testing.T) {
	const numEntries = 128
	m := NewMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapOfStore_StructKeys_IntValues(t *testing.T) {
	const numEntries = 128
	m := NewMapOf[point, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(point{int32(i), -int32(i)}, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(point{int32(i), -int32(i)})
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapOfStore_StructKeys_StructValues(t *testing.T) {
	const numEntries = 128
	m := NewMapOf[point, point]()
	for i := 0; i < numEntries; i++ {
		m.Store(point{int32(i), -int32(i)}, point{-int32(i), int32(i)})
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(point{int32(i), -int32(i)})
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v.x != -int32(i) {
			t.Fatalf("x value does not match for %d: %v", i, v)
		}
		if v.y != int32(i) {
			t.Fatalf("y value does not match for %d: %v", i, v)
		}
	}
}

func TestMapOfWithHasher(t *testing.T) {
	const numEntries = 10000
	m := NewMapOfWithHasher[int, int](murmur3Finalizer, nil)
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func murmur3Finalizer(i int, _ uintptr) uintptr {
	if bits.UintSize == 32 {
		h := uintptr(i)
		h = (h ^ (h >> 16)) * 0x85ebca6b
		h = (h ^ (h >> 13)) * 0xc2b2ae35
		return h ^ (h >> 16)
	}
	h := uint32(i >> 32)
	h = (h ^ (h >> 16)) * 0x85ebca6b
	h = (h ^ (h >> 13)) * 0xc2b2ae35
	h = h ^ (h >> 16)
	l := uint32(i)
	l = (l ^ (l >> 16)) * 0x85ebca6b
	l = (l ^ (l >> 13)) * 0xc2b2ae35
	l = l ^ (l >> 16)
	return uintptr(h) << 32 & uintptr(l)
	//}
	//h := uintptr(i)
	//h = (h ^ (h >> 33)) * 0xff51afd7ed558ccd
	//h = (h ^ (h >> 33)) * 0xc4ceb9fe1a85ec53
	//return h ^ (h >> 33)
}

func TestMapOfWithHasher_HashCodeCollisions(t *testing.T) {
	const numEntries = 1000
	m := NewMapOfWithHasher[int, int](func(i int, _ uintptr) uintptr {
		// We intentionally use an awful hash function here to make sure
		// that the map copes with key collisions.
		return 42
	}, nil, WithPresize(numEntries))
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapOfLoadOrStore(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadOrStore(strconv.Itoa(i), i); !loaded {
			t.Fatalf("value not found for %d", i)
		}
	}
}

func TestMapOfLoadOrCompute(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
			return i, true
		})
		if loaded {
			t.Fatalf("value not computed for %d", i)
		}
		if v != 0 {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	if m.Size() != 0 {
		t.Fatalf("zero map size expected: %d", m.Size())
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
			return i, false
		})
		if loaded {
			t.Fatalf("value not computed for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(strconv.Itoa(i), func() (newValue int, cancel bool) {
			t.Fatalf("value func invoked")
			return newValue, false
		})
		if !loaded {
			t.Fatalf("value not loaded for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func TestMapOfLoadOrCompute_FunctionCalledOnce(t *testing.T) {
	m := NewMapOf[int, int]()
	for i := 0; i < 100; {
		m.LoadOrCompute(i, func() (newValue int, cancel bool) {
			newValue, i = i, i+1
			return newValue, false
		})
	}
	m.Range(func(k, v int) bool {
		if k != v {
			t.Fatalf("%dth key is not equal to value %d", k, v)
		}
		return true
	})
}

func TestMapOfCompute(t *testing.T) {
	m := NewMapOf[string, int]()
	// Store a new value.
	v, ok := m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 0 {
			t.Fatalf("oldValue should be 0 when computing a new value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when computing a new value")
		}
		newValue = 42
		op = UpdateOp
		return
	})
	if v != 42 {
		t.Fatalf("v should be 42 when computing a new value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when computing a new value")
	}
	// Update an existing value.
	v, ok = m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 42 {
			t.Fatalf("oldValue should be 42 when updating the value: %d", oldValue)
		}
		if !loaded {
			t.Fatal("loaded should be true when updating the value")
		}
		newValue = oldValue + 42
		op = UpdateOp
		return
	})
	if v != 84 {
		t.Fatalf("v should be 84 when updating the value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when updating the value")
	}
	// Check that NoOp doesn't update the value
	v, ok = m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		return 0, CancelOp
	})
	if v != 84 {
		t.Fatalf("v should be 84 after using NoOp: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when updating the value")
	}
	// Delete an existing value.
	v, ok = m.Compute("foobar", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 84 {
			t.Fatalf("oldValue should be 84 when deleting the value: %d", oldValue)
		}
		if !loaded {
			t.Fatal("loaded should be true when deleting the value")
		}
		op = DeleteOp
		return
	})
	if v != 84 {
		t.Fatalf("v should be 84 when deleting the value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when deleting the value")
	}
	// Try to delete a non-existing value. Notice different key.
	v, ok = m.Compute("barbaz", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 0 {
			t.Fatalf("oldValue should be 0 when trying to delete a non-existing value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when trying to delete a non-existing value")
		}
		// We're returning a non-zero value, but the map should ignore it.
		newValue = 42
		op = DeleteOp
		return
	})
	if v != 0 {
		t.Fatalf("v should be 0 when trying to delete a non-existing value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when trying to delete a non-existing value")
	}
	// Try NoOp on a non-existing value
	v, ok = m.Compute("barbaz", func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
		if oldValue != 0 {
			t.Fatalf("oldValue should be 0 when trying to delete a non-existing value: %d", oldValue)
		}
		if loaded {
			t.Fatal("loaded should be false when trying to delete a non-existing value")
		}
		// We're returning a non-zero value, but the map should ignore it.
		newValue = 42
		op = CancelOp
		return
	})
	if v != 0 {
		t.Fatalf("v should be 0 when trying to delete a non-existing value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when trying to delete a non-existing value")
	}
}

func TestMapOfStringStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapOfIntStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int32, int32]()
	for i := 0; i < numEntries; i++ {
		m.Store(int32(i), int32(i))
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(int32(i))
		if _, ok := m.Load(int32(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapOfStructStoreThenDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[point, string]()
	for i := 0; i < numEntries; i++ {
		m.Store(point{int32(i), 42}, strconv.Itoa(i))
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(point{int32(i), 42})
		if _, ok := m.Load(point{int32(i), 42}); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapOfStringStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries; i++ {
		if v, loaded := m.LoadAndDelete(strconv.Itoa(i)); !loaded || v != i {
			t.Fatalf("value was not found or different for %d: %v", i, v)
		}
		if _, ok := m.Load(strconv.Itoa(i)); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapOfIntStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadAndDelete(i); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(i); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapOfStructStoreThenLoadAndDelete(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[point, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(point{42, int32(i)}, i)
	}
	for i := 0; i < numEntries; i++ {
		if _, loaded := m.LoadAndDelete(point{42, int32(i)}); !loaded {
			t.Fatalf("value was not found for %d", i)
		}
		if _, ok := m.Load(point{42, int32(i)}); ok {
			t.Fatalf("value was not expected for %d", i)
		}
	}
}

func TestMapOfStoreThenParallelDelete_DoesNotShrinkBelowMinTableLen(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int, int](WithShrinkEnabled())
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}

	cdone := make(chan bool)
	go func() {
		for i := 0; i < numEntries; i++ {
			m.Delete(i)
		}
		cdone <- true
	}()
	//go func() {
	//	for i := 0; i < numEntries; i++ {
	//		m.Delete(i)
	//	}
	//	cdone <- true
	//}()
	//go func() {
	//	for i := 0; i < numEntries; i++ {
	//		m.Delete(i)
	//	}
	//	cdone <- true
	//}()
	//go func() {
	//	for i := 0; i < numEntries; i++ {
	//		m.Delete(i)
	//	}
	//	cdone <- true
	//}()
	//go func() {
	//	for i := 0; i < numEntries; i++ {
	//		m.Delete(i)
	//	}
	//	cdone <- true
	//}()
	// Wait for the goroutines to finish.
	<-cdone
	//<-cdone
	//<-cdone
	//<-cdone
	//<-cdone
	m.Shrink()
	stats := m.Stats()
	if stats.RootBuckets != DefaultMinMapTableLen {
		t.Fatalf("table length was different from the minimum: %d", stats.RootBuckets)
	}
}

func sizeBasedOnTypedRange(m *MapOf[string, int]) int {
	size := 0
	m.Range(func(key string, value int) bool {
		size++
		return true
	})
	return size
}

func TestMapOfSize(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[string, int]()
	size := m.Size()
	if size != 0 {
		t.Fatalf("zero size expected: %d", size)
	}
	expectedSize := 0
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
		expectedSize++
		size := m.Size()
		if size != expectedSize {
			t.Fatalf("size of %d was expected, got: %d", expectedSize, size)
		}
		rsize := sizeBasedOnTypedRange(m)
		if size != rsize {
			t.Fatalf("size does not match number of entries in Range: %v, %v", size, rsize)
		}
	}
	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
		expectedSize--
		size := m.Size()
		if size != expectedSize {
			t.Fatalf("size of %d was expected, got: %d", expectedSize, size)
		}
		rsize := sizeBasedOnTypedRange(m)
		if size != rsize {
			t.Fatalf("size does not match number of entries in Range: %v, %v", size, rsize)
		}
	}
}

func TestMapOfClear(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	size := m.Size()
	if size != numEntries {
		t.Fatalf("size of %d was expected, got: %d", numEntries, size)
	}
	m.Clear()
	size = m.Size()
	if size != 0 {
		t.Fatalf("zero size was expected, got: %d", size)
	}
	rsize := sizeBasedOnTypedRange(m)
	if rsize != 0 {
		t.Fatalf("zero number of entries in Range was expected, got: %d", rsize)
	}
}

func assertMapOfCapacity[K comparable, V any](t *testing.T, m *MapOf[K, V], expectedCap int) {
	stats := m.Stats()
	if stats.Capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, stats.Capacity)
	}
}

func TestNewMapOfPresized(t *testing.T) {
	//assertMapOfCapacity(t, NewMapOf[string, string](), DefaultMinMapOfTableCap)
	//assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(0)), DefaultMinMapOfTableCap)
	//assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(0)), DefaultMinMapOfTableCap)
	//assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(-100)), DefaultMinMapOfTableCap)
	//assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(-100)), DefaultMinMapOfTableCap)
	//assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(500)), 1280)
	//assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(500)), 1280)
	//assertMapOfCapacity(t, NewMapOf[int, int](WithPresize(1_000_000)), 2621440)
	//assertMapOfCapacity(t, NewMapOf[int, int](WithPresize(1_000_000)), 2621440)
	//assertMapOfCapacity(t, NewMapOf[point, point](WithPresize(100)), 160)
	//assertMapOfCapacity(t, NewMapOf[point, point](WithPresize(100)), 160)

	var capacity, expectedCap int
	capacity, expectedCap = NewMapOf[string, string]().Stats().Capacity, DefaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(0)).Stats().Capacity, DefaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(0)).Stats().Capacity, DefaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(-100)).Stats().Capacity, DefaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(-100)).Stats().Capacity, DefaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(500)).Stats().Capacity, calcTableLen(500)*entriesPerMapOfBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(500)).Stats().Capacity, calcTableLen(500)*entriesPerMapOfBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(1_000_000)).Stats().Capacity, calcTableLen(1_000_000)*entriesPerMapOfBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(1_000_000)).Stats().Capacity, calcTableLen(1_000_000)*entriesPerMapOfBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(100)).Stats().Capacity, calcTableLen(100)*entriesPerMapOfBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](WithPresize(100)).Stats().Capacity, calcTableLen(100)*entriesPerMapOfBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
}

func TestNewMapOfPresized_DoesNotShrinkBelowMinTableLen(t *testing.T) {
	const minTableLen = 1024
	const numEntries = int(minTableLen*float64(entriesPerMapOfBucket)*MapLoadFactor) - entriesPerMapOfBucket
	m := NewMapOf[int, int](WithPresize(numEntries), WithShrinkEnabled())
	for i := 0; i < 2*numEntries; i++ {
		m.Store(i, i)
	}

	stats := m.Stats()
	if stats.RootBuckets < minTableLen {
		t.Fatalf("table did not grow: %d", stats.RootBuckets)
	}

	for i := 0; i < 2*numEntries; i++ {
		m.Delete(i)
	}

	m.Shrink()

	stats = m.Stats()
	if stats.RootBuckets != minTableLen {
		t.Fatalf("table length was different from the minimum: %v", stats)
	}
}

func TestNewMapOfGrowOnly_OnlyShrinksOnClear(t *testing.T) {
	const minTableLen = 128
	const numEntries = minTableLen * EntriesPerMapOfBucket
	m := NewMapOf[int, int](WithPresize(numEntries), WithGrowOnly())

	stats := m.Stats()
	initialTableLen := stats.RootBuckets

	for i := 0; i < 2*numEntries; i++ {
		m.Store(i, i)
	}
	stats = m.Stats()
	maxTableLen := stats.RootBuckets
	if maxTableLen <= minTableLen {
		t.Fatalf("table did not grow: %d", maxTableLen)
	}

	for i := 0; i < numEntries; i++ {
		m.Delete(i)
	}
	stats = m.Stats()
	if stats.RootBuckets != maxTableLen {
		t.Fatalf("table length was different from the expected: %d", stats.RootBuckets)
	}

	m.Clear()
	stats = m.Stats()
	if stats.RootBuckets != initialTableLen {
		t.Fatalf("table length was different from the initial: %d", stats.RootBuckets)
	}
}

func TestMapOfResize(t *testing.T) {
	const numEntries = 100_000
	m := NewMapOf[string, int](WithShrinkEnabled())

	for i := 0; i < numEntries; i++ {
		m.Store(strconv.Itoa(i), i)
	}
	stats := m.Stats()
	if stats.Size != numEntries {
		t.Fatalf("size was too small: %d", stats.Size)
	}
	expectedCapacity := int(math.RoundToEven(MapLoadFactor+1)) * stats.RootBuckets * EntriesPerMapOfBucket
	if stats.Capacity > expectedCapacity {
		t.Fatalf("capacity was too large: %d, expected: %d", stats.Capacity, expectedCapacity)
	}
	if stats.RootBuckets <= DefaultMinMapTableLen {
		t.Fatalf("table was too small: %d", stats.RootBuckets)
	}
	if stats.TotalGrowths == 0 {
		t.Fatalf("non-zero total growths expected: %d", stats.TotalGrowths)
	}
	if stats.TotalShrinks > 0 {
		t.Fatalf("zero total shrinks expected: %d", stats.TotalShrinks)
	}
	// This is useful when debugging table resize and occupancy.
	// Use -v flag to see the output.
	t.Log(stats.ToString())

	for i := 0; i < numEntries; i++ {
		m.Delete(strconv.Itoa(i))
	}
	m.Shrink()

	stats = m.Stats()
	if stats.Size > 0 {
		t.Fatalf("zero size was expected: %d", stats.Size)
	}
	// TODO: Asynchronous shrinking requires a delay period
	expectedCapacity = stats.RootBuckets * EntriesPerMapOfBucket
	if stats.Capacity != expectedCapacity {
		t.Logf("capacity was too large: %d, expected: %d", stats.Capacity, expectedCapacity)
	}
	if stats.RootBuckets != DefaultMinMapTableLen {
		t.Logf("table was too large: %d", stats.RootBuckets)
	}
	if stats.TotalShrinks == 0 {
		t.Fatalf("non-zero total shrinks expected: %d", stats.TotalShrinks)
	}
	t.Log(stats.ToString())
}

func TestMapOfResize_CounterLenLimit(t *testing.T) {
	const numEntries = 1_000_000
	m := NewMapOf[string, string]()

	for i := 0; i < numEntries; i++ {
		m.Store("foo"+strconv.Itoa(i), "bar"+strconv.Itoa(i))
	}
	stats := m.Stats()
	if stats.Size != numEntries {
		t.Fatalf("size was too small: %d", stats.Size)
	}
	maxCounterLen := runtime.GOMAXPROCS(0) * 2
	if stats.CounterLen > maxCounterLen {
		t.Fatalf("number of counter stripes was too large: %d, expected: %d",
			stats.CounterLen, maxCounterLen)
	}
}

func parallelSeqTypedResizer(m *MapOf[int, int], numEntries int, positive bool, cdone chan bool) {
	for i := 0; i < numEntries; i++ {
		if positive {
			m.Store(i, i)
		} else {
			m.Store(-i, -i)
		}
	}
	cdone <- true
}

func TestMapOfParallelResize_GrowOnly(t *testing.T) {
	const numEntries = 100_000
	m := NewMapOf[int, int]()
	cdone := make(chan bool)
	go parallelSeqTypedResizer(m, numEntries, true, cdone)
	go parallelSeqTypedResizer(m, numEntries, false, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	for i := -numEntries + 1; i < numEntries; i++ {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	if s := m.Size(); s != 2*numEntries-1 {
		t.Fatalf("unexpected size: %v", s)
	}
}

func parallelRandTypedResizer(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
	//r := rand1.New(rand1.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		coin := rand.Int64N(2)
		for j := 0; j < numEntries; j++ {
			if coin == 1 {
				m.Store(strconv.Itoa(j), j)
			} else {
				m.Delete(strconv.Itoa(j))
			}
		}
	}
	cdone <- true
}

func TestMapOfParallelResize(t *testing.T) {
	const numIters = 1_000
	const numEntries = 2 * EntriesPerMapOfBucket * DefaultMinMapTableLen
	m := NewMapOf[string, int]()
	cdone := make(chan bool)
	go parallelRandTypedResizer(t, m, numIters, numEntries, cdone)
	go parallelRandTypedResizer(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			// The entry may be deleted and that's ok.
			continue
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	s := m.Size()
	if s > numEntries {
		t.Fatalf("unexpected size: %v", s)
	}
	rs := sizeBasedOnTypedRange(m)
	if s != rs {
		t.Fatalf("size does not match number of entries in Range: %v, %v", s, rs)
	}
}

func parallelRandTypedClearer(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
	//r := rand1.New(rand1.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		coin := rand.Int64N(2)
		for j := 0; j < numEntries; j++ {
			if coin == 1 {
				m.Store(strconv.Itoa(j), j)
			} else {
				m.Clear()
			}
		}
	}
	cdone <- true
}

func TestMapOfParallelClear(t *testing.T) {
	const numIters = 100
	const numEntries = 1_000
	m := NewMapOf[string, int]()
	cdone := make(chan bool)
	go parallelRandTypedClearer(t, m, numIters, numEntries, cdone)
	go parallelRandTypedClearer(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map size.
	s := m.Size()
	if s > numEntries {
		t.Fatalf("unexpected size: %v", s)
	}
	rs := sizeBasedOnTypedRange(m)
	if s != rs {
		t.Fatalf("size does not match number of entries in Range: %v, %v", s, rs)
	}
}

func parallelSeqTypedStorer(t *testing.T, m *MapOf[string, int], storeEach, numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			if storeEach == 0 || j%storeEach == 0 {
				m.Store(strconv.Itoa(j), j)
				// Due to atomic snapshots we must see a "<j>"/j pair.
				v, ok := m.Load(strconv.Itoa(j))
				if !ok {
					t.Errorf("value was not found for %d", j)
					break
				}
				if v != j {
					t.Errorf("value was not expected for %d: %d", j, v)
					break
				}
			}
		}
	}
	cdone <- true
}

func TestMapOfParallelStores(t *testing.T) {
	const numStorers = 4
	const numIters = 10_000
	const numEntries = 100
	m := NewMapOf[string, int]()
	cdone := make(chan bool)
	for i := 0; i < numStorers; i++ {
		go parallelSeqTypedStorer(t, m, i, numIters, numEntries, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numStorers; i++ {
		<-cdone
	}
	// Verify map contents.
	for i := 0; i < numEntries; i++ {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelRandTypedStorer(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
	//r := rand1.New(rand1.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := rand.IntN(numEntries)
		if v, loaded := m.LoadOrStore(strconv.Itoa(j), j); loaded {
			if v != j {
				t.Errorf("value was not expected for %d: %d", j, v)
			}
		}
	}
	cdone <- true
}

func parallelRandTypedDeleter(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
	//r := rand1.New(rand1.NewSource(time.Now().UnixNano()))
	for i := 0; i < numIters; i++ {
		j := rand.IntN(numEntries)
		if v, loaded := m.LoadAndDelete(strconv.Itoa(j)); loaded {
			if v != j {
				t.Errorf("value was not expected for %d: %d", j, v)
			}
		}
	}
	cdone <- true
}

func parallelTypedLoader(t *testing.T, m *MapOf[string, int], numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			// Due to atomic snapshots we must either see no entry, or a "<j>"/j pair.
			if v, ok := m.Load(strconv.Itoa(j)); ok {
				if v != j {
					t.Errorf("value was not expected for %d: %d", j, v)
				}
			}
		}
	}
	cdone <- true
}

func TestMapOfAtomicSnapshot(t *testing.T) {
	const numIters = 100_000
	const numEntries = 100
	m := NewMapOf[string, int]()
	cdone := make(chan bool)
	// Update or delete random entry in parallel with loads.
	go parallelRandTypedStorer(t, m, numIters, numEntries, cdone)
	go parallelRandTypedDeleter(t, m, numIters, numEntries, cdone)
	go parallelTypedLoader(t, m, numIters, numEntries, cdone)
	// Wait for the goroutines to finish.
	for i := 0; i < 3; i++ {
		<-cdone
	}
}

func TestMapOfParallelStoresAndDeletes(t *testing.T) {
	const numWorkers = 2
	const numIters = 100_000
	const numEntries = 1000
	m := NewMapOf[string, int]()
	cdone := make(chan bool)
	// Update random entry in parallel with deletes.
	for i := 0; i < numWorkers; i++ {
		go parallelRandTypedStorer(t, m, numIters, numEntries, cdone)
		go parallelRandTypedDeleter(t, m, numIters, numEntries, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < 2*numWorkers; i++ {
		<-cdone
	}
}

func parallelTypedComputer(m *MapOf[uint64, uint64], numIters, numEntries int, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			m.Compute(uint64(j), func(oldValue uint64, loaded bool) (newValue uint64, op ComputeOp) {
				return oldValue + 1, UpdateOp
			})
		}
	}
	cdone <- true
}

func TestMapOfParallelComputes(t *testing.T) {
	const numWorkers = 4 // Also stands for numEntries.
	const numIters = 10_000
	m := NewMapOf[uint64, uint64]()
	cdone := make(chan bool)
	for i := 0; i < numWorkers; i++ {
		go parallelTypedComputer(m, numIters, numWorkers, cdone)
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numWorkers; i++ {
		<-cdone
	}
	// Verify map contents.
	for i := 0; i < numWorkers; i++ {
		v, ok := m.Load(uint64(i))
		if !ok {
			t.Fatalf("value not found for %d", i)
		}
		if v != numWorkers*numIters {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
}

func parallelTypedRangeStorer(m *MapOf[int, int], numEntries int, stopFlag *int64, cdone chan bool) {
	for {
		for i := 0; i < numEntries; i++ {
			m.Store(i, i)
		}
		if atomic.LoadInt64(stopFlag) != 0 {
			break
		}
	}
	cdone <- true
}

func parallelTypedRangeDeleter(m *MapOf[int, int], numEntries int, stopFlag *int64, cdone chan bool) {
	for {
		for i := 0; i < numEntries; i++ {
			m.Delete(i)
		}
		if atomic.LoadInt64(stopFlag) != 0 {
			break
		}
	}
	cdone <- true
}

func TestMapOfParallelRange(t *testing.T) {
	const numEntries = 10_000
	m := NewMapOf[int, int](WithPresize(numEntries))
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	// Start goroutines that would be storing and deleting items in parallel.
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelTypedRangeStorer(m, numEntries, &stopFlag, cdone)
	go parallelTypedRangeDeleter(m, numEntries, &stopFlag, cdone)
	// Iterate the map and verify that no duplicate keys were met.
	met := make(map[int]int)
	m.Range(func(key int, value int) bool {
		if key != value {
			t.Fatalf("got unexpected value for key %d: %d", key, value)
			return false
		}
		met[key] += 1
		return true
	})
	if len(met) == 0 {
		t.Fatal("no entries were met when iterating")
	}
	for k, c := range met {
		if c != 1 {
			t.Fatalf("met key %d multiple times: %d", k, c)
		}
	}
	// Make sure that both goroutines finish.
	atomic.StoreInt64(&stopFlag, 1)
	<-cdone
	<-cdone
}

func parallelTypedShrinker(t *testing.T, m *MapOf[uint64, *point], numIters, numEntries int, stopFlag *int64, cdone chan bool) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			if p, loaded := m.LoadOrStore(uint64(j), &point{int32(j), int32(j)}); loaded {
				t.Errorf("value was present for %d: %v", j, p)
			}
		}
		for j := 0; j < numEntries; j++ {
			m.Delete(uint64(j))
		}
	}
	atomic.StoreInt64(stopFlag, 1)
	cdone <- true
}

func parallelTypedUpdater(t *testing.T, m *MapOf[uint64, *point], idx int, stopFlag *int64, cdone chan bool) {
	for atomic.LoadInt64(stopFlag) != 1 {
		sleepUs := int(rand.IntN(10))
		if p, loaded := m.LoadOrStore(uint64(idx), &point{int32(idx), int32(idx)}); loaded {
			t.Errorf("value was present for %d: %v", idx, p)
		}
		time.Sleep(time.Duration(sleepUs) * time.Microsecond)
		if _, ok := m.Load(uint64(idx)); !ok {
			t.Errorf("value was not found for %d", idx)
		}
		m.Delete(uint64(idx))
	}
	cdone <- true
}

func TestMapOfDoesNotLoseEntriesOnResize(t *testing.T) {
	const numIters = 10_000
	const numEntries = 128
	m := NewMapOf[uint64, *point]()
	cdone := make(chan bool)
	stopFlag := int64(0)
	go parallelTypedShrinker(t, m, numIters, numEntries, &stopFlag, cdone)
	go parallelTypedUpdater(t, m, numEntries, &stopFlag, cdone)
	// Wait for the goroutines to finish.
	<-cdone
	<-cdone
	// Verify map contents.
	if s := m.Size(); s != 0 {
		t.Fatalf("map is not empty: %d", s)
	}
}

func TestMapOfStats(t *testing.T) {
	m := NewMapOf[int, int]()

	stats := m.Stats()
	if stats.RootBuckets != DefaultMinMapTableLen {
		t.Fatalf("unexpected number of root buckets: %s", stats.ToString())
	}
	if stats.TotalBuckets != stats.RootBuckets {
		t.Fatalf("unexpected number of total buckets: %s", stats.ToString())
	}
	if stats.EmptyBuckets != stats.RootBuckets {
		t.Fatalf("unexpected number of empty buckets: %s", stats.ToString())
	}
	if stats.Capacity != EntriesPerMapOfBucket*DefaultMinMapTableLen {
		t.Fatalf("unexpected capacity: %s", stats.ToString())
	}
	if stats.Size != 0 {
		t.Fatalf("unexpected size: %s", stats.ToString())
	}
	if stats.Counter != 0 {
		t.Fatalf("unexpected counter: %s", stats.ToString())
	}
	if stats.CounterLen != 1 {
		t.Fatalf("unexpected counter length: %s", stats.ToString())
	}

	for i := 0; i < 200; i++ {
		m.Store(i, i)
	}

	stats = m.Stats()
	if stats.RootBuckets > 2*DefaultMinMapTableLen {
		t.Fatalf("unexpected number of root buckets: %s", stats.ToString())
	}
	if stats.TotalBuckets < stats.RootBuckets {
		t.Fatalf("unexpected number of total buckets: %s", stats.ToString())
	}
	if stats.EmptyBuckets >= stats.RootBuckets {
		t.Fatalf("unexpected number of empty buckets: %s", stats.ToString())
	}
	if stats.Capacity != EntriesPerMapOfBucket*stats.TotalBuckets {
		t.Fatalf("unexpected capacity: %s", stats.ToString())
	}
	if stats.Size != 200 {
		t.Fatalf("unexpected size: %s", stats.ToString())
	}
	if stats.Counter != 200 {
		t.Fatalf("unexpected counter: %s", stats.ToString())
	}
	if stats.CounterLen != 1 {
		t.Fatalf("unexpected counter length: %s", stats.ToString())
	}
}

//
//func TestToPlainMapOf_NilPointer(t *testing.T) {
//	pm := ToPlainMapOf[int, int](nil)
//	if len(pm) != 0 {
//		t.Fatalf("got unexpected size of nil map copy: %d", len(pm))
//	}
//}

func TestToPlainMapOf(t *testing.T) {
	const numEntries = 1000
	m := NewMapOf[int, int]()
	for i := 0; i < numEntries; i++ {
		m.Store(i, i)
	}
	pm := m.ToMap()
	if len(pm) != numEntries {
		t.Fatalf("got unexpected size of nil map copy: %d", len(pm))
	}
	for i := 0; i < numEntries; i++ {
		if v := pm[i]; v != i {
			t.Fatalf("unexpected value for key %d: %d", i, v)
		}
	}
}

func BenchmarkMapOf_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewMapOf[string, int]()
			benchmarkMapOfStringKeys(b, func(k string) (int, bool) {
				return m.Load(k)
			}, func(k string, v int) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkMapOf_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewMapOf[string, int](WithPresize(benchmarkNumEntries))
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(benchmarkKeyPrefix+strconv.Itoa(i), i)
			}
			b.ResetTimer()
			benchmarkMapOfStringKeys(b, func(k string) (int, bool) {
				return m.Load(k)
			}, func(k string, v int) {
				m.Store(k, v)
			}, func(k string) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func benchmarkMapOfStringKeys(
	b *testing.B,
	loadFn func(k string) (int, bool),
	storeFn func(k string, v int),
	deleteFn func(k string),
	readPercentage int,
) {
	runParallel(b, func(pb *testing.PB) {
		// convert percent to permille to support 99% case
		storeThreshold := 10 * readPercentage
		deleteThreshold := 10*readPercentage + ((1000 - 10*readPercentage) / 2)
		for pb.Next() {
			op := int(rand.Int() % 1000)
			i := int(rand.Int() % benchmarkNumEntries)
			if op >= deleteThreshold {
				deleteFn(benchmarkKeys[i])
			} else if op >= storeThreshold {
				storeFn(benchmarkKeys[i], i)
			} else {
				loadFn(benchmarkKeys[i])
			}
		}
	})
}

func BenchmarkMapOfInt_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			m := NewMapOf[int, int]()
			benchmarkMapOfIntKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkMapOfInt_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewMapOf[int, int](WithPresize(benchmarkNumEntries))
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapOfIntKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkMapOfInt_Murmur3Finalizer_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			m := NewMapOfWithHasher[int, int](murmur3Finalizer, nil, WithPresize(benchmarkNumEntries))
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapOfIntKeys(b, func(k int) (int, bool) {
				return m.Load(k)
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func BenchmarkIntMapStandard_NoWarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		if bc.readPercentage == 100 {
			// This benchmark doesn't make sense without a warm-up.
			continue
		}
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			benchmarkMapOfIntKeys(b, func(k int) (value int, ok bool) {
				v, ok := m.Load(k)
				if ok {
					return v.(int), ok
				} else {
					return 0, false
				}
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

// This is a nice scenario for sync.Map since a lot of updates
// will hit the readOnly part of the map.
func BenchmarkIntMapStandard_WarmUp(b *testing.B) {
	for _, bc := range benchmarkCases {
		b.Run(bc.name, func(b *testing.B) {
			var m sync.Map
			for i := 0; i < benchmarkNumEntries; i++ {
				m.Store(i, i)
			}
			b.ResetTimer()
			benchmarkMapOfIntKeys(b, func(k int) (value int, ok bool) {
				v, ok := m.Load(k)
				if ok {
					return v.(int), ok
				} else {
					return 0, false
				}
			}, func(k int, v int) {
				m.Store(k, v)
			}, func(k int) {
				m.Delete(k)
			}, bc.readPercentage)
		})
	}
}

func benchmarkMapOfIntKeys(
	b *testing.B,
	loadFn func(k int) (int, bool),
	storeFn func(k int, v int),
	deleteFn func(k int),
	readPercentage int,
) {
	runParallel(b, func(pb *testing.PB) {
		// convert percent to permille to support 99% case
		storeThreshold := 10 * readPercentage
		deleteThreshold := 10*readPercentage + ((1000 - 10*readPercentage) / 2)
		for pb.Next() {
			op := int(rand.IntN(1000))
			i := int(rand.IntN(benchmarkNumEntries))
			if op >= deleteThreshold {
				deleteFn(i)
			} else if op >= storeThreshold {
				storeFn(i, i)
			} else {
				loadFn(i)
			}
		}
	})
}

func BenchmarkMapOfRange(b *testing.B) {
	m := NewMapOf[string, int](WithPresize(benchmarkNumEntries))
	for i := 0; i < benchmarkNumEntries; i++ {
		m.Store(benchmarkKeys[i], i)
	}
	b.ResetTimer()
	runParallel(b, func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			m.Range(func(key string, value int) bool {
				foo++
				return true
			})
			_ = foo
		}
	})
}

func runParallel(b *testing.B, benchFn func(pb *testing.PB)) {
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(benchFn)
	opsPerSec := float64(b.N) / float64(time.Since(start).Seconds())
	b.ReportMetric(opsPerSec, "ops/s")
}

const (
	// number of entries to use in benchmarks
	benchmarkNumEntries = 1_000
	// key prefix used in benchmarks
	benchmarkKeyPrefix = "what_a_looooooooooooooooooooooong_key_prefix_"
)
const (
	//entriesPerMapBucket     = 3
	//EntriesPerMapBucket     = entriesPerMapBucket
	EntriesPerMapOfBucket = entriesPerMapOfBucket
	MapLoadFactor         = mapLoadFactor
	DefaultMinMapTableLen = defaultMinMapTableLen
	//DefaultMinMapTableCap   = defaultMinMapTableLen * entriesPerMapBucket
	DefaultMinMapOfTableCap = defaultMinMapTableLen * entriesPerMapOfBucket
)

var benchmarkKeys []string

func init() {
	benchmarkKeys = make([]string, benchmarkNumEntries)
	for i := 0; i < benchmarkNumEntries; i++ {
		benchmarkKeys[i] = benchmarkKeyPrefix + strconv.Itoa(i)
	}
}

var benchmarkCases = []struct {
	name           string
	readPercentage int
}{
	{"reads=100%", 100}, // 100% loads,    0% stores,    0% deletes
	{"reads=99%", 99},   //  99% loads,  0.5% stores,  0.5% deletes
	{"reads=90%", 90},   //  90% loads,    5% stores,    5% deletes
	{"reads=75%", 75},   //  75% loads, 12.5% stores, 12.5% deletes
}

//----------------------------------------------------------------

// TestMapOfClone tests the Clone function of MapOf
func TestMapOfClone(t *testing.T) {
	// Test with empty map
	t.Run("EmptyMap", func(t *testing.T) {
		m := NewMapOf[string, int]()
		clone := m.Clone()
		if !clone.IsZero() {
			t.Fatalf("expected cloned empty map to be zero, got non-zero")
		}
		if clone.Size() != 0 {
			t.Fatalf("expected cloned empty map size to be 0, got: %d", clone.Size())
		}
	})

	// Test with populated map
	t.Run("PopulatedMap", func(t *testing.T) {
		const numEntries = 1000
		m := NewMapOf[string, int]()
		for i := 0; i < numEntries; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		clone := m.Clone()

		// Verify size
		if clone.Size() != numEntries {
			t.Fatalf("expected cloned map size to be %d, got: %d", numEntries, clone.Size())
		}

		// Verify all entries were copied correctly
		for i := 0; i < numEntries; i++ {
			key := strconv.Itoa(i)
			val, ok := clone.Load(key)
			if !ok {
				t.Fatalf("key %s missing in cloned map", key)
			}
			if val != i {
				t.Fatalf("expected value %d for key %s, got: %d", i, key, val)
			}
		}

		// Verify independence - modifying original should not affect clone
		m.Store("new", 9999)
		if _, ok := clone.Load("new"); ok {
			t.Fatalf("clone should not be affected by changes to original map")
		}

		// Verify independence - modifying clone should not affect original
		clone.Store("clone-only", 8888)
		if _, ok := m.Load("clone-only"); ok {
			t.Fatalf("original should not be affected by changes to cloned map")
		}
	})
}

// TestMapOfMerge tests the Merge function of MapOf
func TestMapOfMerge(t *testing.T) {
	// Test merging with nil map
	t.Run("NilMap", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("a", 1)
		m.Store("b", 2)

		// Merge with nil should not change the map
		m.Merge(nil, nil)

		if m.Size() != 2 {
			t.Fatalf("expected map size to remain 2 after merging with nil, got: %d", m.Size())
		}
		expectPresentMapOf(t, "a", 1)(m.Load("a"))
		expectPresentMapOf(t, "b", 2)(m.Load("b"))
	})

	// Test merging with empty map
	t.Run("EmptyMap", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("a", 1)
		m.Store("b", 2)

		empty := NewMapOf[string, int]()

		// Merge with empty should not change the map
		m.Merge(empty, nil)

		if m.Size() != 2 {
			t.Fatalf("expected map size to remain 2 after merging with empty map, got: %d", m.Size())
		}
		expectPresentMapOf(t, "a", 1)(m.Load("a"))
		expectPresentMapOf(t, "b", 2)(m.Load("b"))
	})

	// Test merging with non-overlapping maps
	t.Run("NonOverlapping", func(t *testing.T) {
		m1 := NewMapOf[string, int]()
		m1.Store("a", 1)
		m1.Store("b", 2)

		m2 := NewMapOf[string, int]()
		m2.Store("c", 3)
		m2.Store("d", 4)

		// Merge non-overlapping maps
		m1.Merge(m2, nil)

		if m1.Size() != 4 {
			t.Fatalf("expected merged map size to be 4, got: %d", m1.Size())
		}
		expectPresentMapOf(t, "a", 1)(m1.Load("a"))
		expectPresentMapOf(t, "b", 2)(m1.Load("b"))
		expectPresentMapOf(t, "c", 3)(m1.Load("c"))
		expectPresentMapOf(t, "d", 4)(m1.Load("d"))
	})

	// Test merging with overlapping maps and default conflict resolution
	t.Run("OverlappingDefaultConflict", func(t *testing.T) {
		m1 := NewMapOf[string, int]()
		m1.Store("a", 1)
		m1.Store("b", 2)
		m1.Store("c", 30) // Will be overwritten

		m2 := NewMapOf[string, int]()
		m2.Store("c", 3) // Overlaps with m1
		m2.Store("d", 4)

		// Merge with default conflict resolution (use value from other map)
		m1.Merge(m2, nil)

		if m1.Size() != 4 {
			t.Fatalf("expected merged map size to be 4, got: %d", m1.Size())
		}
		expectPresentMapOf(t, "a", 1)(m1.Load("a"))
		expectPresentMapOf(t, "b", 2)(m1.Load("b"))
		expectPresentMapOf(t, "c", 3)(m1.Load("c")) // Should be overwritten with value from m2
		expectPresentMapOf(t, "d", 4)(m1.Load("d"))
	})

	// Test merging with overlapping maps and custom conflict resolution
	t.Run("OverlappingCustomConflict", func(t *testing.T) {
		m1 := NewMapOf[string, int]()
		m1.Store("a", 1)
		m1.Store("b", 2)
		m1.Store("c", 30)

		m2 := NewMapOf[string, int]()
		m2.Store("c", 3) // Overlaps with m1
		m2.Store("d", 4)

		// Custom conflict resolution: sum the values
		m1.Merge(m2, func(this, other *EntryOf[string, int]) *EntryOf[string, int] {
			return &EntryOf[string, int]{Value: this.Value + other.Value}
		})

		if m1.Size() != 4 {
			t.Fatalf("expected merged map size to be 4, got: %d", m1.Size())
		}
		expectPresentMapOf(t, "a", 1)(m1.Load("a"))
		expectPresentMapOf(t, "b", 2)(m1.Load("b"))
		expectPresentMapOf(t, "c", 33)(m1.Load("c")) // Should be sum of values (30+3)
		expectPresentMapOf(t, "d", 4)(m1.Load("d"))
	})
}

// TestMapOfFilterAndTransform tests the FilterAndTransform function of MapOf
func TestMapOfFilterAndTransform(t *testing.T) {
	// Test with empty map
	t.Run("EmptyMap", func(t *testing.T) {
		m := NewMapOf[string, int]()

		// Should not panic with empty map
		m.FilterAndTransform(
			func(key string, value int) bool { return true },
			func(key string, value int) (int, bool) { return value * 2, true },
		)

		if m.Size() != 0 {
			t.Fatalf("expected empty map to remain empty, got size: %d", m.Size())
		}
	})

	// Test filtering only (no transformation)
	t.Run("FilterOnly", func(t *testing.T) {
		m := NewMapOf[string, int]()
		for i := 0; i < 10; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		// Keep only even numbers
		m.FilterAndTransform(
			func(key string, value int) bool { return value%2 == 0 },
			nil, // No transformation
		)

		if m.Size() != 5 {
			t.Fatalf("expected filtered map size to be 5, got: %d", m.Size())
		}

		// Verify only even numbers remain
		m.Range(func(key string, value int) bool {
			if value%2 != 0 {
				t.Fatalf("expected only even values, found odd value: %d", value)
			}
			return true
		})

		// Check specific values
		expectPresentMapOf(t, "0", 0)(m.Load("0"))
		expectPresentMapOf(t, "2", 2)(m.Load("2"))
		expectMissingMapOf(t, "1", 0)(m.Load("1"))
		expectMissingMapOf(t, "3", 0)(m.Load("3"))
	})

	// Test transformation only (no filtering)
	t.Run("TransformOnly", func(t *testing.T) {
		m := NewMapOf[string, int]()
		for i := 0; i < 5; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		// Double all values
		m.FilterAndTransform(
			func(key string, value int) bool { return true }, // Keep all
			func(key string, value int) (int, bool) { return value * 2, true },
		)

		if m.Size() != 5 {
			t.Fatalf("expected map size to remain 5, got: %d", m.Size())
		}

		// Verify all values are doubled
		for i := 0; i < 5; i++ {
			key := strconv.Itoa(i)
			expectPresentMapOf(t, key, i*2)(m.Load(key))
		}
	})

	// Test both filtering and transformation
	t.Run("FilterAndTransform", func(t *testing.T) {
		m := NewMapOf[string, int]()
		for i := 0; i < 10; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		// Keep only even numbers and double them
		m.FilterAndTransform(
			func(key string, value int) bool { return value%2 == 0 },
			func(key string, value int) (int, bool) { return value * 2, true },
		)

		if m.Size() != 5 {
			t.Fatalf("expected filtered map size to be 5, got: %d", m.Size())
		}

		// Verify only even numbers remain and they're doubled
		expectedValues := map[string]int{
			"0": 0,  // 0*2
			"2": 4,  // 2*2
			"4": 8,  // 4*2
			"6": 12, // 6*2
			"8": 16, // 8*2
		}

		for key, expected := range expectedValues {
			expectPresentMapOf(t, key, expected)(m.Load(key))
		}

		// Check odd numbers are removed
		for i := 1; i < 10; i += 2 {
			expectMissingMapOf(t, strconv.Itoa(i), 0)(m.Load(strconv.Itoa(i)))
		}
	})

	// Test selective transformation (only transform some values)
	t.Run("SelectiveTransform", func(t *testing.T) {
		m := NewMapOf[string, int]()
		for i := 0; i < 10; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		// Keep all numbers but only double even ones
		m.FilterAndTransform(
			func(key string, value int) bool { return true }, // Keep all
			func(key string, value int) (int, bool) {
				if value%2 == 0 {
					return value * 2, true // Transform even numbers
				}
				return value, false // Don't transform odd numbers
			},
		)

		if m.Size() != 10 {
			t.Fatalf("expected map size to remain 10, got: %d", m.Size())
		}

		// Verify even numbers are doubled, odd numbers unchanged
		for i := 0; i < 10; i++ {
			key := strconv.Itoa(i)
			if i%2 == 0 {
				expectPresentMapOf(t, key, i*2)(m.Load(key)) // Even numbers doubled
			} else {
				expectPresentMapOf(t, key, i)(m.Load(key)) // Odd numbers unchanged
			}
		}
	})
}

// TestMapOfFromMap tests the FromMap function of MapOf
func TestMapOfFromMap(t *testing.T) {
	// Test with empty map
	t.Run("EmptyMap", func(t *testing.T) {
		m := NewMapOf[string, int]()
		source := map[string]int{}

		m.FromMap(source)

		if m.Size() != 0 {
			t.Fatalf("expected map size to be 0, got: %d", m.Size())
		}
	})

	// Test with nil map
	t.Run("NilMap", func(t *testing.T) {
		m := NewMapOf[string, int]()
		var source map[string]int = nil

		m.FromMap(source)

		if m.Size() != 0 {
			t.Fatalf("expected map size to be 0, got: %d", m.Size())
		}
	})

	// Test with populated map
	t.Run("PopulatedMap", func(t *testing.T) {
		m := NewMapOf[string, int]()
		source := map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		}

		m.FromMap(source)

		if m.Size() != 3 {
			t.Fatalf("expected map size to be 3, got: %d", m.Size())
		}

		expectPresentMapOf(t, "a", 1)(m.Load("a"))
		expectPresentMapOf(t, "b", 2)(m.Load("b"))
		expectPresentMapOf(t, "c", 3)(m.Load("c"))
	})

	// Test with existing data (should overwrite)
	t.Run("OverwriteExisting", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("a", 100) // Will be overwritten
		m.Store("b", 200) // Will be overwritten
		m.Store("d", 400) // Will remain

		source := map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		}

		m.FromMap(source)

		if m.Size() != 4 {
			t.Fatalf("expected map size to be 4, got: %d", m.Size())
		}

		// Check overwritten values
		expectPresentMapOf(t, "a", 1)(m.Load("a"))
		expectPresentMapOf(t, "b", 2)(m.Load("b"))

		// Check new value
		expectPresentMapOf(t, "c", 3)(m.Load("c"))

		// Check existing value not in source
		expectPresentMapOf(t, "d", 400)(m.Load("d"))
	})

	// Test with large map
	t.Run("LargeMap", func(t *testing.T) {
		m := NewMapOf[string, int]()
		source := make(map[string]int, 1000)

		for i := 0; i < 1000; i++ {
			source[strconv.Itoa(i)] = i
		}

		m.FromMap(source)

		if m.Size() != 1000 {
			t.Fatalf("expected map size to be 1000, got: %d", m.Size())
		}

		// Check random samples
		for _, i := range []int{0, 42, 99, 500, 999} {
			key := strconv.Itoa(i)
			expectPresentMapOf(t, key, i)(m.Load(key))
		}
	})
}

// TestMapOfBatchUpsert tests the BatchUpsert function of MapOf
func TestMapOfBatchUpsert(t *testing.T) {
	// Test with empty entries slice
	t.Run("EmptyEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		entries := []EntryOf[string, int]{}

		previous, loaded := m.BatchUpsert(entries)

		if len(previous) != 0 {
			t.Fatalf("expected empty previous values slice, got length: %d", len(previous))
		}
		if len(loaded) != 0 {
			t.Fatalf("expected empty loaded slice, got length: %d", len(loaded))
		}
	})

	// Test with new entries (no existing keys)
	t.Run("NewEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		entries := []EntryOf[string, int]{
			{Key: "a", Value: 1},
			{Key: "b", Value: 2},
			{Key: "c", Value: 3},
		}

		previous, loaded := m.BatchUpsert(entries)

		// Check return values
		if len(previous) != 3 {
			t.Fatalf("expected previous values slice length 3, got: %d", len(previous))
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check all entries should have zero values and loaded=false
		for i, val := range previous {
			if val != 0 {
				t.Fatalf("expected zero value for new entry at index %d, got: %d", i, val)
			}
			if loaded[i] {
				t.Fatalf("expected loaded[%d] to be false for new entry", i)
			}
		}

		// Verify map contents
		if m.Size() != 3 {
			t.Fatalf("expected map size to be 3, got: %d", m.Size())
		}
		expectPresentMapOf(t, "a", 1)(m.Load("a"))
		expectPresentMapOf(t, "b", 2)(m.Load("b"))
		expectPresentMapOf(t, "c", 3)(m.Load("c"))
	})

	// Test with existing entries (updating existing keys)
	t.Run("ExistingEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate the map
		m.Store("a", 100)
		m.Store("b", 200)

		entries := []EntryOf[string, int]{
			{Key: "a", Value: 1}, // Will update
			{Key: "b", Value: 2}, // Will update
			{Key: "c", Value: 3}, // Will insert
		}

		previous, loaded := m.BatchUpsert(entries)

		// Check return values
		if len(previous) != 3 {
			t.Fatalf("expected previous values slice length 3, got: %d", len(previous))
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check previous values and loaded status
		expectedPrevious := []int{100, 200, 0}
		expectedLoaded := []bool{true, true, false}

		for i := range entries {
			if previous[i] != expectedPrevious[i] {
				t.Fatalf("expected previous[%d] to be %d, got: %d", i, expectedPrevious[i], previous[i])
			}
			if loaded[i] != expectedLoaded[i] {
				t.Fatalf("expected loaded[%d] to be %v, got: %v", i, expectedLoaded[i], loaded[i])
			}
		}

		// Verify map contents
		if m.Size() != 3 {
			t.Fatalf("expected map size to be 3, got: %d", m.Size())
		}
		expectPresentMapOf(t, "a", 1)(m.Load("a"))
		expectPresentMapOf(t, "b", 2)(m.Load("b"))
		expectPresentMapOf(t, "c", 3)(m.Load("c"))
	})

	// Test with parallel processing
	t.Run("ParallelProcessing", func(t *testing.T) {
		m := NewMapOf[string, int]()
		const numEntries = 1000
		entries := make([]EntryOf[string, int], numEntries)

		for i := 0; i < numEntries; i++ {
			entries[i] = EntryOf[string, int]{Key: strconv.Itoa(i), Value: i}
		}

		// Use parallel processing (negative value uses CPU count)
		previous, loaded := m.BatchUpsert(entries)

		// Check return values
		if len(previous) != numEntries {
			t.Fatalf("expected previous values slice length %d, got: %d", numEntries, len(previous))
		}
		if len(loaded) != numEntries {
			t.Fatalf("expected loaded slice length %d, got: %d", numEntries, len(loaded))
		}

		// All entries should be new
		for i := 0; i < numEntries; i++ {
			if loaded[i] {
				t.Fatalf("expected loaded[%d] to be false for new entry", i)
			}
		}

		// Verify map size
		if m.Size() != numEntries {
			t.Fatalf("expected map size to be %d, got: %d", numEntries, m.Size())
		}

		// Check random samples
		for _, i := range []int{0, 42, 99, 500, 999} {
			key := strconv.Itoa(i)
			expectPresentMapOf(t, key, i)(m.Load(key))
		}
	})
}

// TestMapOfBatchInsert tests the BatchInsert function of MapOf
func TestMapOfBatchInsert(t *testing.T) {
	// Test with empty entries slice
	t.Run("EmptyEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		entries := []EntryOf[string, int]{}

		actual, loaded := m.BatchInsert(entries)

		if len(actual) != 0 {
			t.Fatalf("expected empty actual values slice, got length: %d", len(actual))
		}
		if len(loaded) != 0 {
			t.Fatalf("expected empty loaded slice, got length: %d", len(loaded))
		}
	})

	// Test with new entries (no existing keys)
	t.Run("NewEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		entries := []EntryOf[string, int]{
			{Key: "a", Value: 1},
			{Key: "b", Value: 2},
			{Key: "c", Value: 3},
		}

		actual, loaded := m.BatchInsert(entries)

		// Check return values
		if len(actual) != 3 {
			t.Fatalf("expected actual values slice length 3, got: %d", len(actual))
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check all entries should have inserted values and loaded=false
		for i, entry := range entries {
			if actual[i] != entry.Value {
				t.Fatalf("expected actual[%d] to be %d, got: %d", i, entry.Value, actual[i])
			}
			if loaded[i] {
				t.Fatalf("expected loaded[%d] to be false for new entry", i)
			}
		}

		// Verify map contents
		if m.Size() != 3 {
			t.Fatalf("expected map size to be 3, got: %d", m.Size())
		}
		expectPresentMapOf(t, "a", 1)(m.Load("a"))
		expectPresentMapOf(t, "b", 2)(m.Load("b"))
		expectPresentMapOf(t, "c", 3)(m.Load("c"))
	})

	// Test with existing entries (not modifying existing keys)
	t.Run("ExistingEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate the map
		m.Store("a", 100)
		m.Store("b", 200)

		entries := []EntryOf[string, int]{
			{Key: "a", Value: 1}, // Should not update
			{Key: "b", Value: 2}, // Should not update
			{Key: "c", Value: 3}, // Will insert
		}

		actual, loaded := m.BatchInsert(entries)

		// Check return values
		if len(actual) != 3 {
			t.Fatalf("expected actual values slice length 3, got: %d", len(actual))
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check actual values and loaded status
		expectedActual := []int{100, 200, 3} // Existing values for a,b; new value for c
		expectedLoaded := []bool{true, true, false}

		for i := range entries {
			if actual[i] != expectedActual[i] {
				t.Fatalf("expected actual[%d] to be %d, got: %d", i, expectedActual[i], actual[i])
			}
			if loaded[i] != expectedLoaded[i] {
				t.Fatalf("expected loaded[%d] to be %v, got: %v", i, expectedLoaded[i], loaded[i])
			}
		}

		// Verify map contents - existing keys should not be modified
		if m.Size() != 3 {
			t.Fatalf("expected map size to be 3, got: %d", m.Size())
		}
		expectPresentMapOf(t, "a", 100)(m.Load("a")) // Still 100, not 1
		expectPresentMapOf(t, "b", 200)(m.Load("b")) // Still 200, not 2
		expectPresentMapOf(t, "c", 3)(m.Load("c"))   // New entry
	})

	// Test with parallel processing
	t.Run("ParallelProcessing", func(t *testing.T) {
		m := NewMapOf[string, int]()
		const numEntries = 1000
		entries := make([]EntryOf[string, int], numEntries)

		for i := 0; i < numEntries; i++ {
			entries[i] = EntryOf[string, int]{Key: strconv.Itoa(i), Value: i}
		}

		// Use parallel processing (negative value uses CPU count)
		actual, loaded := m.BatchInsert(entries)

		// Check return values
		if len(actual) != numEntries {
			t.Fatalf("expected actual values slice length %d, got: %d", numEntries, len(actual))
		}
		if len(loaded) != numEntries {
			t.Fatalf("expected loaded slice length %d, got: %d", numEntries, len(loaded))
		}

		// All entries should be new
		for i := 0; i < numEntries; i++ {
			if loaded[i] {
				t.Fatalf("expected loaded[%d] to be false for new entry", i)
			}
			if actual[i] != i {
				t.Fatalf("expected actual[%d] to be %d, got: %d", i, i, actual[i])
			}
		}

		// Verify map size
		if m.Size() != numEntries {
			t.Fatalf("expected map size to be %d, got: %d", numEntries, m.Size())
		}

		// Check random samples
		for _, i := range []int{0, 42, 99, 500, 999} {
			key := strconv.Itoa(i)
			expectPresentMapOf(t, key, i)(m.Load(key))
		}
	})
}

// TestMapOfBatchDelete tests the BatchDelete function of MapOf
func TestMapOfBatchDelete(t *testing.T) {
	// Test with empty keys slice
	t.Run("EmptyKeys", func(t *testing.T) {
		m := NewMapOf[string, int]()
		keys := []string{}

		previous, loaded := m.BatchDelete(keys)

		if len(previous) != 0 {
			t.Fatalf("expected empty previous values slice, got length: %d", len(previous))
		}
		if len(loaded) != 0 {
			t.Fatalf("expected empty loaded slice, got length: %d", len(loaded))
		}
	})

	// Test with non-existent keys
	t.Run("NonExistentKeys", func(t *testing.T) {
		m := NewMapOf[string, int]()
		keys := []string{"a", "b", "c"}

		previous, loaded := m.BatchDelete(keys)

		// Check return values
		if len(previous) != 3 {
			t.Fatalf("expected previous values slice length 3, got: %d", len(previous))
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// All entries should have zero values and loaded=false
		for i := range keys {
			if previous[i] != 0 {
				t.Fatalf("expected previous[%d] to be 0 for non-existent key, got: %d", i, previous[i])
			}
			if loaded[i] {
				t.Fatalf("expected loaded[%d] to be false for non-existent key", i)
			}
		}

		// Map should remain empty
		if m.Size() != 0 {
			t.Fatalf("expected map size to be 0, got: %d", m.Size())
		}
	})

	// Test with existing keys
	t.Run("ExistingKeys", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate the map
		m.Store("a", 1)
		m.Store("b", 2)
		m.Store("c", 3)
		m.Store("d", 4)

		keys := []string{"a", "c", "e"} // a,c exist; e doesn't

		previous, loaded := m.BatchDelete(keys)

		// Check return values
		if len(previous) != 3 {
			t.Fatalf("expected previous values slice length 3, got: %d", len(previous))
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check previous values and loaded status
		expectedPrevious := []int{1, 3, 0}
		expectedLoaded := []bool{true, true, false}

		for i := range keys {
			if previous[i] != expectedPrevious[i] {
				t.Fatalf("expected previous[%d] to be %d, got: %d", i, expectedPrevious[i], previous[i])
			}
			if loaded[i] != expectedLoaded[i] {
				t.Fatalf("expected loaded[%d] to be %v, got: %v", i, expectedLoaded[i], loaded[i])
			}
		}

		// Verify map contents - deleted keys should be gone
		if m.Size() != 2 {
			t.Fatalf("expected map size to be 2, got: %d", m.Size())
		}
		expectMissingMapOf(t, "a", 0)(m.Load("a")) // Deleted
		expectPresentMapOf(t, "b", 2)(m.Load("b")) // Still present
		expectMissingMapOf(t, "c", 0)(m.Load("c")) // Deleted
		expectPresentMapOf(t, "d", 4)(m.Load("d")) // Still present
	})

	// Test with parallel processing
	t.Run("ParallelProcessing", func(t *testing.T) {
		m := NewMapOf[string, int]()
		const numEntries = 1000

		// Pre-populate the map
		for i := 0; i < numEntries; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		// Delete even-numbered keys
		keys := make([]string, numEntries/2)
		for i := 0; i < numEntries; i += 2 {
			keys[i/2] = strconv.Itoa(i)
		}

		// Use parallel processing (negative value uses CPU count)
		previous, loaded := m.BatchDelete(keys)

		// Check return values
		if len(previous) != numEntries/2 {
			t.Fatalf("expected previous values slice length %d, got: %d", numEntries/2, len(previous))
		}
		if len(loaded) != numEntries/2 {
			t.Fatalf("expected loaded slice length %d, got: %d", numEntries/2, len(loaded))
		}

		// All deleted entries should have correct values and loaded=true
		for i := 0; i < numEntries/2; i++ {
			expectedValue := i * 2 // Even numbers
			if previous[i] != expectedValue {
				t.Fatalf("expected previous[%d] to be %d, got: %d", i, expectedValue, previous[i])
			}
			if !loaded[i] {
				t.Fatalf("expected loaded[%d] to be true for existing key", i)
			}
		}

		// Verify map size
		if m.Size() != numEntries/2 {
			t.Fatalf("expected map size to be %d, got: %d", numEntries/2, m.Size())
		}

		// Check random samples - even numbers should be gone, odd numbers present
		for i := 0; i < 20; i++ {
			key := strconv.Itoa(i)
			if i%2 == 0 {
				expectMissingMapOf(t, key, 0)(m.Load(key)) // Even numbers deleted
			} else {
				expectPresentMapOf(t, key, i)(m.Load(key)) // Odd numbers present
			}
		}
	})
}

// TestMapOfBatchUpdate tests the BatchUpdate function of MapOf
func TestMapOfBatchUpdate(t *testing.T) {
	// Test with empty entries slice
	t.Run("EmptyEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		entries := []EntryOf[string, int]{}

		previous, loaded := m.BatchUpdate(entries)

		if len(previous) != 0 {
			t.Fatalf("expected empty previous values slice, got length: %d", len(previous))
		}
		if len(loaded) != 0 {
			t.Fatalf("expected empty loaded slice, got length: %d", len(loaded))
		}
	})

	// Test with non-existent keys
	t.Run("NonExistentKeys", func(t *testing.T) {
		m := NewMapOf[string, int]()
		entries := []EntryOf[string, int]{
			{Key: "a", Value: 1},
			{Key: "b", Value: 2},
			{Key: "c", Value: 3},
		}

		previous, loaded := m.BatchUpdate(entries)

		// Check return values
		if len(previous) != 3 {
			t.Fatalf("expected previous values slice length 3, got: %d", len(previous))
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// All entries should have zero values and loaded=false
		for i := range entries {
			if previous[i] != 0 {
				t.Fatalf("expected previous[%d] to be 0 for non-existent key, got: %d", i, previous[i])
			}
			if loaded[i] {
				t.Fatalf("expected loaded[%d] to be false for non-existent key", i)
			}
		}

		// Map should remain empty (update only affects existing keys)
		if m.Size() != 0 {
			t.Fatalf("expected map size to be 0, got: %d", m.Size())
		}
	})

	// Test with existing keys
	t.Run("ExistingKeys", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate the map
		m.Store("a", 100)
		m.Store("b", 200)
		m.Store("d", 400)

		entries := []EntryOf[string, int]{
			{Key: "a", Value: 1}, // Will update
			{Key: "b", Value: 2}, // Will update
			{Key: "c", Value: 3}, // Won't update (doesn't exist)
			{Key: "d", Value: 4}, // Will update
		}

		previous, loaded := m.BatchUpdate(entries)

		// Check return values
		if len(previous) != 4 {
			t.Fatalf("expected previous values slice length 4, got: %d", len(previous))
		}
		if len(loaded) != 4 {
			t.Fatalf("expected loaded slice length 4, got: %d", len(loaded))
		}

		// Check previous values and loaded status
		expectedPrevious := []int{100, 200, 0, 400}
		expectedLoaded := []bool{true, true, false, true}

		for i := range entries {
			if previous[i] != expectedPrevious[i] {
				t.Fatalf("expected previous[%d] to be %d, got: %d", i, expectedPrevious[i], previous[i])
			}
			if loaded[i] != expectedLoaded[i] {
				t.Fatalf("expected loaded[%d] to be %v, got: %v", i, expectedLoaded[i], loaded[i])
			}
		}

		// Verify map contents - only existing keys should be updated
		if m.Size() != 3 {
			t.Fatalf("expected map size to be 3, got: %d", m.Size())
		}
		expectPresentMapOf(t, "a", 1)(m.Load("a")) // Updated
		expectPresentMapOf(t, "b", 2)(m.Load("b")) // Updated
		expectMissingMapOf(t, "c", 0)(m.Load("c")) // Not inserted
		expectPresentMapOf(t, "d", 4)(m.Load("d")) // Updated
	})

	// Test with parallel processing
	t.Run("ParallelProcessing", func(t *testing.T) {
		m := NewMapOf[string, int]()
		const numEntries = 1000

		// Pre-populate the map with odd-numbered keys
		for i := 1; i < numEntries; i += 2 {
			m.Store(strconv.Itoa(i), i*10) // Store with a different value to check updates
		}

		// Create entries for all keys (both odd and even)
		entries := make([]EntryOf[string, int], numEntries)
		for i := 0; i < numEntries; i++ {
			entries[i] = EntryOf[string, int]{Key: strconv.Itoa(i), Value: i}
		}

		// Use parallel processing (negative value uses CPU count)
		previous, loaded := m.BatchUpdate(entries)

		// Check return values
		if len(previous) != numEntries {
			t.Fatalf("expected previous values slice length %d, got: %d", numEntries, len(previous))
		}
		if len(loaded) != numEntries {
			t.Fatalf("expected loaded slice length %d, got: %d", numEntries, len(loaded))
		}

		// Verify results - odd numbers should be updated, even numbers should not be affected
		for i := 0; i < numEntries; i++ {
			if i%2 == 1 { // Odd numbers - should be updated
				if !loaded[i] {
					t.Fatalf("expected loaded[%d] to be true for existing key", i)
				}
				if previous[i] != i*10 {
					t.Fatalf("expected previous[%d] to be %d, got: %d", i, i*10, previous[i])
				}
			} else { // Even numbers - should not be affected
				if loaded[i] {
					t.Fatalf("expected loaded[%d] to be false for non-existent key", i)
				}
				if previous[i] != 0 {
					t.Fatalf("expected previous[%d] to be 0, got: %d", i, previous[i])
				}
			}
		}

		// Verify map size - should only contain odd numbers
		expectedSize := numEntries / 2
		if m.Size() != expectedSize {
			t.Fatalf("expected map size to be %d, got: %d", expectedSize, m.Size())
		}

		// Check random samples
		for i := 0; i < 20; i++ {
			key := strconv.Itoa(i)
			if i%2 == 1 { // Odd numbers should be updated
				expectPresentMapOf(t, key, i)(m.Load(key))
			} else { // Even numbers should not be present
				expectMissingMapOf(t, key, 0)(m.Load(key))
			}
		}
	})
}

// TestMapOfRangeProcessEntry tests the RangeProcessEntry function of MapOf
func TestMapOfRangeProcessEntry(t *testing.T) {
	// Test with empty map
	t.Run("EmptyMap", func(t *testing.T) {
		m := NewMapOf[string, int]()
		processCount := 0

		m.RangeProcessEntry(func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
			processCount++
			return loaded // No modification
		})

		if processCount != 0 {
			t.Fatalf("expected process count to be 0 for empty map, got: %d", processCount)
		}
	})

	// Test updating values
	t.Run("UpdateValues", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate the map
		for i := 0; i < 10; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		processCount := 0
		m.RangeProcessEntry(func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
			processCount++
			// Double all values
			return &EntryOf[string, int]{Key: loaded.Key, Value: loaded.Value * 2}
		})

		if processCount != 10 {
			t.Fatalf("expected process count to be 10, got: %d", processCount)
		}

		// Verify all values are doubled
		for i := 0; i < 10; i++ {
			key := strconv.Itoa(i)
			expectPresentMapOf(t, key, i*2)(m.Load(key))
		}
	})

	// Test deleting entries
	t.Run("DeleteEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate the map
		for i := 0; i < 10; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		originalSize := m.Size()
		if originalSize != 10 {
			t.Fatalf("expected original size to be 10, got: %d", originalSize)
		}

		// Delete even-numbered entries
		m.RangeProcessEntry(func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
			if loaded.Value%2 == 0 {
				return nil // Delete entry
			}
			return loaded // Keep entry
		})

		// Verify only odd-numbered entries remain
		expectedSize := 5
		if m.Size() != expectedSize {
			t.Fatalf("expected size to be %d after deletion, got: %d", expectedSize, m.Size())
		}

		for i := 0; i < 10; i++ {
			key := strconv.Itoa(i)
			if i%2 == 0 {
				// Even numbers should be deleted
				expectMissingMapOf(t, key, 0)(m.Load(key))
			} else {
				// Odd numbers should remain
				expectPresentMapOf(t, key, i)(m.Load(key))
			}
		}
	})

	// Test mixed operations (update some, delete some, keep some)
	t.Run("MixedOperations", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate the map
		for i := 0; i < 15; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		m.RangeProcessEntry(func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
			value := loaded.Value
			switch {
			case value%3 == 0:
				// Divisible by 3: delete
				return nil
			case value%3 == 1:
				// Remainder 1: multiply by 10
				return &EntryOf[string, int]{Key: loaded.Key, Value: value * 10}
			default:
				// Remainder 2: keep unchanged
				return loaded
			}
		})

		// Verify results
		for i := 0; i < 15; i++ {
			key := strconv.Itoa(i)
			switch {
			case i%3 == 0:
				// Should be deleted
				expectMissingMapOf(t, key, 0)(m.Load(key))
			case i%3 == 1:
				// Should be multiplied by 10
				expectPresentMapOf(t, key, i*10)(m.Load(key))
			default:
				// Should remain unchanged
				expectPresentMapOf(t, key, i)(m.Load(key))
			}
		}
	})

	// Test concurrent safety (basic test)
	t.Run("ConcurrentSafety", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate the map
		for i := 0; i < 100; i++ {
			m.Store(strconv.Itoa(i), i)
		}

		// This should not panic or cause data races
		m.RangeProcessEntry(func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
			// Just return the same entry
			return loaded
		})

		// Verify map is still intact
		if m.Size() != 100 {
			t.Fatalf("expected size to remain 100, got: %d", m.Size())
		}
	})
}

// TestMapOfLoadAndUpdate tests the LoadAndUpdate function of MapOf
func TestMapOfLoadAndUpdate(t *testing.T) {
	// Test with non-existent key
	t.Run("NonExistentKey", func(t *testing.T) {
		m := NewMapOf[string, int]()

		previous, loaded := m.LoadAndUpdate("nonexistent", 42)

		if loaded {
			t.Fatalf("expected loaded to be false for non-existent key")
		}
		if previous != 0 {
			t.Fatalf("expected previous value to be zero for non-existent key, got: %d", previous)
		}

		// Key should still not exist in the map
		expectMissingMapOf(t, "nonexistent", 0)(m.Load("nonexistent"))
	})

	// Test with existing key
	t.Run("ExistingKey", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("existing", 100)

		previous, loaded := m.LoadAndUpdate("existing", 200)

		if !loaded {
			t.Fatalf("expected loaded to be true for existing key")
		}
		if previous != 100 {
			t.Fatalf("expected previous value to be 100, got: %d", previous)
		}

		// Key should now have the new value
		expectPresentMapOf(t, "existing", 200)(m.Load("existing"))
	})

	// Test updating with same value
	t.Run("SameValue", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("key", 42)

		previous, loaded := m.LoadAndUpdate("key", 42)

		if !loaded {
			t.Fatalf("expected loaded to be true for existing key")
		}
		if previous != 42 {
			t.Fatalf("expected previous value to be 42, got: %d", previous)
		}

		// Value should remain the same
		expectPresentMapOf(t, "key", 42)(m.Load("key"))
	})

	// Test multiple updates on same key
	t.Run("MultipleUpdates", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("counter", 0)

		// Perform multiple updates
		for i := 1; i <= 5; i++ {
			previous, loaded := m.LoadAndUpdate("counter", i*10)

			if !loaded {
				t.Fatalf("expected loaded to be true for iteration %d", i)
			}
			expectedPrevious := (i - 1) * 10
			if previous != expectedPrevious {
				t.Fatalf("expected previous value to be %d for iteration %d, got: %d", expectedPrevious, i, previous)
			}
		}

		// Final value should be 50
		expectPresentMapOf(t, "counter", 50)(m.Load("counter"))
	})

	// Test with different key types
	t.Run("IntegerKeys", func(t *testing.T) {
		m := NewMapOf[int, string]()
		m.Store(1, "one")
		m.Store(2, "two")

		// Update existing key
		previous, loaded := m.LoadAndUpdate(1, "ONE")
		if !loaded || previous != "one" {
			t.Fatalf("expected loaded=true and previous='one', got loaded=%v, previous='%s'", loaded, previous)
		}

		// Try non-existent key
		previous, loaded = m.LoadAndUpdate(3, "three")
		if loaded || previous != "" {
			t.Fatalf("expected loaded=false and previous='', got loaded=%v, previous='%s'", loaded, previous)
		}

		// Verify final state
		expectPresentMapOf(t, 1, "ONE")(m.Load(1))
		expectPresentMapOf(t, 2, "two")(m.Load(2))
		expectMissingMapOf(t, 3, "")(m.Load(3))
	})

	// Test concurrent updates
	t.Run("ConcurrentUpdates", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("shared", 0)

		const numGoroutines = 10
		const updatesPerGoroutine = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < updatesPerGoroutine; j++ {
					// Each goroutine tries to update with its own value
					newValue := goroutineID*1000 + j
					m.LoadAndUpdate("shared", newValue)
				}
			}(i)
		}

		wg.Wait()

		// The key should still exist and have some value
		value, ok := m.Load("shared")
		if !ok {
			t.Fatalf("expected 'shared' key to exist after concurrent updates")
		}
		t.Logf("Final value after concurrent updates: %d", value)
	})

	// Test with zero values
	t.Run("ZeroValues", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("zero", 0)

		previous, loaded := m.LoadAndUpdate("zero", 42)

		if !loaded {
			t.Fatalf("expected loaded to be true for existing key with zero value")
		}
		if previous != 0 {
			t.Fatalf("expected previous value to be 0, got: %d", previous)
		}

		expectPresentMapOf(t, "zero", 42)(m.Load("zero"))
	})
}

func TestMapOfGrow_Basic(t *testing.T) {
	m := NewMapOf[string, int]()

	initialStats := m.Stats()
	initialCapacity := initialStats.Capacity
	initialBuckets := initialStats.RootBuckets

	m.Grow(1000)

	afterGrowStats := m.Stats()
	if afterGrowStats.Capacity <= initialCapacity {
		t.Fatalf("Grow should increase capacity: initial=%d, after=%d",
			initialCapacity, afterGrowStats.Capacity)
	}
	if afterGrowStats.RootBuckets <= initialBuckets {
		t.Fatalf("Grow should increase buckets: initial=%d, after=%d",
			initialBuckets, afterGrowStats.RootBuckets)
	}
	if afterGrowStats.TotalGrowths == 0 {
		t.Fatal("TotalGrowths should be incremented")
	}

	m.Store("test", 42)
	if val, ok := m.Load("test"); !ok || val != 42 {
		t.Fatal("Map should work normally after Grow")
	}
}

func TestMapOfGrow_ZeroAndNegative(t *testing.T) {
	m := NewMapOf[string, int]()
	initialStats := m.Stats()

	m.Grow(0)
	afterZeroStats := m.Stats()
	if afterZeroStats.Capacity != initialStats.Capacity {
		t.Fatal("Grow(0) should not change capacity")
	}
	if afterZeroStats.TotalGrowths != initialStats.TotalGrowths {
		t.Fatal("Grow(0) should not increment TotalGrowths")
	}

	m.Grow(-100)
	afterNegativeStats := m.Stats()
	if afterNegativeStats.Capacity != initialStats.Capacity {
		t.Fatal("Grow(-100) should not change capacity")
	}
	if afterNegativeStats.TotalGrowths != initialStats.TotalGrowths {
		t.Fatal("Grow(-100) should not increment TotalGrowths")
	}
}

func TestMapOfGrow_UninitializedMap(t *testing.T) {
	var m MapOf[string, int]

	m.Grow(200)

	stats := m.Stats()
	t.Log(stats)
	if stats.Capacity == 0 {
		t.Fatal("Map should be initialized after Grow")
	}
	if stats.TotalGrowths == 0 {
		t.Fatal("TotalGrowths should be incremented")
	}

	m.Store("test", 42)
	if val, ok := m.Load("test"); !ok || val != 42 {
		t.Fatal("Map should work normally after Grow on uninitialized map")
	}
}

func TestMapOfShrink_Basic(t *testing.T) {
	m := NewMapOf[string, int]()

	for i := 0; i < 10000; i++ {
		m.Store(strconv.Itoa(i), i)
	}

	afterStoreStats := m.Stats()
	initialCapacity := afterStoreStats.Capacity
	initialBuckets := afterStoreStats.RootBuckets

	for i := 0; i < 9000; i++ {
		m.Delete(strconv.Itoa(i))
	}

	m.Shrink()

	afterShrinkStats := m.Stats()
	if afterShrinkStats.Capacity >= initialCapacity {
		t.Fatalf("Shrink should decrease capacity: initial=%d, after=%d",
			initialCapacity, afterShrinkStats.Capacity)
	}
	if afterShrinkStats.RootBuckets >= initialBuckets {
		t.Fatalf("Shrink should decrease buckets: initial=%d, after=%d",
			initialBuckets, afterShrinkStats.RootBuckets)
	}
	if afterShrinkStats.TotalShrinks == 0 {
		t.Fatal("TotalShrinks should be incremented")
	}

	for i := 9000; i < 10000; i++ {
		if val, ok := m.Load(strconv.Itoa(i)); !ok || val != i {
			t.Fatalf("Data should be preserved after Shrink: key=%d", i)
		}
	}
}

func TestMapOfShrink_MinTableLen(t *testing.T) {
	m := NewMapOf[string, int](WithPresize(1000))
	initialStats := m.Stats()
	minBuckets := initialStats.RootBuckets

	for i := 0; i < 10; i++ {
		m.Store(strconv.Itoa(i), i)
	}

	m.Shrink()

	afterShrinkStats := m.Stats()
	if afterShrinkStats.RootBuckets < minBuckets {
		t.Fatalf("Shrink should not go below minTableLen: min=%d, after=%d",
			minBuckets, afterShrinkStats.RootBuckets)
	}
}

func TestMapOfShrink_UninitializedMap(t *testing.T) {
	var m MapOf[string, int]

	m.Shrink()

	stats := m.Stats()
	if stats.Capacity != 0 {
		t.Fatal("Shrink on uninitialized map should not initialize it")
	}
}

func TestMapOfGrowShrink_Concurrent(t *testing.T) {
	m := NewMapOf[int, int]()
	const numGoroutines = 10
	const numOperations = 100
	const sizeAdd = 200

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3) // grow, shrink, data operations

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				m.Grow(sizeAdd)
				runtime.Gosched()
			}
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				m.Shrink()
				runtime.Gosched()
			}
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := base*numOperations + j
				m.Store(key, key)
				if val, ok := m.Load(key); !ok || val != key {
					t.Errorf("Data corruption during concurrent resize: key=%d", key)
				}
				m.Delete(key)
				runtime.Gosched()
			}
		}(i)
	}

	wg.Wait()

	stats := m.Stats()
	if stats.TotalGrowths == 0 {
		t.Fatal("Should have some growths")
	}
	t.Logf("Final stats: %s", stats.ToString())
}

func TestMapOfGrow_Performance(t *testing.T) {
	const numEntries = 100000

	m1 := NewMapOf[int, int]()
	start1 := time.Now()
	for i := 0; i < numEntries; i++ {
		m1.Store(i, i)
	}
	duration1 := time.Since(start1)

	m2 := NewMapOf[int, int]()
	m2.Grow(numEntries)
	start2 := time.Now()
	for i := 0; i < numEntries; i++ {
		m2.Store(i, i)
	}
	duration2 := time.Since(start2)

	if duration2 > duration1*2 {
		t.Logf("Pre-allocation might be slower than expected: without=%v, with=%v",
			duration1, duration2)
	}

	stats1 := m1.Stats()
	stats2 := m2.Stats()

	if stats2.TotalGrowths > stats1.TotalGrowths {
		t.Fatalf("Pre-allocated map should have fewer growths: pre=%d, normal=%d",
			stats2.TotalGrowths, stats1.TotalGrowths)
	}

	t.Logf("Without pre-allocation: %v, growths=%d", duration1, stats1.TotalGrowths)
	t.Logf("With pre-allocation: %v, growths=%d", duration2, stats2.TotalGrowths)
}

func TestMapOfShrink_AutomaticVsManual(t *testing.T) {
	const numEntries = 10000

	m1 := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m1.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries-100; i++ {
		m1.Delete(strconv.Itoa(i))
	}
	stats1 := m1.Stats()

	m2 := NewMapOf[string, int]()
	for i := 0; i < numEntries; i++ {
		m2.Store(strconv.Itoa(i), i)
	}
	for i := 0; i < numEntries-100; i++ {
		m2.Delete(strconv.Itoa(i))
	}
	m2.Shrink()
	stats2 := m2.Stats()

	if stats2.TotalShrinks == 0 {
		t.Fatal("Manual shrink should increment TotalShrinks")
	}

	t.Logf("Automatic shrink stats: %s", stats1.ToString())
	t.Logf("Manual shrink stats: %s", stats2.ToString())
}

func TestMapOfGrowShrink_DataIntegrity(t *testing.T) {
	m := NewMapOf[string, string]()
	const numEntries = 1000

	testData := make(map[string]string)
	for i := 0; i < numEntries; i++ {
		key := "key_" + strconv.Itoa(i)
		value := "value_" + strconv.Itoa(i)
		testData[key] = value
		m.Store(key, value)
	}

	for cycle := 0; cycle < 5; cycle++ {
		m.Grow(numEntries * 2)

		for key, expectedValue := range testData {
			if actualValue, ok := m.Load(key); !ok || actualValue != expectedValue {
				t.Fatalf("Data corruption after Grow cycle %d: key=%s, expected=%s, actual=%s, ok=%v",
					cycle, key, expectedValue, actualValue, ok)
			}
		}

		m.Shrink()

		for key, expectedValue := range testData {
			if actualValue, ok := m.Load(key); !ok || actualValue != expectedValue {
				t.Fatalf("Data corruption after Shrink cycle %d: key=%s, expected=%s, actual=%s, ok=%v",
					cycle, key, expectedValue, actualValue, ok)
			}
		}
	}

	stats := m.Stats()
	if stats.Size != numEntries {
		t.Fatalf("Final size mismatch: expected=%d, actual=%d", numEntries, stats.Size)
	}
}

// TestMapOfCompareAndSwap tests the CompareAndSwap function}

// TestMapOfDefaultHasher tests the defaultHasher function with different key types
func TestMapOfDefaultHasher(t *testing.T) {
	t.Run("UintKeys", func(t *testing.T) {
		m := NewMapOf[uint, string]()
		m.Store(uint(123), "value123")
		m.Store(uint(456), "value456")

		expectPresentMapOf(t, uint(123), "value123")(m.Load(uint(123)))
		expectPresentMapOf(t, uint(456), "value456")(m.Load(uint(456)))
	})

	t.Run("IntKeys", func(t *testing.T) {
		m := NewMapOf[int, string]()
		m.Store(-123, "negative")
		m.Store(456, "positive")

		expectPresentMapOf(t, -123, "negative")(m.Load(-123))
		expectPresentMapOf(t, 456, "positive")(m.Load(456))
	})

	t.Run("UintptrKeys", func(t *testing.T) {
		m := NewMapOf[uintptr, string]()
		m.Store(uintptr(0x1000), "addr1")
		m.Store(uintptr(0x2000), "addr2")

		expectPresentMapOf(t, uintptr(0x1000), "addr1")(m.Load(uintptr(0x1000)))
		expectPresentMapOf(t, uintptr(0x2000), "addr2")(m.Load(uintptr(0x2000)))
	})

	t.Run("Uint64Keys", func(t *testing.T) {
		m := NewMapOf[uint64, string]()
		m.Store(uint64(0x123456789ABCDEF0), "large1")
		m.Store(uint64(0xFEDCBA9876543210), "large2")

		expectPresentMapOf(t, uint64(0x123456789ABCDEF0), "large1")(m.Load(uint64(0x123456789ABCDEF0)))
		expectPresentMapOf(t, uint64(0xFEDCBA9876543210), "large2")(m.Load(uint64(0xFEDCBA9876543210)))
	})

	t.Run("Int64Keys", func(t *testing.T) {
		m := NewMapOf[int64, string]()
		m.Store(int64(-9223372036854775808), "min")
		m.Store(int64(9223372036854775807), "max")

		expectPresentMapOf(t, int64(-9223372036854775808), "min")(m.Load(int64(-9223372036854775808)))
		expectPresentMapOf(t, int64(9223372036854775807), "max")(m.Load(int64(9223372036854775807)))
	})

	t.Run("Uint32Keys", func(t *testing.T) {
		m := NewMapOf[uint32, string]()
		m.Store(uint32(0xFFFFFFFF), "max32")
		m.Store(uint32(0x12345678), "mid32")

		expectPresentMapOf(t, uint32(0xFFFFFFFF), "max32")(m.Load(uint32(0xFFFFFFFF)))
		expectPresentMapOf(t, uint32(0x12345678), "mid32")(m.Load(uint32(0x12345678)))
	})

	t.Run("Int32Keys", func(t *testing.T) {
		m := NewMapOf[int32, string]()
		m.Store(int32(-2147483648), "min32")
		m.Store(int32(2147483647), "max32")

		expectPresentMapOf(t, int32(-2147483648), "min32")(m.Load(int32(-2147483648)))
		expectPresentMapOf(t, int32(2147483647), "max32")(m.Load(int32(2147483647)))
	})

	t.Run("Uint16Keys", func(t *testing.T) {
		m := NewMapOf[uint16, string]()
		m.Store(uint16(0xFFFF), "max16")
		m.Store(uint16(0x1234), "mid16")

		expectPresentMapOf(t, uint16(0xFFFF), "max16")(m.Load(uint16(0xFFFF)))
		expectPresentMapOf(t, uint16(0x1234), "mid16")(m.Load(uint16(0x1234)))
	})

	t.Run("Int16Keys", func(t *testing.T) {
		m := NewMapOf[int16, string]()
		m.Store(int16(-32768), "min16")
		m.Store(int16(32767), "max16")

		expectPresentMapOf(t, int16(-32768), "min16")(m.Load(int16(-32768)))
		expectPresentMapOf(t, int16(32767), "max16")(m.Load(int16(32767)))
	})

	t.Run("Uint8Keys", func(t *testing.T) {
		m := NewMapOf[uint8, string]()
		m.Store(uint8(255), "max8")
		m.Store(uint8(128), "mid8")

		expectPresentMapOf(t, uint8(255), "max8")(m.Load(uint8(255)))
		expectPresentMapOf(t, uint8(128), "mid8")(m.Load(uint8(128)))
	})

	t.Run("Int8Keys", func(t *testing.T) {
		m := NewMapOf[int8, string]()
		m.Store(int8(-128), "min8")
		m.Store(int8(127), "max8")

		expectPresentMapOf(t, int8(-128), "min8")(m.Load(int8(-128)))
		expectPresentMapOf(t, int8(127), "max8")(m.Load(int8(127)))
	})
}

// TestMapOfDefaultHasherComprehensive tests all branches of defaultHasher function
func TestMapOfDefaultHasherComprehensive(t *testing.T) {
	t.Run("Float32Keys", func(t *testing.T) {
		m := &MapOf[float32, string]{}
		keys := []float32{1.1, 2.2, 3.3, 0.0, -1.1}
		for i, key := range keys {
			m.Store(key, fmt.Sprintf("value%d", i))
		}
		for i, key := range keys {
			if val, found := m.Load(key); !found || val != fmt.Sprintf("value%d", i) {
				t.Fatalf("Expected to find key %v with value value%d, got found=%v, val=%s", key, i, found, val)
			}
		}
	})

	t.Run("Float64Keys", func(t *testing.T) {
		m := &MapOf[float64, string]{}
		keys := []float64{1.123456789, 2.987654321, 0.0, -3.141592653}
		for i, key := range keys {
			m.Store(key, fmt.Sprintf("val%d", i))
		}
		for i, key := range keys {
			if val, found := m.Load(key); !found || val != fmt.Sprintf("val%d", i) {
				t.Fatalf("Expected to find key %v with value val%d, got found=%v, val=%s", key, i, found, val)
			}
		}
	})

	t.Run("BoolKeys", func(t *testing.T) {
		m := &MapOf[bool, int]{}
		m.Store(true, 1)
		m.Store(false, 0)

		if val, found := m.Load(true); !found || val != 1 {
			t.Fatalf("Expected true->1, got found=%v, val=%d", found, val)
		}
		if val, found := m.Load(false); !found || val != 0 {
			t.Fatalf("Expected false->0, got found=%v, val=%d", found, val)
		}
	})

	t.Run("ComplexKeys", func(t *testing.T) {
		m := &MapOf[complex64, string]{}
		keys := []complex64{1 + 2i, 3 + 4i, 0 + 0i, -1 - 2i}
		for i, key := range keys {
			m.Store(key, fmt.Sprintf("complex%d", i))
		}
		for i, key := range keys {
			if val, found := m.Load(key); !found || val != fmt.Sprintf("complex%d", i) {
				t.Fatalf("Expected to find key %v with value complex%d, got found=%v, val=%s", key, i, found, val)
			}
		}
	})

	t.Run("Complex128Keys", func(t *testing.T) {
		m := &MapOf[complex128, string]{}
		keys := []complex128{1.1 + 2.2i, 3.3 + 4.4i, 0 + 0i}
		for i, key := range keys {
			m.Store(key, fmt.Sprintf("c128_%d", i))
		}
		for i, key := range keys {
			if val, found := m.Load(key); !found || val != fmt.Sprintf("c128_%d", i) {
				t.Fatalf("Expected to find key %v with value c128_%d, got found=%v, val=%s", key, i, found, val)
			}
		}
	})

	t.Run("ArrayKeys", func(t *testing.T) {
		m := &MapOf[[3]int, string]{}
		keys := [][3]int{{1, 2, 3}, {4, 5, 6}, {0, 0, 0}}
		for i, key := range keys {
			m.Store(key, fmt.Sprintf("array%d", i))
		}
		for i, key := range keys {
			if val, found := m.Load(key); !found || val != fmt.Sprintf("array%d", i) {
				t.Fatalf("Expected to find key %v with value array%d, got found=%v, val=%s", key, i, found, val)
			}
		}
	})

	t.Run("StructKeys", func(t *testing.T) {
		type TestStruct struct {
			A int
			B string
		}
		m := &MapOf[TestStruct, int]{}
		keys := []TestStruct{{1, "a"}, {2, "b"}, {0, ""}}
		for i, key := range keys {
			m.Store(key, i*100)
		}
		for i, key := range keys {
			if val, found := m.Load(key); !found || val != i*100 {
				t.Fatalf("Expected to find key %v with value %d, got found=%v, val=%d", key, i*100, found, val)
			}
		}
	})

	t.Run("IntegerTypesEdgeCases", func(t *testing.T) {
		// Test edge values for different integer types
		m8 := &MapOf[int8, string]{}
		m8.Store(127, "max_int8")
		m8.Store(-128, "min_int8")
		m8.Store(0, "zero_int8")

		m16 := &MapOf[int16, string]{}
		m16.Store(32767, "max_int16")
		m16.Store(-32768, "min_int16")

		m32 := &MapOf[int32, string]{}
		m32.Store(2147483647, "max_int32")
		m32.Store(-2147483648, "min_int32")

		// Verify all values
		if val, found := m8.Load(127); !found || val != "max_int8" {
			t.Fatalf("Expected max_int8, got found=%v, val=%s", found, val)
		}
		if val, found := m16.Load(32767); !found || val != "max_int16" {
			t.Fatalf("Expected max_int16, got found=%v, val=%s", found, val)
		}
		if val, found := m32.Load(2147483647); !found || val != "max_int32" {
			t.Fatalf("Expected max_int32, got found=%v, val=%s", found, val)
		}
	})

	t.Run("UnsignedTypesEdgeCases", func(t *testing.T) {
		mu8 := &MapOf[uint8, string]{}
		mu8.Store(255, "max_uint8")
		mu8.Store(0, "zero_uint8")

		mu16 := &MapOf[uint16, string]{}
		mu16.Store(65535, "max_uint16")

		mu32 := &MapOf[uint32, string]{}
		mu32.Store(4294967295, "max_uint32")

		// Verify values
		if val, found := mu8.Load(255); !found || val != "max_uint8" {
			t.Fatalf("Expected max_uint8, got found=%v, val=%s", found, val)
		}
		if val, found := mu16.Load(65535); !found || val != "max_uint16" {
			t.Fatalf("Expected max_uint16, got found=%v, val=%s", found, val)
		}
		if val, found := mu32.Load(4294967295); !found || val != "max_uint32" {
			t.Fatalf("Expected max_uint32, got found=%v, val=%s", found, val)
		}
	})
}

func TestMapOfEdgeCases(t *testing.T) {
	t.Run("ZeroValues", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("", 0)     // empty string key, zero value
		m.Store("zero", 0) // zero value

		expectPresentMapOf(t, "", 0)(m.Load(""))
		expectPresentMapOf(t, "zero", 0)(m.Load("zero"))
	})

	t.Run("LargeKeys", func(t *testing.T) {
		m := NewMapOf[string, int]()
		largeKey := strings.Repeat("x", 1000)
		m.Store(largeKey, 42)

		expectPresentMapOf(t, largeKey, 42)(m.Load(largeKey))
	})

	t.Run("ManyOperations", func(t *testing.T) {
		m := NewMapOf[int, int]()

		// Store many values
		for i := 0; i < 1000; i++ {
			m.Store(i, i*2)
		}

		// Verify all values
		for i := 0; i < 1000; i++ {
			expectPresentMapOf(t, i, i*2)(m.Load(i))
		}

		// Delete half
		for i := 0; i < 500; i++ {
			m.Delete(i)
		}

		// Verify deletions
		for i := 0; i < 500; i++ {
			expectMissingMapOf(t, i, 0)(m.Load(i))
		}

		// Verify remaining
		for i := 500; i < 1000; i++ {
			expectPresentMapOf(t, i, i*2)(m.Load(i))
		}
	})

	t.Run("StoreOverwrite", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("key", 1)
		m.Store("key", 2) // overwrite
		m.Store("key", 3) // overwrite again

		expectPresentMapOf(t, "key", 3)(m.Load("key"))
	})
}

func TestMapOfCompareAndSwap(t *testing.T) { // Test with comparable values
	t.Run("ComparableValues", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("key1", 100)

		// Successful swap
		if !m.CompareAndSwap("key1", 100, 200) {
			t.Fatal("CompareAndSwap should succeed when old value matches")
		}
		expectPresentMapOf(t, "key1", 200)(m.Load("key1"))

		// Failed swap - wrong old value
		if m.CompareAndSwap("key1", 100, 300) {
			t.Fatal("CompareAndSwap should fail when old value doesn't match")
		}
		expectPresentMapOf(t, "key1", 200)(m.Load("key1"))

		// Failed swap - non-existent key
		if m.CompareAndSwap("nonexistent", 100, 300) {
			t.Fatal("CompareAndSwap should fail for non-existent key")
		}

		// Swap with same value (should succeed)
		if !m.CompareAndSwap("key1", 200, 200) {
			t.Fatal("CompareAndSwap should succeed when swapping to same value")
		}
		expectPresentMapOf(t, "key1", 200)(m.Load("key1"))
	})

	// Test with non-comparable values (should panic)
	t.Run("NonComparableValues", func(t *testing.T) {
		var m MapOf[string, []int] // slice is not comparable
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
		m := NewMapOf[string, int]()
		if m.CompareAndSwap("key1", 100, 200) {
			t.Fatal("CompareAndSwap should fail on empty map")
		}
	})
}

// TestMapOfCompareAndDelete tests the CompareAndDelete function
func TestMapOfCompareAndDelete(t *testing.T) {
	// Test with comparable values
	t.Run("ComparableValues", func(t *testing.T) {
		m := NewMapOf[string, int]()
		m.Store("key1", 100)
		m.Store("key2", 200)

		// Successful delete
		if !m.CompareAndDelete("key1", 100) {
			t.Fatal("CompareAndDelete should succeed when value matches")
		}
		expectMissingMapOf(t, "key1", 0)(m.Load("key1"))

		// Failed delete - wrong value
		if m.CompareAndDelete("key2", 100) {
			t.Fatal("CompareAndDelete should fail when value doesn't match")
		}
		expectPresentMapOf(t, "key2", 200)(m.Load("key2"))

		// Failed delete - non-existent key
		if m.CompareAndDelete("nonexistent", 100) {
			t.Fatal("CompareAndDelete should fail for non-existent key")
		}
	})

	// Test with non-comparable values (should panic)
	t.Run("NonComparableValues", func(t *testing.T) {
		var m MapOf[string, []int] // slice is not comparable
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
		m := NewMapOf[string, int]()
		if m.CompareAndDelete("key1", 100) {
			t.Fatal("CompareAndDelete should fail on empty map")
		}
	})
}

func TestMapOf_LoadEntry(t *testing.T) {
	m := NewMapOf[string, int]()

	// Test loading from empty map
	entry := m.LoadEntry("key1")
	if entry != nil {
		t.Errorf("Expected nil for non-existent key, got %v", entry)
	}

	// Verify Load also returns false for empty map
	value, ok := m.Load("key1")
	if ok {
		t.Errorf("Expected Load to return false for non-existent key, got true with value %v", value)
	}

	// Store a value
	m.Store("key1", 100)

	// Test LoadEntry and Load consistency for existing key
	entry = m.LoadEntry("key1")
	if entry == nil {
		t.Fatal("Expected entry for existing key, got nil")
	}
	if entry.Key != "key1" {
		t.Errorf("Expected key 'key1', got %v", entry.Key)
	}
	if entry.Value != 100 {
		t.Errorf("Expected value 100, got %v", entry.Value)
	}

	// Verify Load returns the same value
	value, ok = m.Load("key1")
	if !ok {
		t.Error("Expected Load to return true for existing key")
	}
	if value != 100 {
		t.Errorf("Expected Load value 100, got %v", value)
	}
	if entry.Value != value {
		t.Errorf("LoadEntry and Load returned different values: %v vs %v", entry.Value, value)
	}

	// Test non-existent key with both functions
	entry = m.LoadEntry("key2")
	value, ok = m.Load("key2")
	if entry != nil {
		t.Errorf("Expected LoadEntry to return nil for non-existent key, got %v", entry)
	}
	if ok {
		t.Errorf("Expected Load to return false for non-existent key, got true with value %v", value)
	}

	// Store multiple values and test consistency
	m.Store("key2", 200)
	m.Store("key3", 300)

	// Test key2
	entry = m.LoadEntry("key2")
	value, ok = m.Load("key2")
	if entry == nil || !ok {
		t.Error("Both LoadEntry and Load should find key2")
	}
	if entry != nil && entry.Value != value {
		t.Errorf("LoadEntry and Load returned different values for key2: %v vs %v", entry.Value, value)
	}

	// Test key3
	entry = m.LoadEntry("key3")
	value, ok = m.Load("key3")
	if entry == nil || !ok {
		t.Error("Both LoadEntry and Load should find key3")
	}
	if entry != nil && entry.Value != value {
		t.Errorf("LoadEntry and Load returned different values for key3: %v vs %v", entry.Value, value)
	}
}

func TestMapOfBatchProcess(t *testing.T) {
	// Test with empty iterator
	t.Run("EmptyIterator", func(t *testing.T) {
		m := NewMapOf[string, int]()
		processCount := 0

		m.BatchProcess(
			func(yield func(string, int) bool) {
				// Empty iterator - no calls to yield
			},
			func(key string, value int, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
				processCount++
				return &EntryOf[string, int]{Value: value}, value, false
			},
		)

		if processCount != 0 {
			t.Fatalf("expected process count to be 0 for empty iterator, got: %d", processCount)
		}
		if m.Size() != 0 {
			t.Fatalf("expected map size to be 0, got: %d", m.Size())
		}
	})

	// Test with single item iterator
	t.Run("SingleItem", func(t *testing.T) {
		m := NewMapOf[string, int]()
		processCount := 0

		m.BatchProcess(
			func(yield func(string, int) bool) {
				yield("key1", 100)
			},
			func(key string, value int, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
				processCount++
				if loaded != nil {
					t.Fatalf("expected loaded to be nil for new key, got: %v", loaded)
				}
				return &EntryOf[string, int]{Value: value}, value, false
			},
		)

		if processCount != 1 {
			t.Fatalf("expected process count to be 1, got: %d", processCount)
		}
		expectPresentMapOf(t, "key1", 100)(m.Load("key1"))
	})

	// Test with multiple items iterator
	t.Run("MultipleItems", func(t *testing.T) {
		m := NewMapOf[string, int]()
		processCount := 0
		expectedItems := map[string]int{
			"key1": 100,
			"key2": 200,
			"key3": 300,
		}

		m.BatchProcess(
			func(yield func(string, int) bool) {
				for key, value := range expectedItems {
					if !yield(key, value) {
						break
					}
				}
			},
			func(key string, value int, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
				processCount++
				if loaded != nil {
					t.Fatalf("expected loaded to be nil for new key %s, got: %v", key, loaded)
				}
				return &EntryOf[string, int]{Value: value}, value, false
			},
		)

		if processCount != len(expectedItems) {
			t.Fatalf("expected process count to be %d, got: %d", len(expectedItems), processCount)
		}
		for key, expectedValue := range expectedItems {
			expectPresentMapOf(t, key, expectedValue)(m.Load(key))
		}
	})

	// Test with existing entries (update scenario)
	t.Run("UpdateExistingEntries", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate map
		m.Store("key1", 50)
		m.Store("key2", 60)
		processCount := 0

		m.BatchProcess(
			func(yield func(string, int) bool) {
				yield("key1", 100) // Update existing
				yield("key2", 200) // Update existing
				yield("key3", 300) // Insert new
			},
			func(key string, value int, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
				processCount++
				if key == "key3" {
					if loaded != nil {
						t.Fatalf("expected loaded to be nil for new key %s, got: %v", key, loaded)
					}
				} else {
					if loaded == nil {
						t.Fatalf("expected loaded to be non-nil for existing key %s", key)
					}
				}
				return &EntryOf[string, int]{Value: value}, value, loaded != nil
			},
		)

		if processCount != 3 {
			t.Fatalf("expected process count to be 3, got: %d", processCount)
		}
		expectPresentMapOf(t, "key1", 100)(m.Load("key1"))
		expectPresentMapOf(t, "key2", 200)(m.Load("key2"))
		expectPresentMapOf(t, "key3", 300)(m.Load("key3"))
	})

	// Test with delete operations
	t.Run("DeleteOperations", func(t *testing.T) {
		m := NewMapOf[string, int]()
		// Pre-populate map
		m.Store("key1", 100)
		m.Store("key2", 200)
		m.Store("key3", 300)
		processCount := 0

		m.BatchProcess(
			func(yield func(string, int) bool) {
				yield("key1", 0) // Delete key1
				yield("key2", 0) // Delete key2
			},
			func(key string, value int, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
				processCount++
				if loaded == nil {
					t.Fatalf("expected loaded to be non-nil for existing key %s", key)
				}
				// Return nil to delete the entry
				return nil, loaded.Value, true
			},
		)

		if processCount != 2 {
			t.Fatalf("expected process count to be 2, got: %d", processCount)
		}
		expectMissingMapOf(t, "key1", 0)(m.Load("key1"))
		expectMissingMapOf(t, "key2", 0)(m.Load("key2"))
		expectPresentMapOf(t, "key3", 300)(m.Load("key3")) // Should still exist
	})

	// Test with growSize parameter
	t.Run("WithGrowSize", func(t *testing.T) {
		m := NewMapOf[string, int]()
		processCount := 0

		m.BatchProcess(
			func(yield func(string, int) bool) {
				for i := 0; i < 100; i++ {
					if !yield(fmt.Sprintf("key%d", i), i) {
						break
					}
				}
			},
			func(key string, value int, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
				processCount++
				return &EntryOf[string, int]{Value: value}, value, false
			},
			100, // growSize
		)

		if processCount != 100 {
			t.Fatalf("expected process count to be 100, got: %d", processCount)
		}
		if m.Size() != 100 {
			t.Fatalf("expected map size to be 100, got: %d", m.Size())
		}
	})

	// Test early termination (yield returns false)
	t.Run("EarlyTermination", func(t *testing.T) {
		m := NewMapOf[string, int]()
		processCount := 0

		m.BatchProcess(
			func(yield func(string, int) bool) {
				for i := 0; i < 10; i++ {
					if i == 5 {
						// Simulate early termination
						if !yield(fmt.Sprintf("key%d", i), i) {
							break
						}
						break // Force early termination
					}
					if !yield(fmt.Sprintf("key%d", i), i) {
						break
					}
				}
			},
			func(key string, value int, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
				processCount++
				return &EntryOf[string, int]{Value: value}, value, false
			},
		)

		if processCount != 6 { // 0,1,2,3,4,5
			t.Fatalf("expected process count to be 6, got: %d", processCount)
		}
	})
}
