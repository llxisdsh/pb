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
	t.Logf("entriesPerBucket : %d", entriesPerBucket)
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

func TestMapOf_InterfaceKey(t *testing.T) {
	type X interface {
		Hello()
	}

	m := NewMapOf[X, int]()

	m.Store(nil, 1)
	if v, ok := m.Load(nil); !ok || v != 1 {
		t.Fatalf("Load(1) = %v, %v; want 1, true", v, ok)
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
		percentage := float64(
			counts[len(counts)-1],
		) * 100 / float64(
			len(latencies),
		)
		t.Logf(
			"  >= %v: %d (%.2f%%)",
			buckets[len(buckets)-1],
			counts[len(counts)-1],
			percentage,
		)
	}
}

// TestMapOfStoreLoadMultiThreadLatency tests Store-Load latency in a
// multithreaded environment
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
	t.Logf(
		"Multi-thread Store-Load Latency Statistics (samples: %d):",
		len(latencies),
	)
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
// 	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
// 	t.Logf("Throughput: %.2f million ops/sec",
// float64(total)/(elapsed.Seconds()*1000000))
//
//	// rand check
//	for i := 0; i < 1000; i++ {
//		idx := i * (total / 1000)
//		if val, ok := m.Load(idx); !ok || val != idx {
// 			t.Errorf("Expected value %d at key %d, got %d, exists: %v", idx, idx, val,
// ok)
//		}
//	}
//}

func TestMapOfMisc(t *testing.T) {
	// var a *SyncMap[int, int] = NewSyncMap[int, int]()
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

// TestMapOfSimpleConcurrentReadWrite test 1 goroutine for store and 1 goroutine
// for load
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

// TestMapOfMultiKeyConcurrentReadWrite tests concurrent read/write with
// multiple keys
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

// TestMapOfConcurrentReadWriteStress performs intensive concurrent stress
// testing
func TestMapOfConcurrentReadWriteStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test")
	}

	// Detect if running with coverage and reduce test intensity
	var writerCount, readerCount, keyCount, iterations int
	if testing.CoverMode() != "" {
		// Reduced parameters for coverage mode to prevent timeout
		writerCount = 2
		readerCount = 4
		keyCount = 100
		iterations = 1000
	} else {
		// Full stress test parameters for normal mode
		writerCount = 4
		readerCount = 16
		keyCount = 1000
		iterations = 10000
	}

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
				// time.Sleep(time.Microsecond) // slightly slow down reading
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
		// const sizeHintFactor = float64(entriesPerBucket) * mapLoadFactor
		growThreshold := int(
			float64(tableLen*entriesPerBucket) * loadFactor,
		)
		growTableLen = calcTableLen(growThreshold)
		_, parallelism = calcParallelism(tableLen, minBucketsPerCPU, cpus)
		if tableLen != lastTableLen ||
			growTableLen != lastGrowTableLen ||
			sizeLen != lastSizeLen ||
			parallelism != lastParallelism {
			t.Logf(
				"sizeHint: %v, tableLen: %v, growThreshold: %v, growTableLen: %v, counterLen: %v, parallelism: %v",
				i,
				tableLen,
				growThreshold,
				growTableLen,
				sizeLen,
				parallelism,
			)
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

// TestMapOf_BatchProcessImmutableEntries tests the BatchProcessImmutableEntries method
func TestMapOf_BatchProcessImmutableEntries(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup initial data
	m.Store("existing1", 10)
	m.Store("existing2", 20)

	// Create immutable entries to process
	immutableEntries := []*EntryOf[string, int]{
		{Key: "existing1", Value: 100}, // Update existing
		{Key: "existing2", Value: 200}, // Update existing
		{Key: "new1", Value: 300},      // Insert new
		{Key: "new2", Value: 400},      // Insert new
	}

	// Process entries with a function that doubles values if key exists, otherwise inserts as-is
	values, status := m.BatchProcessImmutableEntries(
		immutableEntries,
		1.0, // growFactor
		func(entry *EntryOf[string, int], loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
			if loaded != nil {
				// Key exists, double the new value and return old value
				newValue := entry.Value * 2
				return &EntryOf[string, int]{Key: entry.Key, Value: newValue}, loaded.Value, true
			} else {
				// Key doesn't exist, insert new value
				return &EntryOf[string, int]{Key: entry.Key, Value: entry.Value}, entry.Value, false
			}
		},
	)

	// Verify results
	if len(values) != 4 || len(status) != 4 {
		t.Fatalf("Expected 4 results, got values: %d, status: %d", len(values), len(status))
	}

	// Check returned values (should be old values for existing keys, new values for new keys)
	expectedValues := []int{10, 20, 300, 400}
	expectedStatus := []bool{true, true, false, false}

	for i := 0; i < 4; i++ {
		if values[i] != expectedValues[i] {
			t.Errorf("values[%d]: expected %d, got %d", i, expectedValues[i], values[i])
		}
		if status[i] != expectedStatus[i] {
			t.Errorf("status[%d]: expected %v, got %v", i, expectedStatus[i], status[i])
		}
	}

	// Verify final map state
	expectedFinalValues := map[string]int{
		"existing1": 200, // 100 * 2
		"existing2": 400, // 200 * 2
		"new1":      300,
		"new2":      400,
	}

	for key, expectedValue := range expectedFinalValues {
		if value, ok := m.Load(key); !ok || value != expectedValue {
			t.Errorf("Final state: key %s expected %d, got %d (exists: %v)", key, expectedValue, value, ok)
		}
	}
}

// TestMapOf_BatchProcessEntries tests the BatchProcessEntries method
func TestMapOf_BatchProcessEntries(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup initial data
	m.Store("key1", 5)
	m.Store("key2", 15)

	// Create entries to process
	entries := []EntryOf[string, int]{
		{Key: "key1", Value: 50}, // Update existing
		{Key: "key2", Value: 60}, // Update existing
		{Key: "key3", Value: 70}, // Insert new
		{Key: "key4", Value: 80}, // Insert new
	}

	// Process entries with a function that adds 100 to the value if key exists
	values, status := m.BatchProcessEntries(
		entries,
		1.0, // growFactor
		func(entry *EntryOf[string, int], loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
			if loaded != nil {
				// Key exists, add 100 to entry value and return old value
				newValue := entry.Value + 100
				return &EntryOf[string, int]{Key: entry.Key, Value: newValue}, loaded.Value, true
			} else {
				// Key doesn't exist, insert entry value as-is
				return &EntryOf[string, int]{Key: entry.Key, Value: entry.Value}, entry.Value, false
			}
		},
	)

	// Verify results
	if len(values) != 4 || len(status) != 4 {
		t.Fatalf("Expected 4 results, got values: %d, status: %d", len(values), len(status))
	}

	// Check returned values
	expectedValues := []int{5, 15, 70, 80}
	expectedStatus := []bool{true, true, false, false}

	for i := 0; i < 4; i++ {
		if values[i] != expectedValues[i] {
			t.Errorf("values[%d]: expected %d, got %d", i, expectedValues[i], values[i])
		}
		if status[i] != expectedStatus[i] {
			t.Errorf("status[%d]: expected %v, got %v", i, expectedStatus[i], status[i])
		}
	}

	// Verify final map state
	expectedFinalValues := map[string]int{
		"key1": 150, // 50 + 100
		"key2": 160, // 60 + 100
		"key3": 70,
		"key4": 80,
	}

	for key, expectedValue := range expectedFinalValues {
		if value, ok := m.Load(key); !ok || value != expectedValue {
			t.Errorf("Final state: key %s expected %d, got %d (exists: %v)", key, expectedValue, value, ok)
		}
	}
}

// TestMapOf_BatchProcessKeys tests the BatchProcessKeys method
func TestMapOf_BatchProcessKeys(t *testing.T) {
	m := NewMapOf[string, int]()

	// Setup initial data
	m.Store("alpha", 1)
	m.Store("beta", 2)

	// Keys to process
	keys := []string{"alpha", "beta", "gamma", "delta"}

	// Process keys with a function that multiplies by 10 if exists, sets to 999 if new
	values, status := m.BatchProcessKeys(
		keys,
		1.0, // growFactor
		func(key string, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
			if loaded != nil {
				// Key exists, multiply by 10 and return old value
				newValue := loaded.Value * 10
				return &EntryOf[string, int]{Key: key, Value: newValue}, loaded.Value, true
			} else {
				// Key doesn't exist, set to 999
				return &EntryOf[string, int]{Key: key, Value: 999}, 999, false
			}
		},
	)

	// Verify results
	if len(values) != 4 || len(status) != 4 {
		t.Fatalf("Expected 4 results, got values: %d, status: %d", len(values), len(status))
	}

	// Check returned values
	expectedValues := []int{1, 2, 999, 999}
	expectedStatus := []bool{true, true, false, false}

	for i := 0; i < 4; i++ {
		if values[i] != expectedValues[i] {
			t.Errorf("values[%d]: expected %d, got %d", i, expectedValues[i], values[i])
		}
		if status[i] != expectedStatus[i] {
			t.Errorf("status[%d]: expected %v, got %v", i, expectedStatus[i], status[i])
		}
	}

	// Verify final map state
	expectedFinalValues := map[string]int{
		"alpha": 10, // 1 * 10
		"beta":  20, // 2 * 10
		"gamma": 999,
		"delta": 999,
	}

	for key, expectedValue := range expectedFinalValues {
		if value, ok := m.Load(key); !ok || value != expectedValue {
			t.Errorf("Final state: key %s expected %d, got %d (exists: %v)", key, expectedValue, value, ok)
		}
	}

	// Test with empty keys slice
	emptyValues, emptyStatus := m.BatchProcessKeys(
		[]string{},
		1.0,
		func(key string, loaded *EntryOf[string, int]) (*EntryOf[string, int], int, bool) {
			t.Error("Process function should not be called for empty keys")
			return loaded, 0, false
		},
	)

	if len(emptyValues) != 0 || len(emptyStatus) != 0 {
		t.Errorf("Expected empty results for empty keys, got values: %d, status: %d", len(emptyValues), len(emptyStatus))
	}
}

// TestMapOf_LoadOrProcessEntry tests the LoadOrProcessEntry method
func TestMapOf_LoadOrProcessEntry(t *testing.T) {
	m := NewMapOf[string, int]()

	t.Run("LoadExistingKey", func(t *testing.T) {
		// Store a key first
		m.Store("existing", 42)

		// LoadOrProcessEntry should return existing value without calling valueFn
		called := false
		value, loaded := m.LoadOrProcessEntry("existing", func() (*EntryOf[string, int], int, bool) {
			called = true
			return &EntryOf[string, int]{Value: 999}, 999, true
		})

		if called {
			t.Error("valueFn should not be called for existing key")
		}
		if !loaded {
			t.Error("loaded should be true for existing key")
		}
		if value != 42 {
			t.Errorf("Expected value 42, got %d", value)
		}
	})

	t.Run("ProcessNonExistentKey", func(t *testing.T) {
		// LoadOrProcessEntry should call valueFn for non-existent key
		called := false
		value, loaded := m.LoadOrProcessEntry("new", func() (*EntryOf[string, int], int, bool) {
			called = true
			return &EntryOf[string, int]{Value: 100}, 100, true
		})

		if !called {
			t.Error("valueFn should be called for non-existent key")
		}
		if !loaded {
			t.Error("loaded should be true when valueFn returns true")
		}
		if value != 100 {
			t.Errorf("Expected value 100, got %d", value)
		}

		// Verify the key was stored
		if val, ok := m.Load("new"); !ok || val != 100 {
			t.Errorf("Expected stored value 100, got %d (ok=%v)", val, ok)
		}
	})

	t.Run("ProcessNonExistentKeyWithNilEntry", func(t *testing.T) {
		// LoadOrProcessEntry with nil entry should not store anything
		value, loaded := m.LoadOrProcessEntry("nil_entry", func() (*EntryOf[string, int], int, bool) {
			return nil, 200, false
		})

		if loaded {
			t.Error("loaded should be false when valueFn returns false")
		}
		if value != 200 {
			t.Errorf("Expected value 200, got %d", value)
		}

		// Verify the key was not stored
		if _, ok := m.Load("nil_entry"); ok {
			t.Error("Key should not be stored when nil entry is returned")
		}
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		const numGoroutines = 10
		const numOperations = 100
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("concurrent_%d_%d", id, j)
					expectedValue := id*1000 + j

					value, loaded := m.LoadOrProcessEntry(key, func() (*EntryOf[string, int], int, bool) {
						return &EntryOf[string, int]{Value: expectedValue}, expectedValue, true
					})

					if !loaded {
						t.Errorf("Expected loaded=true for key %s", key)
					}
					if value != expectedValue {
						t.Errorf("Expected value %d for key %s, got %d", expectedValue, key, value)
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify all keys were stored correctly
		for i := 0; i < numGoroutines; i++ {
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("concurrent_%d_%d", i, j)
				expectedValue := i*1000 + j

				if val, ok := m.Load(key); !ok || val != expectedValue {
					t.Errorf("Expected %s=%d, got %d (ok=%v)", key, expectedValue, val, ok)
				}
			}
		}
	})
}

// TestMapOf_RangeKeys tests the RangeKeys method
func TestMapOf_RangeKeys(t *testing.T) {
	m := NewMapOf[string, int]()

	t.Run("EmptyMap", func(t *testing.T) {
		keys := make([]string, 0)
		m.RangeKeys(func(key string) bool {
			keys = append(keys, key)
			return true
		})

		if len(keys) != 0 {
			t.Errorf("Expected 0 keys from empty map, got %d", len(keys))
		}
	})

	t.Run("SingleKey", func(t *testing.T) {
		m.Store("single", 42)

		keys := make([]string, 0)
		m.RangeKeys(func(key string) bool {
			keys = append(keys, key)
			return true
		})

		if len(keys) != 1 {
			t.Errorf("Expected 1 key, got %d", len(keys))
		}
		if keys[0] != "single" {
			t.Errorf("Expected key 'single', got '%s'", keys[0])
		}
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		m.Clear()
		expectedKeys := []string{"key1", "key2", "key3", "key4", "key5"}
		for i, key := range expectedKeys {
			m.Store(key, i)
		}

		keys := make([]string, 0)
		m.RangeKeys(func(key string) bool {
			keys = append(keys, key)
			return true
		})

		if len(keys) != len(expectedKeys) {
			t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(keys))
		}

		// Convert to map for easier comparison (order is not guaranteed)
		keyMap := make(map[string]bool)
		for _, key := range keys {
			keyMap[key] = true
		}

		for _, expectedKey := range expectedKeys {
			if !keyMap[expectedKey] {
				t.Errorf("Expected key '%s' not found in range", expectedKey)
			}
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		m.Clear()
		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("key%d", i), i)
		}

		keys := make([]string, 0)
		count := 0
		m.RangeKeys(func(key string) bool {
			keys = append(keys, key)
			count++
			return count < 3 // Stop after 3 keys
		})

		if len(keys) != 3 {
			t.Errorf("Expected 3 keys due to early termination, got %d", len(keys))
		}
	})

	t.Run("ConcurrentModification", func(t *testing.T) {
		m.Clear()
		// Store initial keys
		for i := 0; i < 5; i++ {
			m.Store(fmt.Sprintf("initial%d", i), i)
		}

		keys := make([]string, 0)
		var mu sync.Mutex

		// Start a goroutine that modifies the map during iteration
		go func() {
			time.Sleep(10 * time.Millisecond)
			m.Store("concurrent", 999)
			m.Delete("initial0")
		}()

		m.RangeKeys(func(key string) bool {
			mu.Lock()
			keys = append(keys, key)
			mu.Unlock()
			time.Sleep(5 * time.Millisecond) // Slow down iteration
			return true
		})

		// Should have at least the initial keys (minus any deleted)
		if len(keys) < 4 {
			t.Errorf("Expected at least 4 keys, got %d", len(keys))
		}
	})

	t.Run("LargeMap", func(t *testing.T) {
		m.Clear()
		const numKeys = 1000
		expectedKeys := make(map[string]bool)

		// Store many keys
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("large%d", i)
			m.Store(key, i)
			expectedKeys[key] = true
		}

		keys := make([]string, 0)
		m.RangeKeys(func(key string) bool {
			keys = append(keys, key)
			return true
		})

		if len(keys) != numKeys {
			t.Errorf("Expected %d keys, got %d", numKeys, len(keys))
		}

		// Verify all keys are present
		for _, key := range keys {
			if !expectedKeys[key] {
				t.Errorf("Unexpected key '%s' found in range", key)
			}
		}
	})
}

// TestMapOf_RangeValues tests the RangeValues method
func TestMapOf_RangeValues(t *testing.T) {
	m := NewMapOf[string, int]()

	t.Run("EmptyMap", func(t *testing.T) {
		values := make([]int, 0)
		m.RangeValues(func(value int) bool {
			values = append(values, value)
			return true
		})

		if len(values) != 0 {
			t.Errorf("Expected 0 values from empty map, got %d", len(values))
		}
	})

	t.Run("SingleValue", func(t *testing.T) {
		m.Store("single", 42)

		values := make([]int, 0)
		m.RangeValues(func(value int) bool {
			values = append(values, value)
			return true
		})

		if len(values) != 1 {
			t.Errorf("Expected 1 value, got %d", len(values))
		}
		if values[0] != 42 {
			t.Errorf("Expected value 42, got %d", values[0])
		}
	})

	t.Run("MultipleValues", func(t *testing.T) {
		m.Clear()
		expectedValues := []int{10, 20, 30, 40, 50}
		for i, value := range expectedValues {
			m.Store(fmt.Sprintf("key%d", i), value)
		}

		values := make([]int, 0)
		m.RangeValues(func(value int) bool {
			values = append(values, value)
			return true
		})

		if len(values) != len(expectedValues) {
			t.Errorf("Expected %d values, got %d", len(expectedValues), len(values))
		}

		// Convert to map for easier comparison (order is not guaranteed)
		valueMap := make(map[int]bool)
		for _, value := range values {
			valueMap[value] = true
		}

		for _, expectedValue := range expectedValues {
			if !valueMap[expectedValue] {
				t.Errorf("Expected value %d not found in range", expectedValue)
			}
		}
	})

	t.Run("DuplicateValues", func(t *testing.T) {
		m.Clear()
		// Store multiple keys with the same value
		m.Store("key1", 100)
		m.Store("key2", 100)
		m.Store("key3", 200)
		m.Store("key4", 100)

		values := make([]int, 0)
		m.RangeValues(func(value int) bool {
			values = append(values, value)
			return true
		})

		if len(values) != 4 {
			t.Errorf("Expected 4 values, got %d", len(values))
		}

		// Count occurrences
		count100 := 0
		count200 := 0
		for _, value := range values {
			if value == 100 {
				count100++
			} else if value == 200 {
				count200++
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
		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("key%d", i), i*10)
		}

		values := make([]int, 0)
		count := 0
		m.RangeValues(func(value int) bool {
			values = append(values, value)
			count++
			return count < 3 // Stop after 3 values
		})

		if len(values) != 3 {
			t.Errorf("Expected 3 values due to early termination, got %d", len(values))
		}
	})

	t.Run("ConcurrentModification", func(t *testing.T) {
		m.Clear()
		// Store initial values
		for i := 0; i < 5; i++ {
			m.Store(fmt.Sprintf("initial%d", i), i*100)
		}

		values := make([]int, 0)
		var mu sync.Mutex

		// Start a goroutine that modifies the map during iteration
		go func() {
			time.Sleep(10 * time.Millisecond)
			m.Store("concurrent", 999)
			m.Delete("initial0")
		}()

		m.RangeValues(func(value int) bool {
			mu.Lock()
			values = append(values, value)
			mu.Unlock()
			time.Sleep(5 * time.Millisecond) // Slow down iteration
			return true
		})

		// Should have at least the initial values (minus any deleted)
		if len(values) < 4 {
			t.Errorf("Expected at least 4 values, got %d", len(values))
		}
	})

	t.Run("LargeMap", func(t *testing.T) {
		m.Clear()
		const numValues = 1000
		expectedValues := make(map[int]bool)

		// Store many values
		for i := 0; i < numValues; i++ {
			value := i * 2 // Even numbers
			m.Store(fmt.Sprintf("large%d", i), value)
			expectedValues[value] = true
		}

		values := make([]int, 0)
		m.RangeValues(func(value int) bool {
			values = append(values, value)
			return true
		})

		if len(values) != numValues {
			t.Errorf("Expected %d values, got %d", numValues, len(values))
		}

		// Verify all values are present
		for _, value := range values {
			if !expectedValues[value] {
				t.Errorf("Unexpected value %d found in range", value)
			}
		}
	})

	t.Run("ZeroValues", func(t *testing.T) {
		m.Clear()
		// Test with zero values
		m.Store("zero1", 0)
		m.Store("zero2", 0)
		m.Store("nonzero", 42)

		values := make([]int, 0)
		m.RangeValues(func(value int) bool {
			values = append(values, value)
			return true
		})

		if len(values) != 3 {
			t.Errorf("Expected 3 values, got %d", len(values))
		}

		zeroCount := 0
		nonZeroCount := 0
		for _, value := range values {
			if value == 0 {
				zeroCount++
			} else if value == 42 {
				nonZeroCount++
			}
		}

		if zeroCount != 2 {
			t.Errorf("Expected 2 zero values, got %d", zeroCount)
		}
		if nonZeroCount != 1 {
			t.Errorf("Expected 1 non-zero value, got %d", nonZeroCount)
		}
	})
}

// TestMapOf_HasKey tests the HasKey method
func TestMapOf_HasKey(t *testing.T) {
	m := NewMapOf[string, int]()

	t.Run("EmptyMap", func(t *testing.T) {
		if m.HasKey("nonexistent") {
			t.Error("Expected HasKey to return false for empty map")
		}
	})

	t.Run("ExistingKey", func(t *testing.T) {
		m.Store("existing", 42)

		if !m.HasKey("existing") {
			t.Error("Expected HasKey to return true for existing key")
		}
	})

	t.Run("NonExistingKey", func(t *testing.T) {
		if m.HasKey("nonexistent") {
			t.Error("Expected HasKey to return false for non-existing key")
		}
	})

	t.Run("DeletedKey", func(t *testing.T) {
		m.Store("toDelete", 100)
		if !m.HasKey("toDelete") {
			t.Error("Expected HasKey to return true before deletion")
		}

		m.Delete("toDelete")
		if m.HasKey("toDelete") {
			t.Error("Expected HasKey to return false after deletion")
		}
	})

	t.Run("ZeroValue", func(t *testing.T) {
		m.Store("zero", 0)

		if !m.HasKey("zero") {
			t.Error("Expected HasKey to return true for key with zero value")
		}
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		m.Clear()
		keys := []string{"key1", "key2", "key3", "key4", "key5"}

		// Store all keys
		for i, key := range keys {
			m.Store(key, i*10)
		}

		// Check all keys exist
		for _, key := range keys {
			if !m.HasKey(key) {
				t.Errorf("Expected HasKey to return true for key %s", key)
			}
		}

		// Check non-existing keys
		nonExistingKeys := []string{"missing1", "missing2", "missing3"}
		for _, key := range nonExistingKeys {
			if m.HasKey(key) {
				t.Errorf("Expected HasKey to return false for non-existing key %s", key)
			}
		}
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		m.Clear()
		const numGoroutines = 10
		const numOperations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Start multiple goroutines that store and check keys
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()

				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("concurrent_%d_%d", id, j)

					// Store the key
					m.Store(key, id*1000+j)

					// Immediately check if it exists
					if !m.HasKey(key) {
						t.Errorf("Expected HasKey to return true for just stored key %s", key)
					}

					// Check some other keys
					otherKey := fmt.Sprintf("concurrent_%d_%d", (id+1)%numGoroutines, j)
					m.HasKey(otherKey) // Just call it, result may vary due to concurrency
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("LargeMap", func(t *testing.T) {
		m.Clear()
		const numKeys = 1000

		// Store many keys
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("large_%d", i)
			m.Store(key, i)
		}

		// Check all keys exist
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("large_%d", i)
			if !m.HasKey(key) {
				t.Errorf("Expected HasKey to return true for key %s", key)
			}
		}

		// Check some non-existing keys
		for i := numKeys; i < numKeys+100; i++ {
			key := fmt.Sprintf("large_%d", i)
			if m.HasKey(key) {
				t.Errorf("Expected HasKey to return false for non-existing key %s", key)
			}
		}
	})

	t.Run("EmptyStringKey", func(t *testing.T) {
		m.Clear()

		// Test empty string as key
		m.Store("", 999)
		if !m.HasKey("") {
			t.Error("Expected HasKey to return true for empty string key")
		}
	})

	t.Run("SpecialCharacterKeys", func(t *testing.T) {
		m.Clear()
		specialKeys := []string{
			"key with spaces",
			"key\nwith\nnewlines",
			"key\twith\ttabs",
			"key-with-dashes",
			"key_with_underscores",
			"key.with.dots",
			"key/with/slashes",
			"key\\with\\backslashes",
			"key@with@symbols",
			"key#with#hash",
			"key$with$dollar",
			"key%with%percent",
			"key^with^caret",
			"key&with&ampersand",
			"key*with*asterisk",
			"key(with)parentheses",
			"key[with]brackets",
			"key{with}braces",
			"key|with|pipes",
			"key+with+plus",
			"key=with=equals",
			"key?with?question",
			"key<with>angles",
			"key\"with\"quotes",
			"key'with'apostrophes",
			"key`with`backticks",
			"key~with~tildes",
			"key!with!exclamation",
		}

		// Store all special keys
		for i, key := range specialKeys {
			m.Store(key, i)
		}

		// Check all special keys exist
		for _, key := range specialKeys {
			if !m.HasKey(key) {
				t.Errorf("Expected HasKey to return true for special key: %q", key)
			}
		}
	})

	t.Run("UnicodeKeys", func(t *testing.T) {
		m.Clear()
		unicodeKeys := []string{
			"键",
			"клавиша",
			"キー",
			"مفتاح",
			"🔑",
			"🗝️",
			"🔐",
			"🔒",
			"🔓",
			"αβγδε",
			"ñáéíóú",
			"çğıöşü",
		}

		// Store all unicode keys
		for i, key := range unicodeKeys {
			m.Store(key, i)
		}

		// Check all unicode keys exist
		for _, key := range unicodeKeys {
			if !m.HasKey(key) {
				t.Errorf("Expected HasKey to return true for unicode key: %q", key)
			}
		}
	})

	t.Run("StoreAndDeletePattern", func(t *testing.T) {
		m.Clear()

		// Pattern: store, check, delete, check
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("pattern_%d", i)

			// Initially should not exist
			if m.HasKey(key) {
				t.Errorf("Expected HasKey to return false for key %s before storing", key)
			}

			// Store and check
			m.Store(key, i)
			if !m.HasKey(key) {
				t.Errorf("Expected HasKey to return true for key %s after storing", key)
			}

			// Delete and check
			m.Delete(key)
			if m.HasKey(key) {
				t.Errorf("Expected HasKey to return false for key %s after deletion", key)
			}
		}
	})
}

// TestMapOfWithKeyHasherUnsafe tests the WithKeyHasherUnsafe function
func TestMapOfWithKeyHasherUnsafe(t *testing.T) {
	t.Run("BasicUnsafeHasher", func(t *testing.T) {
		// Custom unsafe hasher that uses string length as hash
		unsafeStringHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
			str := *(*string)(ptr)
			return uintptr(len(str)) ^ seed
		}

		m := NewMapOf[string, int](WithKeyHasherUnsafe(unsafeStringHasher))

		// Test basic operations
		m.Store("a", 1)
		m.Store("bb", 2)
		m.Store("ccc", 3)

		if val, ok := m.Load("a"); !ok || val != 1 {
			t.Errorf("Expected a=1, got %d, exists=%v", val, ok)
		}
		if val, ok := m.Load("bb"); !ok || val != 2 {
			t.Errorf("Expected bb=2, got %d, exists=%v", val, ok)
		}
		if val, ok := m.Load("ccc"); !ok || val != 3 {
			t.Errorf("Expected ccc=3, got %d, exists=%v", val, ok)
		}
	})

	t.Run("UnsafeHasherWithLinearDistribution", func(t *testing.T) {
		// Custom unsafe hasher for integers
		unsafeIntHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
			val := *(*int)(ptr)
			return uintptr(val*31) ^ seed
		}

		m := NewMapOf[int, string](WithKeyHasherUnsafe(unsafeIntHasher, LinearDistribution))

		// Test with sequential keys (good for linear distribution)
		for i := 0; i < 100; i++ {
			m.Store(i, fmt.Sprintf("value%d", i))
		}

		// Verify all values
		for i := 0; i < 100; i++ {
			expected := fmt.Sprintf("value%d", i)
			if val, ok := m.Load(i); !ok || val != expected {
				t.Errorf("Expected %d=%s, got %s, exists=%v", i, expected, val, ok)
			}
		}
	})

	t.Run("UnsafeHasherWithShiftDistribution", func(t *testing.T) {
		// Custom unsafe hasher for strings
		unsafeStringHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
			str := *(*string)(ptr)
			hash := seed
			for _, b := range []byte(str) {
				hash = hash*33 + uintptr(b)
			}
			return hash
		}

		m := NewMapOf[string, int](WithKeyHasherUnsafe(unsafeStringHasher, ShiftDistribution))

		// Test with random-like string keys
		keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
		for i, key := range keys {
			m.Store(key, i*10)
		}

		// Verify all values
		for i, key := range keys {
			expected := i * 10
			if val, ok := m.Load(key); !ok || val != expected {
				t.Errorf("Expected %s=%d, got %d, exists=%v", key, expected, val, ok)
			}
		}
	})

	t.Run("UnsafeHasherWithCollisions", func(t *testing.T) {
		// Intentionally bad hasher that causes collisions
		badHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
			return 42 // Always return the same hash
		}

		m := NewMapOf[string, int](WithKeyHasherUnsafe(badHasher))

		// Store multiple values that will all hash to the same value
		testData := map[string]int{
			"key1": 100,
			"key2": 200,
			"key3": 300,
			"key4": 400,
		}

		for key, val := range testData {
			m.Store(key, val)
		}

		// Verify all values can still be retrieved correctly
		for key, expected := range testData {
			if val, ok := m.Load(key); !ok || val != expected {
				t.Errorf("Expected %s=%d, got %d, exists=%v", key, expected, val, ok)
			}
		}
	})

	t.Run("UnsafeHasherNilFunction", func(t *testing.T) {
		// Test with nil hasher (should use default)
		m := NewMapOf[string, int](WithKeyHasherUnsafe(nil))

		m.Store("test", 42)
		if val, ok := m.Load("test"); !ok || val != 42 {
			t.Errorf("Expected test=42, got %d, exists=%v", val, ok)
		}
	})

	t.Run("UnsafeHasherStructKeys", func(t *testing.T) {
		type TestStruct struct {
			ID   int
			Name string
		}

		// Custom unsafe hasher for struct
		structHasher := func(ptr unsafe.Pointer, seed uintptr) uintptr {
			s := *(*TestStruct)(ptr)
			return uintptr(s.ID)*31 + uintptr(len(s.Name)) + seed
		}

		m := NewMapOf[TestStruct, string](WithKeyHasherUnsafe(structHasher))

		key1 := TestStruct{ID: 1, Name: "Alice"}
		key2 := TestStruct{ID: 2, Name: "Bob"}

		m.Store(key1, "value1")
		m.Store(key2, "value2")

		if val, ok := m.Load(key1); !ok || val != "value1" {
			t.Errorf("Expected key1=value1, got %s, exists=%v", val, ok)
		}
		if val, ok := m.Load(key2); !ok || val != "value2" {
			t.Errorf("Expected key2=value2, got %s, exists=%v", val, ok)
		}
	})
}

// TestMapOfWithValueEqualUnsafe tests the WithValueEqualUnsafe function
func TestMapOfWithValueEqualUnsafe(t *testing.T) {
	t.Run("BasicUnsafeValueEqual", func(t *testing.T) {
		type TestValue struct {
			ID   int
			Name string
		}

		// Custom unsafe equality function that only compares ID
		unsafeEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
			val1 := *(*TestValue)(ptr1)
			val2 := *(*TestValue)(ptr2)
			return val1.ID == val2.ID
		}

		m := NewMapOf[string, TestValue](WithValueEqualUnsafe(unsafeEqual))

		val1 := TestValue{ID: 1, Name: "Alice"}
		val2 := TestValue{ID: 1, Name: "Bob"} // Same ID, different name
		val3 := TestValue{ID: 2, Name: "Charlie"}

		m.Store("key1", val1)

		// CompareAndSwap should succeed because IDs match (1 == 1)
		if !m.CompareAndSwap("key1", val2, val3) {
			t.Error("CompareAndSwap should succeed with custom equality (same ID)")
		}

		// Verify the value was swapped
		if result, ok := m.Load("key1"); !ok || result.ID != 2 {
			t.Errorf("Expected ID=2 after swap, got ID=%d, exists=%v", result.ID, ok)
		}
	})

	t.Run("UnsafeValueEqualWithCompareAndDelete", func(t *testing.T) {
		type TestValue struct {
			Score int
			Extra string
		}

		// Custom unsafe equality that only compares Score
		unsafeEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
			val1 := *(*TestValue)(ptr1)
			val2 := *(*TestValue)(ptr2)
			return val1.Score == val2.Score
		}

		m := NewMapOf[string, TestValue](WithValueEqualUnsafe(unsafeEqual))

		stored := TestValue{Score: 100, Extra: "original"}
		compare := TestValue{Score: 100, Extra: "different"} // Same score, different extra

		m.Store("test", stored)

		// CompareAndDelete should succeed because scores match
		if !m.CompareAndDelete("test", compare) {
			t.Error("CompareAndDelete should succeed with custom equality (same score)")
		}

		// Verify the key was deleted
		if _, ok := m.Load("test"); ok {
			t.Error("Key should be deleted after CompareAndDelete")
		}
	})

	t.Run("UnsafeValueEqualSliceComparison", func(t *testing.T) {
		// Custom unsafe equality for slice comparison
		unsafeSliceEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
			slice1 := *(*[]int)(ptr1)
			slice2 := *(*[]int)(ptr2)

			if len(slice1) != len(slice2) {
				return false
			}

			for i := range slice1 {
				if slice1[i] != slice2[i] {
					return false
				}
			}
			return true
		}

		m := NewMapOf[string, []int](WithValueEqualUnsafe(unsafeSliceEqual))

		slice1 := []int{1, 2, 3}
		slice2 := []int{1, 2, 3} // Same content, different slice
		slice3 := []int{4, 5, 6}

		m.Store("key", slice1)

		// CompareAndSwap should succeed because slice contents are equal
		if !m.CompareAndSwap("key", slice2, slice3) {
			t.Error("CompareAndSwap should succeed with custom slice equality")
		}

		// Verify the value was swapped
		if result, ok := m.Load("key"); !ok || len(result) != 3 || result[0] != 4 {
			t.Errorf("Expected [4,5,6] after swap, got %v, exists=%v", result, ok)
		}
	})

	t.Run("UnsafeValueEqualFloatTolerance", func(t *testing.T) {
		// Custom unsafe equality for float comparison with tolerance
		const tolerance = 0.001
		unsafeFloatEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
			val1 := *(*float64)(ptr1)
			val2 := *(*float64)(ptr2)
			diff := val1 - val2
			if diff < 0 {
				diff = -diff
			}
			return diff < tolerance
		}

		m := NewMapOf[string, float64](WithValueEqualUnsafe(unsafeFloatEqual))

		m.Store("pi", 3.14159)

		// These should be considered equal due to tolerance
		closeValue := 3.14160 // Within tolerance
		newValue := 2.71828

		if !m.CompareAndSwap("pi", closeValue, newValue) {
			t.Error("CompareAndSwap should succeed with float tolerance equality")
		}

		// Verify the value was swapped
		if result, ok := m.Load("pi"); !ok || result != newValue {
			t.Errorf("Expected %f after swap, got %f, exists=%v", newValue, result, ok)
		}
	})

	t.Run("UnsafeValueEqualNilFunction", func(t *testing.T) {
		// Test with nil equality function (should use default)
		m := NewMapOf[string, int](WithValueEqualUnsafe(nil))

		m.Store("test", 42)

		// Should use default equality
		if !m.CompareAndSwap("test", 42, 84) {
			t.Error("CompareAndSwap should succeed with default equality")
		}

		if val, ok := m.Load("test"); !ok || val != 84 {
			t.Errorf("Expected test=84, got %d, exists=%v", val, ok)
		}
	})

	t.Run("UnsafeValueEqualCaseInsensitiveString", func(t *testing.T) {
		// Custom unsafe equality for case-insensitive string comparison
		unsafeCaseInsensitiveEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
			str1 := *(*string)(ptr1)
			str2 := *(*string)(ptr2)
			return strings.EqualFold(str1, str2)
		}

		m := NewMapOf[int, string](WithValueEqualUnsafe(unsafeCaseInsensitiveEqual))

		m.Store(1, "Hello")

		// Should match despite different case
		if !m.CompareAndSwap(1, "HELLO", "World") {
			t.Error("CompareAndSwap should succeed with case-insensitive equality")
		}

		if val, ok := m.Load(1); !ok || val != "World" {
			t.Errorf("Expected 1=World, got %s, exists=%v", val, ok)
		}
	})

	t.Run("UnsafeValueEqualComplexStruct", func(t *testing.T) {
		type ComplexValue struct {
			Primary   int
			Secondary string
			Metadata  map[string]interface{}
		}

		// Custom unsafe equality that only compares Primary field
		unsafeComplexEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
			val1 := *(*ComplexValue)(ptr1)
			val2 := *(*ComplexValue)(ptr2)
			return val1.Primary == val2.Primary
		}

		m := NewMapOf[string, ComplexValue](WithValueEqualUnsafe(unsafeComplexEqual))

		val1 := ComplexValue{
			Primary:   100,
			Secondary: "original",
			Metadata:  map[string]interface{}{"key": "value1"},
		}

		val2 := ComplexValue{
			Primary:   100, // Same primary
			Secondary: "different",
			Metadata:  map[string]interface{}{"key": "value2"},
		}

		val3 := ComplexValue{
			Primary:   200,
			Secondary: "new",
			Metadata:  map[string]interface{}{"key": "value3"},
		}

		m.Store("complex", val1)

		// Should succeed because Primary fields match
		if !m.CompareAndSwap("complex", val2, val3) {
			t.Error("CompareAndSwap should succeed with custom complex equality")
		}

		// Verify the value was swapped
		if result, ok := m.Load("complex"); !ok || result.Primary != 200 {
			t.Errorf("Expected Primary=200 after swap, got Primary=%d, exists=%v", result.Primary, ok)
		}
	})
}

// TestMapOfWithBuiltInHasher tests the WithBuiltInHasher function
func TestMapOfWithBuiltInHasher(t *testing.T) {
	t.Run("BasicBuiltInHasher", func(t *testing.T) {
		// Test with built-in hasher for string keys
		m := NewMapOf[string, int](WithBuiltInHasher[string]())

		// Basic operations should work normally
		m.Store("hello", 100)
		m.Store("world", 200)

		if val, ok := m.Load("hello"); !ok || val != 100 {
			t.Errorf("Expected hello=100, got %d, exists=%v", val, ok)
		}

		if val, ok := m.Load("world"); !ok || val != 200 {
			t.Errorf("Expected world=200, got %d, exists=%v", val, ok)
		}

		// Test Range functionality
		count := 0
		m.Range(func(key string, value int) bool {
			count++
			return true
		})

		if count != 2 {
			t.Errorf("Expected 2 items in range, got %d", count)
		}
	})

	t.Run("BuiltInHasherWithIntKeys", func(t *testing.T) {
		// Test with built-in hasher for int keys
		m := NewMapOf[int, string](WithBuiltInHasher[int]())

		// Test with various int values
		testData := map[int]string{
			0:          "zero",
			1:          "one",
			-1:         "negative one",
			1000000:    "million",
			-1000000:   "negative million",
			2147483647: "max int32",
		}

		// Store all test data
		for k, v := range testData {
			m.Store(k, v)
		}

		// Verify all data can be retrieved
		for k, expected := range testData {
			if val, ok := m.Load(k); !ok || val != expected {
				t.Errorf("Expected %d=%s, got %s, exists=%v", k, expected, val, ok)
			}
		}

		// Test deletion
		m.Delete(0)

		if _, ok := m.Load(0); ok {
			t.Error("Key 0 should be deleted")
		}
	})

	t.Run("BuiltInHasherWithStructKeys", func(t *testing.T) {
		type TestKey struct {
			ID   int
			Name string
		}

		// Test with built-in hasher for struct keys
		m := NewMapOf[TestKey, string](WithBuiltInHasher[TestKey]())

		key1 := TestKey{ID: 1, Name: "Alice"}
		key2 := TestKey{ID: 2, Name: "Bob"}
		key3 := TestKey{ID: 1, Name: "Alice"} // Same as key1

		m.Store(key1, "value1")
		m.Store(key2, "value2")

		// key3 should match key1 (same struct values)
		if val, ok := m.Load(key3); !ok || val != "value1" {
			t.Errorf("Expected key3 to match key1, got %s, exists=%v", val, ok)
		}

		// Test LoadOrStore
		if val, loaded := m.LoadOrStore(key1, "new_value"); loaded != true || val != "value1" {
			t.Errorf("LoadOrStore should return existing value, got %s, loaded=%v", val, loaded)
		}

		newKey := TestKey{ID: 3, Name: "Charlie"}
		if val, loaded := m.LoadOrStore(newKey, "value3"); loaded != false || val != "value3" {
			t.Errorf("LoadOrStore should store new value, got %s, loaded=%v", val, loaded)
		}
	})

	t.Run("BuiltInHasherWithFloat64Keys", func(t *testing.T) {
		// Test with built-in hasher for float64 keys
		m := NewMapOf[float64, string](WithBuiltInHasher[float64]())

		testFloats := map[float64]string{
			1.0:     "one",
			-1.0:    "negative one",
			3.14159: "pi",
			2.71828: "e",
		}

		// Handle 0.0 and -0.0 separately since they're the same key in Go maps
		m.Store(0.0, "zero")
		m.Store(-0.0, "negative zero") // This will overwrite "zero"

		// Store all test data
		for k, v := range testFloats {
			m.Store(k, v)
		}

		// Verify all data can be retrieved
		for k, expected := range testFloats {
			if val, ok := m.Load(k); !ok || val != expected {
				t.Errorf("Expected %f=%s, got %s, exists=%v", k, expected, val, ok)
			}
		}

		// Test that NaN handling works (NaN != NaN)
		nan1 := math.NaN()
		nan2 := math.NaN()

		m.Store(nan1, "nan1")

		// Different NaN values should not match
		if val, ok := m.Load(nan2); ok {
			t.Errorf("Different NaN values should not match, got %s", val)
		}
	})

	t.Run("BuiltInHasherWithStringKeys", func(t *testing.T) {
		// Test with built-in hasher for string keys with various lengths
		m := NewMapOf[string, int](WithBuiltInHasher[string]())

		testStrings := []string{
			"",                                    // empty string
			"a",                                   // single char
			"hello",                               // short string
			"this is a longer string for testing", // long string
			"unicode: 你好世界",                   // unicode string
		}

		// Store test data
		for i, s := range testStrings {
			m.Store(s, i)
		}

		// Verify all data can be retrieved
		for i, s := range testStrings {
			if val, ok := m.Load(s); !ok || val != i {
				t.Errorf("Expected %s=%d, got %d, exists=%v", s, i, val, ok)
			}
		}
	})

	t.Run("BuiltInHasherPerformanceComparison", func(t *testing.T) {
		// Compare built-in hasher with default hasher performance
		const numItems = 1000

		// Map with built-in hasher
		m1 := NewMapOf[string, int](WithBuiltInHasher[string]())

		// Map with default hasher (no explicit hasher)
		m2 := NewMapOf[string, int]()

		// Generate test data
		keys := make([]string, numItems)
		for i := 0; i < numItems; i++ {
			keys[i] = fmt.Sprintf("key_%d_%s", i, strings.Repeat("x", i%10))
		}

		// Test both maps with same data
		for i, key := range keys {
			m1.Store(key, i)
			m2.Store(key, i)
		}

		// Verify both maps have same data
		for i, key := range keys {
			val1, ok1 := m1.Load(key)
			val2, ok2 := m2.Load(key)

			if !ok1 || !ok2 || val1 != val2 || val1 != i {
				t.Errorf("Mismatch for key %s: m1(%d,%v) vs m2(%d,%v), expected %d",
					key, val1, ok1, val2, ok2, i)
			}
		}
	})

	t.Run("BuiltInHasherWithComplexKeys", func(t *testing.T) {
		type ComplexKey struct {
			ID       int64
			Category string
			Active   bool
			Score    float64
		}

		// Test with built-in hasher for complex struct keys
		m := NewMapOf[ComplexKey, string](WithBuiltInHasher[ComplexKey]())

		key1 := ComplexKey{ID: 12345, Category: "premium", Active: true, Score: 95.5}
		key2 := ComplexKey{ID: 67890, Category: "basic", Active: false, Score: 72.3}
		key3 := ComplexKey{ID: 12345, Category: "premium", Active: true, Score: 95.5} // Same as key1

		m.Store(key1, "premium_user")
		m.Store(key2, "basic_user")

		// key3 should match key1
		if val, ok := m.Load(key3); !ok || val != "premium_user" {
			t.Errorf("Expected key3 to match key1, got %s, exists=%v", val, ok)
		}

		// Test CompareAndSwap
		if !m.CompareAndSwap(key1, "premium_user", "upgraded_user") {
			t.Error("CompareAndSwap should succeed")
		}

		if val, ok := m.Load(key1); !ok || val != "upgraded_user" {
			t.Errorf("Expected upgraded_user after CompareAndSwap, got %s, exists=%v", val, ok)
		}
	})

	t.Run("BuiltInHasherConsistency", func(t *testing.T) {
		// Test that built-in hasher produces consistent results across multiple maps
		m1 := NewMapOf[string, int](WithBuiltInHasher[string]())
		m2 := NewMapOf[string, int](WithBuiltInHasher[string]())

		testKeys := []string{"test", "hello", "world", "golang", "mapof"}

		// Store same data in both maps
		for i, key := range testKeys {
			m1.Store(key, i)
			m2.Store(key, i)
		}

		// Both maps should behave identically
		for i, key := range testKeys {
			val1, ok1 := m1.Load(key)
			val2, ok2 := m2.Load(key)

			if ok1 != ok2 || val1 != val2 || val1 != i {
				t.Errorf("Inconsistent behavior for key %s: m1(%d,%v) vs m2(%d,%v), expected %d",
					key, val1, ok1, val2, ok2, i)
			}
		}

		// Test deletion consistency
		for _, key := range testKeys[:2] {
			m1.Delete(key)
			m2.Delete(key)

			// Verify both keys are deleted
			_, ok1 := m1.Load(key)
			_, ok2 := m2.Load(key)

			if ok1 != ok2 {
				t.Errorf("Inconsistent deletion for key %s: m1_exists=%v vs m2_exists=%v", key, ok1, ok2)
			}
		}
	})
}

// Test types for interface testing
type CustomKey struct {
	ID   int64
	Name string
}

func (c CustomKey) HashCode(seed uintptr) uintptr {
	// Simple hash combining ID and name length
	return uintptr(c.ID)*31 + uintptr(len(c.Name)) + seed
}

type SequentialKey int64

func (s SequentialKey) HashCode(seed uintptr) uintptr {
	return uintptr(s) + seed
}

func (s SequentialKey) HashOpts() []HashOptimization {
	return []HashOptimization{LinearDistribution}
}

type CustomValue struct {
	Data []int
	Meta string
}

func (c CustomValue) Equal(other CustomValue) bool {
	// Custom equality: only compare Data slice, ignore Meta
	if len(c.Data) != len(other.Data) {
		return false
	}
	for i, v := range c.Data {
		if v != other.Data[i] {
			return false
		}
	}
	return true
}

type SmartKey struct {
	ID int64
}

func (s SmartKey) HashCode(seed uintptr) uintptr {
	return uintptr(s.ID) + seed
}

func (s SmartKey) HashOpts() []HashOptimization {
	return []HashOptimization{LinearDistribution}
}

type SmartValue struct {
	Value   int
	Ignored string
}

func (s SmartValue) Equal(other SmartValue) bool {
	return s.Value == other.Value // ignore Ignored field
}

type HashKey int

func (h HashKey) HashCode(seed uintptr) uintptr {
	return uintptr(h) + seed
}

type EqualValue struct {
	Value int
}

func (e EqualValue) Equal(other EqualValue) bool {
	return e.Value == other.Value
}

// TestMapOf_Interfaces tests the IHashCode, IHashOpts, and IEqual interfaces
func TestMapOf_Interfaces(t *testing.T) {
	t.Run("IHashCode", func(t *testing.T) {
		m := NewMapOf[CustomKey, string]()
		key1 := CustomKey{ID: 1, Name: "test"}
		key2 := CustomKey{ID: 2, Name: "hello"}

		m.Store(key1, "value1")
		m.Store(key2, "value2")

		if val, ok := m.Load(key1); !ok || val != "value1" {
			t.Errorf("Expected value1, got %s, exists=%v", val, ok)
		}
		if val, ok := m.Load(key2); !ok || val != "value2" {
			t.Errorf("Expected value2, got %s, exists=%v", val, ok)
		}

		// Test that different keys with same hash don't collide incorrectly
		key3 := CustomKey{ID: 1, Name: "test"} // same as key1
		if val, ok := m.Load(key3); !ok || val != "value1" {
			t.Errorf(
				"Expected value1 for equivalent key, got %s, exists=%v",
				val,
				ok,
			)
		}
	})

	t.Run("IHashOpts", func(t *testing.T) {
		m := NewMapOf[SequentialKey, string]()

		// Store sequential keys
		for i := SequentialKey(1); i <= 10; i++ {
			m.Store(i, fmt.Sprintf("value%d", i))
		}

		// Verify all values can be retrieved
		for i := SequentialKey(1); i <= 10; i++ {
			expected := fmt.Sprintf("value%d", i)
			if val, ok := m.Load(i); !ok || val != expected {
				t.Errorf("Expected %s, got %s, exists=%v", expected, val, ok)
			}
		}

		// Test size
		if size := m.Size(); size != 10 {
			t.Errorf("Expected size 10, got %d", size)
		}
	})

	t.Run("IEqual", func(t *testing.T) {
		m := NewMapOf[string, CustomValue]()

		val1 := CustomValue{Data: []int{1, 2, 3}, Meta: "meta1"}
		val2 := CustomValue{
			Data: []int{1, 2, 3},
			Meta: "meta2",
		} // different Meta, same Data
		val3 := CustomValue{
			Data: []int{4, 5, 6},
			Meta: "meta1",
		} // different Data, same Meta

		m.Store("key1", val1)

		// Test CompareAndSwap with custom equality
		// Should succeed because val1 and val2 are equal according to custom
		// logic
		if !m.CompareAndSwap("key1", val2, val3) {
			t.Error("CompareAndSwap should have succeeded with custom equality")
		}

		// Verify the value was swapped
		if actual, ok := m.Load("key1"); !ok {
			t.Error("Key should exist after CompareAndSwap")
		} else if !actual.Equal(val3) {
			t.Errorf("Expected value to be swapped to val3, got %+v", actual)
		}

		// Test CompareAndDelete with custom equality
		val4 := CustomValue{
			Data: []int{4, 5, 6},
			Meta: "different_meta",
		} // same Data as val3
		if !m.CompareAndDelete("key1", val4) {
			t.Error(
				"CompareAndDelete should have succeeded with custom equality",
			)
		}

		// Verify the key was deleted
		if _, ok := m.Load("key1"); ok {
			t.Error("Key should be deleted after CompareAndDelete")
		}
	})

	t.Run("CombinedInterfaces", func(t *testing.T) {
		m := NewMapOf[SmartKey, SmartValue]()

		key := SmartKey{ID: 42}
		val1 := SmartValue{Value: 100, Ignored: "ignore1"}
		val2 := SmartValue{
			Value:   100,
			Ignored: "ignore2",
		}                                                  // same Value, different Ignored
		val3 := SmartValue{Value: 200, Ignored: "ignore1"} // different Value

		m.Store(key, val1)

		// Test that custom hash and equality work together
		if actual, ok := m.Load(key); !ok || !actual.Equal(val1) {
			t.Errorf("Failed to load stored value")
		}

		// Test CompareAndSwap with custom equality
		if !m.CompareAndSwap(key, val2, val3) {
			t.Error("CompareAndSwap should succeed with custom equality")
		}

		if actual, ok := m.Load(key); !ok || !actual.Equal(val3) {
			t.Errorf("Value should be swapped to val3")
		}
	})

	t.Run("InterfacePriority", func(t *testing.T) {
		// Test that explicit WithKeyHasher overrides IHashCode

		// Custom hasher that should take precedence
		customHasher := func(key HashKey, seed uintptr) uintptr {
			return uintptr(key)*2 + seed // different from IHashCode
		}

		m := NewMapOf[HashKey, string](WithKeyHasher(customHasher))

		m.Store(HashKey(1), "test")
		if val, ok := m.Load(HashKey(1)); !ok || val != "test" {
			t.Errorf("Custom hasher should work, got %s, exists=%v", val, ok)
		}

		// Test that explicit WithValueEqual overrides IEqual

		// Custom equality that should take precedence
		customEqual := func(a, b EqualValue) bool {
			return false // always return false for testing
		}

		m2 := NewMapOf[string, EqualValue](WithValueEqual(customEqual))
		val := EqualValue{Value: 42}
		m2.Store("key", val)

		// CompareAndSwap should fail because custom equality always returns
		// false
		if m2.CompareAndSwap("key", val, EqualValue{Value: 43}) {
			t.Error(
				"CompareAndSwap should fail with custom equality that always returns false",
			)
		}
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

		testAllMapOf(
			t,
			m,
			testDataMapMapOf(testData[:]),
			func(_ string, _ int) bool {
				return true
			},
		)
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
						// Try a couple of things to interfere with the clear.
						expectNotDeletedMapOf(
							t,
							s,
							math.MaxInt,
						)(
							m.CompareAndDelete(s, math.MaxInt),
						)
						m.CompareAndSwap(
							s,
							i,
							i+1,
						) // May succeed or fail; we don't care.
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
					expectNotDeletedMapOf(
						t,
						s,
						math.MaxInt,
					)(
						m.CompareAndDelete(s, math.MaxInt),
					)
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
			expectNotDeletedMapOf(
				t,
				testData[15],
				math.MaxInt,
			)(
				m.CompareAndDelete(testData[15], math.MaxInt),
			)
			expectDeletedMapOf(
				t,
				testData[15],
				15,
			)(
				m.CompareAndDelete(testData[15], 15),
			)
			expectNotDeletedMapOf(
				t,
				testData[15],
				15,
			)(
				m.CompareAndDelete(testData[15], 15),
			)
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
				expectNotDeletedMapOf(
					t,
					testData[i],
					math.MaxInt,
				)(
					m.CompareAndDelete(testData[i], math.MaxInt),
				)
				expectDeletedMapOf(
					t,
					testData[i],
					i,
				)(
					m.CompareAndDelete(testData[i], i),
				)
				expectNotDeletedMapOf(
					t,
					testData[i],
					i,
				)(
					m.CompareAndDelete(testData[i], i),
				)
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

			testAllMapOf(
				t,
				m,
				testDataMapMapOf(testData[:]),
				func(s string, i int) bool {
					expectDeletedMapOf(t, s, i)(m.CompareAndDelete(s, i))
					return true
				},
			)
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
						expectDeletedMapOf(
							t,
							key,
							id,
						)(
							m.CompareAndDelete(key, id),
						)
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
						expectNotDeletedMapOf(
							t,
							s,
							math.MaxInt,
						)(
							m.CompareAndDelete(s, math.MaxInt),
						)
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
					expectNotSwappedMapOf(
						t,
						s,
						math.MaxInt,
						i+j+1,
					)(
						m.CompareAndSwap(s, math.MaxInt, i+j+1),
					)
					expectSwappedMapOf(
						t,
						s,
						i,
						i+j+1,
					)(
						m.CompareAndSwap(s, i+j, i+j+1),
					)
					expectNotSwappedMapOf(
						t,
						s,
						i+j,
						i+j+1,
					)(
						m.CompareAndSwap(s, i+j, i+j+1),
					)
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
			expectNotSwappedMapOf(
				t,
				testData[15],
				math.MaxInt,
				16,
			)(
				m.CompareAndSwap(testData[15], math.MaxInt, 16),
			)
			expectSwappedMapOf(
				t,
				testData[15],
				15,
				16,
			)(
				m.CompareAndSwap(testData[15], 15, 16),
			)
			expectNotSwappedMapOf(
				t,
				testData[15],
				15,
				16,
			)(
				m.CompareAndSwap(testData[15], 15, 16),
			)
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
				expectNotSwappedMapOf(
					t,
					testData[i],
					math.MaxInt,
					i+1,
				)(
					m.CompareAndSwap(testData[i], math.MaxInt, i+1),
				)
				expectSwappedMapOf(
					t,
					testData[i],
					i,
					i+1,
				)(
					m.CompareAndSwap(testData[i], i, i+1),
				)
				expectNotSwappedMapOf(
					t,
					testData[i],
					i,
					i+1,
				)(
					m.CompareAndSwap(testData[i], i, i+1),
				)
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
						expectSwappedMapOf(
							t,
							key,
							id,
							id+1,
						)(
							m.CompareAndSwap(key, id, id+1),
						)
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
						expectSwappedMapOf(
							t,
							key,
							id,
							id+1,
						)(
							m.CompareAndSwap(key, id, id+1),
						)
						expectPresentMapOf(t, key, id+1)(m.Load(key))
						expectDeletedMapOf(
							t,
							key,
							id+1,
						)(
							m.CompareAndDelete(key, id+1),
						)
						expectNotSwappedMapOf(
							t,
							key,
							id+1,
							id+2,
						)(
							m.CompareAndSwap(key, id+1, id+2),
						)
						expectNotDeletedMapOf(
							t,
							key,
							id+1,
						)(
							m.CompareAndDelete(key, id+1),
						)
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
						expectNotSwappedMapOf(
							t,
							s,
							math.MaxInt,
							i+1,
						)(
							m.CompareAndSwap(s, math.MaxInt, i+1),
						)
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
					expectLoadedFromSwapMapOf(
						t,
						s,
						i+j,
						i+j+1,
					)(
						m.Swap(s, i+j+1),
					)
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
			expectLoadedFromSwapMapOf(
				t,
				testData[15],
				15,
				16,
			)(
				m.Swap(testData[15], 16),
			)
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
				expectLoadedFromSwapMapOf(
					t,
					testData[i],
					i,
					i+1,
				)(
					m.Swap(testData[i], i+1),
				)
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
						expectNotLoadedFromSwapMapOf(
							t,
							key,
							id,
						)(
							m.Swap(key, id),
						)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromSwapMapOf(
							t,
							key,
							id,
							id,
						)(
							m.Swap(key, id),
						)
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromSwapMapOf(
							t,
							key,
							id,
							id+1,
						)(
							m.Swap(key, id+1),
						)
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
						expectNotLoadedFromSwapMapOf(
							t,
							key,
							id,
						)(
							m.Swap(key, id),
						)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromSwapMapOf(
							t,
							key,
							id,
							id,
						)(
							m.Swap(key, id),
						)
					}
					for _, s := range testData {
						key := makeKey(s)
						expectPresentMapOf(t, key, id)(m.Load(key))
						expectLoadedFromSwapMapOf(
							t,
							key,
							id,
							id+1,
						)(
							m.Swap(key, id+1),
						)
						expectPresentMapOf(t, key, id+1)(m.Load(key))
						expectDeletedMapOf(
							t,
							key,
							id+1,
						)(
							m.CompareAndDelete(key, id+1),
						)
						expectNotLoadedFromSwapMapOf(
							t,
							key,
							id+2,
						)(
							m.Swap(key, id+2),
						)
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
			expectLoadedFromDeleteMapOf(
				t,
				testData[15],
				15,
			)(
				m.LoadAndDelete(testData[15]),
			)
			expectMissingMapOf(t, testData[15], 0)(m.Load(testData[15]))
			expectNotLoadedFromDeleteMapOf(
				t,
				testData[15],
				0,
			)(
				m.LoadAndDelete(testData[15]),
			)
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
				expectLoadedFromDeleteMapOf(
					t,
					testData[i],
					i,
				)(
					m.LoadAndDelete(testData[i]),
				)
				expectMissingMapOf(t, testData[i], 0)(m.Load(testData[i]))
				expectNotLoadedFromDeleteMapOf(
					t,
					testData[i],
					0,
				)(
					m.LoadAndDelete(testData[i]),
				)
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

			testAllMapOf(
				t,
				m,
				testDataMapMapOf(testData[:]),
				func(s string, i int) bool {
					expectLoadedFromDeleteMapOf(t, s, i)(m.LoadAndDelete(s))
					return true
				},
			)
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
						expectLoadedFromDeleteMapOf(
							t,
							key,
							id,
						)(
							m.LoadAndDelete(key),
						)
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

func testAllMapOf[K, V comparable](
	t *testing.T,
	m *MapOf[K, V],
	testData map[K]V,
	yield func(K, V) bool,
) {
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

func expectPresentMapOf[K, V comparable](
	t *testing.T,
	key K,
	want V,
) func(got V, ok bool) {
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

func expectMissingMapOf[K, V comparable](
	t *testing.T,
	key K,
	want V,
) func(got V, ok bool) {
	t.Helper()
	if want != *new(V) {
		// This is awkward, but the want argument is necessary to smooth over
		// type inference.
		// Just make sure the want argument always looks the same.
		panic("expectMissingMapOf must always have a zero value variable")
	}
	return func(got V, ok bool) {
		t.Helper()

		if ok {
			t.Errorf(
				"expected key %v to be missing from map, got value %v",
				key,
				got,
			)
		}
		if !ok && got != want {
			t.Errorf(
				"expected missing key %v to be paired with the zero value; got %v",
				key,
				got,
			)
		}
	}
}

func expectLoadedMapOf[K, V comparable](
	t *testing.T,
	key K,
	want V,
) func(got V, loaded bool) {
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

func expectStoredMapOf[K, V comparable](
	t *testing.T,
	key K,
	want V,
) func(got V, loaded bool) {
	t.Helper()
	return func(got V, loaded bool) {
		t.Helper()

		if loaded {
			t.Errorf(
				"expected inserted key %v to have been stored, not loaded",
				key,
			)
		}
		if got != want {
			t.Errorf(
				"expected inserted key %v to have value %v, got %v",
				key,
				want,
				got,
			)
		}
	}
}

func expectDeletedMapOf[K, V comparable](
	t *testing.T,
	key K,
	old V,
) func(deleted bool) {
	t.Helper()
	return func(deleted bool) {
		t.Helper()

		if !deleted {
			t.Errorf(
				"expected key %v with value %v to be in map and deleted",
				key,
				old,
			)
		}
	}
}

func expectNotDeletedMapOf[K, V comparable](
	t *testing.T,
	key K,
	old V,
) func(deleted bool) {
	t.Helper()
	return func(deleted bool) {
		t.Helper()

		if deleted {
			t.Errorf(
				"expected key %v with value %v to not be in map and thus not deleted",
				key,
				old,
			)
		}
	}
}

func expectSwappedMapOf[K, V comparable](
	t *testing.T,
	key K,
	old, new V,
) func(swapped bool) {
	t.Helper()
	return func(swapped bool) {
		t.Helper()

		if !swapped {
			t.Errorf(
				"expected key %v with value %v to be in map and swapped for %v",
				key,
				old,
				new,
			)
		}
	}
}

func expectNotSwappedMapOf[K, V comparable](
	t *testing.T,
	key K,
	old, new V,
) func(swapped bool) {
	t.Helper()
	return func(swapped bool) {
		t.Helper()

		if swapped {
			t.Errorf(
				"expected key %v with value %v to not be in map or not swapped for %v",
				key,
				old,
				new,
			)
		}
	}
}

func expectLoadedFromSwapMapOf[K, V comparable](
	t *testing.T,
	key K,
	want, new V,
) func(got V, loaded bool) {
	t.Helper()
	return func(got V, loaded bool) {
		t.Helper()

		if !loaded {
			t.Errorf(
				"expected key %v to be in map and for %v to have been swapped for %v",
				key,
				want,
				new,
			)
		} else if want != got {
			t.Errorf("key %v had its value %v swapped for %v, but expected it to have value %v", key, got, new, want)
		}
	}
}

func expectNotLoadedFromSwapMapOf[K, V comparable](
	t *testing.T,
	key K,
	new V,
) func(old V, loaded bool) {
	t.Helper()
	return func(old V, loaded bool) {
		t.Helper()

		if loaded {
			t.Errorf(
				"expected key %v to not be in map, but found value %v for it",
				key,
				old,
			)
		}
	}
}

func expectLoadedFromDeleteMapOf[K, V comparable](
	t *testing.T,
	key K,
	want V,
) func(got V, loaded bool) {
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

func expectNotLoadedFromDeleteMapOf[K, V comparable](
	t *testing.T,
	key K,
	_ V,
) func(old V, loaded bool) {
	t.Helper()
	return func(old V, loaded bool) {
		t.Helper()

		if loaded {
			t.Errorf(
				"expected key %v to not be in map, but found value %v for it",
				key,
				old,
			)
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
// 				t.Errorf("consecutive cache reads returned different values: a != b (%p
// vs %p)\n", a, b)
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
			t.Fatalf(
				"got unexpected key/value for iteration %d: %v/%v",
				iters,
				key,
				value,
			)
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
	m := NewMapOf[int, int](WithKeyHasher(murmur3Finalizer))
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
	m := NewMapOf[int, int](WithKeyHasher(
		func(i int, _ uintptr) uintptr {
			// We intentionally use an awful hash function here to make sure
			// that the map copes with key collisions.
			return 42
		}),
		WithPresize(numEntries),
	)
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
		v, loaded := m.LoadOrCompute(
			strconv.Itoa(i),
			func() (newValue int, cancel bool) {
				return i, true
			},
		)
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
		v, loaded := m.LoadOrCompute(
			strconv.Itoa(i),
			func() (newValue int, cancel bool) {
				return i, false
			},
		)
		if loaded {
			t.Fatalf("value not computed for %d", i)
		}
		if v != i {
			t.Fatalf("values do not match for %d: %v", i, v)
		}
	}
	for i := 0; i < numEntries; i++ {
		v, loaded := m.LoadOrCompute(
			strconv.Itoa(i),
			func() (newValue int, cancel bool) {
				t.Fatalf("value func invoked")
				return newValue, false
			},
		)
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
	v, ok := m.Compute(
		"foobar",
		func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
			if oldValue != 0 {
				t.Fatalf(
					"oldValue should be 0 when computing a new value: %d",
					oldValue,
				)
			}
			if loaded {
				t.Fatal("loaded should be false when computing a new value")
			}
			newValue = 42
			op = UpdateOp
			return
		},
	)
	if v != 42 {
		t.Fatalf("v should be 42 when computing a new value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when computing a new value")
	}
	// Update an existing value.
	v, ok = m.Compute(
		"foobar",
		func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
			if oldValue != 42 {
				t.Fatalf(
					"oldValue should be 42 when updating the value: %d",
					oldValue,
				)
			}
			if !loaded {
				t.Fatal("loaded should be true when updating the value")
			}
			newValue = oldValue + 42
			op = UpdateOp
			return
		},
	)
	if v != 84 {
		t.Fatalf("v should be 84 when updating the value: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when updating the value")
	}
	// Check that NoOp doesn't update the value
	v, ok = m.Compute(
		"foobar",
		func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
			return 0, CancelOp
		},
	)
	if v != 84 {
		t.Fatalf("v should be 84 after using NoOp: %d", v)
	}
	if !ok {
		t.Fatal("ok should be true when updating the value")
	}
	// Delete an existing value.
	v, ok = m.Compute(
		"foobar",
		func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
			if oldValue != 84 {
				t.Fatalf(
					"oldValue should be 84 when deleting the value: %d",
					oldValue,
				)
			}
			if !loaded {
				t.Fatal("loaded should be true when deleting the value")
			}
			op = DeleteOp
			return
		},
	)
	if v != 84 {
		t.Fatalf("v should be 84 when deleting the value: %d", v)
	}
	if ok {
		t.Fatal("ok should be false when deleting the value")
	}
	// Try to delete a non-existing value. Notice different key.
	v, ok = m.Compute(
		"barbaz",
		func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
			if oldValue != 0 {
				t.Fatalf(
					"oldValue should be 0 when trying to delete a non-existing value: %d",
					oldValue,
				)
			}
			if loaded {
				t.Fatal(
					"loaded should be false when trying to delete a non-existing value",
				)
			}
			// We're returning a non-zero value, but the map should ignore it.
			newValue = 42
			op = DeleteOp
			return
		},
	)
	if v != 0 {
		t.Fatalf(
			"v should be 0 when trying to delete a non-existing value: %d",
			v,
		)
	}
	if ok {
		t.Fatal("ok should be false when trying to delete a non-existing value")
	}
	// Try NoOp on a non-existing value
	v, ok = m.Compute(
		"barbaz",
		func(oldValue int, loaded bool) (newValue int, op ComputeOp) {
			if oldValue != 0 {
				t.Fatalf(
					"oldValue should be 0 when trying to delete a non-existing value: %d",
					oldValue,
				)
			}
			if loaded {
				t.Fatal(
					"loaded should be false when trying to delete a non-existing value",
				)
			}
			// We're returning a non-zero value, but the map should ignore it.
			newValue = 42
			op = CancelOp
			return
		},
	)
	if v != 0 {
		t.Fatalf(
			"v should be 0 when trying to delete a non-existing value: %d",
			v,
		)
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

func TestMapOfStoreThenParallelDelete_DoesNotShrinkBelowMinLen(
	t *testing.T,
) {
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
	if stats.RootBuckets != minTableLen {
		t.Fatalf(
			"table length was different from the minimum: %d",
			stats.RootBuckets,
		)
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
			t.Fatalf(
				"size does not match number of entries in Range: %v, %v",
				size,
				rsize,
			)
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
			t.Fatalf(
				"size does not match number of entries in Range: %v, %v",
				size,
				rsize,
			)
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

//func assertMapOfCapacity[K comparable, V any](
//	t *testing.T,
//	m *MapOf[K, V],
//	expectedCap int,
//) {
//	stats := m.Stats()
//	if stats.Capacity != expectedCap {
//		t.Fatalf(
//			"capacity was different from %d: %d",
//			expectedCap,
//			stats.Capacity,
//		)
//	}
//}

func TestNewMapOfPresized(t *testing.T) {
	// assertMapOfCapacity(t, NewMapOf[string, string](),
	// DefaultMinMapOfTableCap) assertMapOfCapacity(t, NewMapOf[string,
	// string](WithPresize(0)), DefaultMinMapOfTableCap) assertMapOfCapacity(t,
	// NewMapOf[string, string](WithPresize(0)), DefaultMinMapOfTableCap)
	// assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(-100)),
	// DefaultMinMapOfTableCap) assertMapOfCapacity(t, NewMapOf[string,
	// string](WithPresize(-100)), DefaultMinMapOfTableCap)
	// assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(500)), 1280)
	// assertMapOfCapacity(t, NewMapOf[string, string](WithPresize(500)), 1280)
	// assertMapOfCapacity(t, NewMapOf[int, int](WithPresize(1_000_000)),
	// 2621440) assertMapOfCapacity(t, NewMapOf[int,
	// int](WithPresize(1_000_000)), 2621440)
	// assertMapOfCapacity(t, NewMapOf[point, point](WithPresize(100)), 160)
	// assertMapOfCapacity(t, NewMapOf[point, point](WithPresize(100)), 160)

	var capacity, expectedCap int
	capacity, expectedCap = NewMapOf[string, string]().Stats().
		Capacity, defaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(0),
	).Stats().
		Capacity, defaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(0),
	).Stats().
		Capacity, defaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(-100),
	).Stats().
		Capacity, defaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(-100),
	).Stats().
		Capacity, defaultMinMapOfTableCap
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(500),
	).Stats().
		Capacity, calcTableLen(
		500,
	)*entriesPerBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(500),
	).Stats().
		Capacity, calcTableLen(
		500,
	)*entriesPerBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(1_000_000),
	).Stats().
		Capacity, calcTableLen(
		1_000_000,
	)*entriesPerBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(1_000_000),
	).Stats().
		Capacity, calcTableLen(
		1_000_000,
	)*entriesPerBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(100),
	).Stats().
		Capacity, calcTableLen(
		100,
	)*entriesPerBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
	capacity, expectedCap = NewMapOf[string, string](
		WithPresize(100),
	).Stats().
		Capacity, calcTableLen(
		100,
	)*entriesPerBucket
	if capacity != expectedCap {
		t.Fatalf("capacity was different from %d: %d", expectedCap, capacity)
	}
}

func TestNewMapOfPresized_DoesNotShrinkBelowMinLen(t *testing.T) {
	const minLen = 1024
	const numEntries = int(minLen*float64(entriesPerBucket)*loadFactor) - entriesPerBucket
	m := NewMapOf[int, int](WithPresize(numEntries), WithShrinkEnabled())
	for i := 0; i < 2*numEntries; i++ {
		m.Store(i, i)
	}

	stats := m.Stats()
	if stats.RootBuckets < minLen {
		t.Fatalf("table did not grow: %d", stats.RootBuckets)
	}

	for i := 0; i < 2*numEntries; i++ {
		m.Delete(i)
	}

	m.Shrink()

	stats = m.Stats()
	if stats.RootBuckets != minLen {
		t.Fatalf("table length was different from the minimum: %v", stats)
	}
}

func TestNewMapOfGrowOnly_OnlyShrinksOnClear(t *testing.T) {
	const minLen = 128
	const numEntries = minLen * entriesPerBucket
	m := NewMapOf[int, int](WithPresize(numEntries))

	stats := m.Stats()
	initialTableLen := stats.RootBuckets

	for i := 0; i < 2*numEntries; i++ {
		m.Store(i, i)
	}
	stats = m.Stats()
	maxTableLen := stats.RootBuckets
	if maxTableLen <= minLen {
		t.Fatalf("table did not grow: %d", maxTableLen)
	}

	for i := 0; i < numEntries; i++ {
		m.Delete(i)
	}
	stats = m.Stats()
	if stats.RootBuckets != maxTableLen {
		t.Fatalf(
			"table length was different from the expected: %d",
			stats.RootBuckets,
		)
	}

	m.Clear()
	stats = m.Stats()
	if stats.RootBuckets != initialTableLen {
		t.Fatalf(
			"table length was different from the initial: %d",
			stats.RootBuckets,
		)
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
	// fastStringHasher has a certain collision rate, which may slightly
	// increase the required capacity.
	expectedCapacity := 2 * (calcTableLen(numEntries) * entriesPerBucket) // + (numEntries/entriesPerBucket + 1)
	if stats.Capacity > expectedCapacity {
		t.Fatalf(
			"capacity was too large: %d, expected: %d",
			stats.Capacity,
			expectedCapacity,
		)
	}
	if stats.RootBuckets <= minTableLen {
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
	expectedCapacity = stats.RootBuckets * entriesPerBucket
	if stats.Capacity != expectedCapacity {
		t.Logf(
			"capacity was too large: %d, expected: %d",
			stats.Capacity,
			expectedCapacity,
		)
	}
	if stats.RootBuckets != minTableLen {
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

func parallelSeqTypedResizer(
	m *MapOf[int, int],
	numEntries int,
	positive bool,
	cdone chan bool,
) {
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

func parallelRandTypedResizer(
	t *testing.T,
	m *MapOf[string, int],
	numIters, numEntries int,
	cdone chan bool,
) {
	// r := rand1.New(rand1.NewSource(time.Now().UnixNano()))
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
	const numEntries = 2 * entriesPerBucket * minTableLen
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
		t.Fatalf(
			"size does not match number of entries in Range: %v, %v",
			s,
			rs,
		)
	}
}

func parallelRandTypedClearer(
	t *testing.T,
	m *MapOf[string, int],
	numIters, numEntries int,
	cdone chan bool,
) {
	// r := rand1.New(rand1.NewSource(time.Now().UnixNano()))
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
		t.Fatalf(
			"size does not match number of entries in Range: %v, %v",
			s,
			rs,
		)
	}
}

func parallelSeqTypedStorer(
	t *testing.T,
	m *MapOf[string, int],
	storeEach, numIters, numEntries int,
	cdone chan bool,
) {
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

func parallelRandTypedStorer(
	t *testing.T,
	m *MapOf[string, int],
	numIters, numEntries int,
	cdone chan bool,
) {
	// r := rand1.New(rand1.NewSource(time.Now().UnixNano()))
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

func parallelRandTypedDeleter(
	t *testing.T,
	m *MapOf[string, int],
	numIters, numEntries int,
	cdone chan bool,
) {
	// r := rand1.New(rand1.NewSource(time.Now().UnixNano()))
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

func parallelTypedLoader(
	t *testing.T,
	m *MapOf[string, int],
	numIters, numEntries int,
	cdone chan bool,
) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			// Due to atomic snapshots we must either see no entry, or a "<j>"/j
			// pair.
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

func parallelTypedComputer(
	m *MapOf[uint64, uint64],
	numIters, numEntries int,
	cdone chan bool,
) {
	for i := 0; i < numIters; i++ {
		for j := 0; j < numEntries; j++ {
			m.Compute(
				uint64(j),
				func(oldValue uint64, loaded bool) (newValue uint64, op ComputeOp) {
					return oldValue + 1, UpdateOp
				},
			)
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

func parallelTypedRangeStorer(
	m *MapOf[int, int],
	numEntries int,
	stopFlag *int64,
	cdone chan bool,
) {
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

func parallelTypedRangeDeleter(
	m *MapOf[int, int],
	numEntries int,
	stopFlag *int64,
	cdone chan bool,
) {
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

func parallelTypedShrinker(
	t *testing.T,
	m *MapOf[uint64, *point],
	numIters, numEntries int,
	stopFlag *int64,
	cdone chan bool,
) {
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

func parallelTypedUpdater(
	t *testing.T,
	m *MapOf[uint64, *point],
	idx int,
	stopFlag *int64,
	cdone chan bool,
) {
	for atomic.LoadInt64(stopFlag) != 1 {
		sleepUs := rand.IntN(10)
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
	if stats.RootBuckets != minTableLen {
		t.Fatalf("unexpected number of root buckets: %s", stats.ToString())
	}
	if stats.TotalBuckets != stats.RootBuckets {
		t.Fatalf("unexpected number of total buckets: %s", stats.ToString())
	}
	if stats.EmptyBuckets != stats.RootBuckets {
		t.Fatalf("unexpected number of empty buckets: %s", stats.ToString())
	}
	if stats.Capacity != entriesPerBucket*minTableLen {
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
	if stats.RootBuckets != calcTableLen(200) {
		t.Fatalf("unexpected number of root buckets: %s", stats.ToString())
	}
	if stats.TotalBuckets < stats.RootBuckets {
		t.Fatalf("unexpected number of total buckets: %s", stats.ToString())
	}
	if stats.EmptyBuckets >= stats.RootBuckets {
		t.Fatalf("unexpected number of empty buckets: %s", stats.ToString())
	}
	if stats.Capacity != entriesPerBucket*stats.TotalBuckets {
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
			op := rand.IntN(1000)
			i := rand.IntN(benchmarkNumEntries)
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
			m := NewMapOf[int, int](
				WithKeyHasher(murmur3Finalizer),
				WithPresize(benchmarkNumEntries),
			)
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
			op := rand.IntN(1000)
			i := rand.IntN(benchmarkNumEntries)
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
	opsPerSec := float64(b.N) / time.Since(start).Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

const (
	// number of entries to use in benchmarks
	benchmarkNumEntries = 1_000
	// key prefix used in benchmarks
	benchmarkKeyPrefix = "what_a_looooooooooooooooooooooong_key_prefix_"

	defaultMinMapOfTableCap = minTableLen * entriesPerBucket
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
			t.Fatalf(
				"expected cloned empty map size to be 0, got: %d",
				clone.Size(),
			)
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
			t.Fatalf(
				"expected cloned map size to be %d, got: %d",
				numEntries,
				clone.Size(),
			)
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
			t.Fatalf(
				"expected map size to remain 2 after merging with nil, got: %d",
				m.Size(),
			)
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
			t.Fatalf(
				"expected map size to remain 2 after merging with empty map, got: %d",
				m.Size(),
			)
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
		expectPresentMapOf(
			t,
			"c",
			3,
		)(
			m1.Load("c"),
		) // Should be overwritten with value from m2
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
		m1.Merge(
			m2,
			func(this, other *EntryOf[string, int]) *EntryOf[string, int] {
				return &EntryOf[string, int]{Value: this.Value + other.Value}
			},
		)

		if m1.Size() != 4 {
			t.Fatalf("expected merged map size to be 4, got: %d", m1.Size())
		}
		expectPresentMapOf(t, "a", 1)(m1.Load("a"))
		expectPresentMapOf(t, "b", 2)(m1.Load("b"))
		expectPresentMapOf(
			t,
			"c",
			33,
		)(
			m1.Load("c"),
		) // Should be sum of values (30+3)
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
			t.Fatalf(
				"expected empty map to remain empty, got size: %d",
				m.Size(),
			)
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
				t.Fatalf(
					"expected only even values, found odd value: %d",
					value,
				)
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
				expectPresentMapOf(
					t,
					key,
					i*2,
				)(
					m.Load(key),
				) // Even numbers doubled
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
		var entries []EntryOf[string, int]

		previous, loaded := m.BatchUpsert(entries)

		if len(previous) != 0 {
			t.Fatalf(
				"expected empty previous values slice, got length: %d",
				len(previous),
			)
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
			t.Fatalf(
				"expected previous values slice length 3, got: %d",
				len(previous),
			)
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check all entries should have zero values and loaded=false
		for i, val := range previous {
			if val != 0 {
				t.Fatalf(
					"expected zero value for new entry at index %d, got: %d",
					i,
					val,
				)
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
			t.Fatalf(
				"expected previous values slice length 3, got: %d",
				len(previous),
			)
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check previous values and loaded status
		expectedPrevious := []int{100, 200, 0}
		expectedLoaded := []bool{true, true, false}

		for i := range entries {
			if previous[i] != expectedPrevious[i] {
				t.Fatalf(
					"expected previous[%d] to be %d, got: %d",
					i,
					expectedPrevious[i],
					previous[i],
				)
			}
			if loaded[i] != expectedLoaded[i] {
				t.Fatalf(
					"expected loaded[%d] to be %v, got: %v",
					i,
					expectedLoaded[i],
					loaded[i],
				)
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
			t.Fatalf(
				"expected previous values slice length %d, got: %d",
				numEntries,
				len(previous),
			)
		}
		if len(loaded) != numEntries {
			t.Fatalf(
				"expected loaded slice length %d, got: %d",
				numEntries,
				len(loaded),
			)
		}

		// All entries should be new
		for i := 0; i < numEntries; i++ {
			if loaded[i] {
				t.Fatalf("expected loaded[%d] to be false for new entry", i)
			}
		}

		// Verify map size
		if m.Size() != numEntries {
			t.Fatalf(
				"expected map size to be %d, got: %d",
				numEntries,
				m.Size(),
			)
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
		var entries []EntryOf[string, int]

		actual, loaded := m.BatchInsert(entries)

		if len(actual) != 0 {
			t.Fatalf(
				"expected empty actual values slice, got length: %d",
				len(actual),
			)
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
			t.Fatalf(
				"expected actual values slice length 3, got: %d",
				len(actual),
			)
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check all entries should have inserted values and loaded=false
		for i, entry := range entries {
			if actual[i] != entry.Value {
				t.Fatalf(
					"expected actual[%d] to be %d, got: %d",
					i,
					entry.Value,
					actual[i],
				)
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
			t.Fatalf(
				"expected actual values slice length 3, got: %d",
				len(actual),
			)
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check actual values and loaded status
		expectedActual := []int{
			100,
			200,
			3,
		} // Existing values for a,b; new value for c
		expectedLoaded := []bool{true, true, false}

		for i := range entries {
			if actual[i] != expectedActual[i] {
				t.Fatalf(
					"expected actual[%d] to be %d, got: %d",
					i,
					expectedActual[i],
					actual[i],
				)
			}
			if loaded[i] != expectedLoaded[i] {
				t.Fatalf(
					"expected loaded[%d] to be %v, got: %v",
					i,
					expectedLoaded[i],
					loaded[i],
				)
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
			t.Fatalf(
				"expected actual values slice length %d, got: %d",
				numEntries,
				len(actual),
			)
		}
		if len(loaded) != numEntries {
			t.Fatalf(
				"expected loaded slice length %d, got: %d",
				numEntries,
				len(loaded),
			)
		}

		// All entries should be new
		for i := 0; i < numEntries; i++ {
			if loaded[i] {
				t.Fatalf("expected loaded[%d] to be false for new entry", i)
			}
			if actual[i] != i {
				t.Fatalf(
					"expected actual[%d] to be %d, got: %d",
					i,
					i,
					actual[i],
				)
			}
		}

		// Verify map size
		if m.Size() != numEntries {
			t.Fatalf(
				"expected map size to be %d, got: %d",
				numEntries,
				m.Size(),
			)
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
		var keys []string

		previous, loaded := m.BatchDelete(keys)

		if len(previous) != 0 {
			t.Fatalf(
				"expected empty previous values slice, got length: %d",
				len(previous),
			)
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
			t.Fatalf(
				"expected previous values slice length 3, got: %d",
				len(previous),
			)
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// All entries should have zero values and loaded=false
		for i := range keys {
			if previous[i] != 0 {
				t.Fatalf(
					"expected previous[%d] to be 0 for non-existent key, got: %d",
					i,
					previous[i],
				)
			}
			if loaded[i] {
				t.Fatalf(
					"expected loaded[%d] to be false for non-existent key",
					i,
				)
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
			t.Fatalf(
				"expected previous values slice length 3, got: %d",
				len(previous),
			)
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// Check previous values and loaded status
		expectedPrevious := []int{1, 3, 0}
		expectedLoaded := []bool{true, true, false}

		for i := range keys {
			if previous[i] != expectedPrevious[i] {
				t.Fatalf(
					"expected previous[%d] to be %d, got: %d",
					i,
					expectedPrevious[i],
					previous[i],
				)
			}
			if loaded[i] != expectedLoaded[i] {
				t.Fatalf(
					"expected loaded[%d] to be %v, got: %v",
					i,
					expectedLoaded[i],
					loaded[i],
				)
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
			t.Fatalf(
				"expected previous values slice length %d, got: %d",
				numEntries/2,
				len(previous),
			)
		}
		if len(loaded) != numEntries/2 {
			t.Fatalf(
				"expected loaded slice length %d, got: %d",
				numEntries/2,
				len(loaded),
			)
		}

		// All deleted entries should have correct values and loaded=true
		for i := 0; i < numEntries/2; i++ {
			expectedValue := i * 2 // Even numbers
			if previous[i] != expectedValue {
				t.Fatalf(
					"expected previous[%d] to be %d, got: %d",
					i,
					expectedValue,
					previous[i],
				)
			}
			if !loaded[i] {
				t.Fatalf("expected loaded[%d] to be true for existing key", i)
			}
		}

		// Verify map size
		if m.Size() != numEntries/2 {
			t.Fatalf(
				"expected map size to be %d, got: %d",
				numEntries/2,
				m.Size(),
			)
		}

		// Check random samples - even numbers should be gone, odd numbers
		// present
		for i := 0; i < 20; i++ {
			key := strconv.Itoa(i)
			if i%2 == 0 {
				expectMissingMapOf(
					t,
					key,
					0,
				)(
					m.Load(key),
				) // Even numbers deleted
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
		var entries []EntryOf[string, int]

		previous, loaded := m.BatchUpdate(entries)

		if len(previous) != 0 {
			t.Fatalf(
				"expected empty previous values slice, got length: %d",
				len(previous),
			)
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
			t.Fatalf(
				"expected previous values slice length 3, got: %d",
				len(previous),
			)
		}
		if len(loaded) != 3 {
			t.Fatalf("expected loaded slice length 3, got: %d", len(loaded))
		}

		// All entries should have zero values and loaded=false
		for i := range entries {
			if previous[i] != 0 {
				t.Fatalf(
					"expected previous[%d] to be 0 for non-existent key, got: %d",
					i,
					previous[i],
				)
			}
			if loaded[i] {
				t.Fatalf(
					"expected loaded[%d] to be false for non-existent key",
					i,
				)
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
			t.Fatalf(
				"expected previous values slice length 4, got: %d",
				len(previous),
			)
		}
		if len(loaded) != 4 {
			t.Fatalf("expected loaded slice length 4, got: %d", len(loaded))
		}

		// Check previous values and loaded status
		expectedPrevious := []int{100, 200, 0, 400}
		expectedLoaded := []bool{true, true, false, true}

		for i := range entries {
			if previous[i] != expectedPrevious[i] {
				t.Fatalf(
					"expected previous[%d] to be %d, got: %d",
					i,
					expectedPrevious[i],
					previous[i],
				)
			}
			if loaded[i] != expectedLoaded[i] {
				t.Fatalf(
					"expected loaded[%d] to be %v, got: %v",
					i,
					expectedLoaded[i],
					loaded[i],
				)
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
			m.Store(
				strconv.Itoa(i),
				i*10,
			) // Store with a different value to check updates
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
			t.Fatalf(
				"expected previous values slice length %d, got: %d",
				numEntries,
				len(previous),
			)
		}
		if len(loaded) != numEntries {
			t.Fatalf(
				"expected loaded slice length %d, got: %d",
				numEntries,
				len(loaded),
			)
		}

		// Verify results - odd numbers should be updated, even numbers should
		// not be affected
		for i := 0; i < numEntries; i++ {
			if i%2 == 1 { // Odd numbers - should be updated
				if !loaded[i] {
					t.Fatalf(
						"expected loaded[%d] to be true for existing key",
						i,
					)
				}
				if previous[i] != i*10 {
					t.Fatalf(
						"expected previous[%d] to be %d, got: %d",
						i,
						i*10,
						previous[i],
					)
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
			t.Fatalf(
				"expected map size to be %d, got: %d",
				expectedSize,
				m.Size(),
			)
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

		m.RangeProcessEntry(
			func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
				processCount++
				return loaded // No modification
			},
		)

		if processCount != 0 {
			t.Fatalf(
				"expected process count to be 0 for empty map, got: %d",
				processCount,
			)
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
		m.RangeProcessEntry(
			func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
				processCount++
				// Double all values
				return &EntryOf[string, int]{
					Key:   loaded.Key,
					Value: loaded.Value * 2,
				}
			},
		)

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
		m.RangeProcessEntry(
			func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
				if loaded.Value%2 == 0 {
					return nil // Delete entry
				}
				return loaded // Keep entry
			},
		)

		// Verify only odd-numbered entries remain
		expectedSize := 5
		if m.Size() != expectedSize {
			t.Fatalf(
				"expected size to be %d after deletion, got: %d",
				expectedSize,
				m.Size(),
			)
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

		m.RangeProcessEntry(
			func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
				value := loaded.Value
				switch {
				case value%3 == 0:
					// Divisible by 3: delete
					return nil
				case value%3 == 1:
					// Remainder 1: multiply by 10
					return &EntryOf[string, int]{
						Key:   loaded.Key,
						Value: value * 10,
					}
				default:
					// Remainder 2: keep unchanged
					return loaded
				}
			},
		)

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
		m.RangeProcessEntry(
			func(loaded *EntryOf[string, int]) *EntryOf[string, int] {
				// Just return the same entry
				return loaded
			},
		)

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
			t.Fatalf(
				"expected previous value to be zero for non-existent key, got: %d",
				previous,
			)
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
				t.Fatalf(
					"expected previous value to be %d for iteration %d, got: %d",
					expectedPrevious,
					i,
					previous,
				)
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
			t.Fatalf(
				"expected loaded=true and previous='one', got loaded=%v, previous='%s'",
				loaded,
				previous,
			)
		}

		// Try non-existent key
		previous, loaded = m.LoadAndUpdate(3, "three")
		if loaded || previous != "" {
			t.Fatalf(
				"expected loaded=false and previous='', got loaded=%v, previous='%s'",
				loaded,
				previous,
			)
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
			t.Fatalf(
				"expected loaded to be true for existing key with zero value",
			)
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

func TestMapOfShrink_MinLen(t *testing.T) {
	m := NewMapOf[string, int](WithPresize(1000))
	initialStats := m.Stats()
	minBuckets := initialStats.RootBuckets

	for i := 0; i < 10; i++ {
		m.Store(strconv.Itoa(i), i)
	}

	m.Shrink()

	afterShrinkStats := m.Stats()
	if afterShrinkStats.RootBuckets < minBuckets {
		t.Fatalf("Shrink should not go below minLen: min=%d, after=%d",
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
					t.Errorf(
						"Data corruption during concurrent resize: key=%d",
						key,
					)
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
		t.Logf(
			"Pre-allocation might be slower than expected: without=%v, with=%v",
			duration1,
			duration2,
		)
	}

	stats1 := m1.Stats()
	stats2 := m2.Stats()

	if stats2.TotalGrowths > stats1.TotalGrowths {
		t.Fatalf(
			"Pre-allocated map should have fewer growths: pre=%d, normal=%d",
			stats2.TotalGrowths,
			stats1.TotalGrowths,
		)
	}

	t.Logf(
		"Without pre-allocation: %v, growths=%d",
		duration1,
		stats1.TotalGrowths,
	)
	t.Logf(
		"With pre-allocation: %v, growths=%d",
		duration2,
		stats2.TotalGrowths,
	)
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
			if actualValue, ok := m.Load(key); !ok ||
				actualValue != expectedValue {
				t.Fatalf(
					"Data corruption after Grow cycle %d: key=%s, expected=%s, actual=%s, ok=%v",
					cycle,
					key,
					expectedValue,
					actualValue,
					ok,
				)
			}
		}

		m.Shrink()

		for key, expectedValue := range testData {
			if actualValue, ok := m.Load(key); !ok ||
				actualValue != expectedValue {
				t.Fatalf(
					"Data corruption after Shrink cycle %d: key=%s, expected=%s, actual=%s, ok=%v",
					cycle,
					key,
					expectedValue,
					actualValue,
					ok,
				)
			}
		}
	}

	stats := m.Stats()
	if stats.Size != numEntries {
		t.Fatalf(
			"Final size mismatch: expected=%d, actual=%d",
			numEntries,
			stats.Size,
		)
	}
}

// TestMapOfDefaultHasher tests the defaultHasher function with different key
// types
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

		expectPresentMapOf(
			t,
			uint64(0x123456789ABCDEF0),
			"large1",
		)(
			m.Load(uint64(0x123456789ABCDEF0)),
		)
		expectPresentMapOf(
			t,
			uint64(0xFEDCBA9876543210),
			"large2",
		)(
			m.Load(uint64(0xFEDCBA9876543210)),
		)
	})

	t.Run("Int64Keys", func(t *testing.T) {
		m := NewMapOf[int64, string]()
		m.Store(int64(-9223372036854775808), "min")
		m.Store(int64(9223372036854775807), "max")

		expectPresentMapOf(
			t,
			int64(-9223372036854775808),
			"min",
		)(
			m.Load(int64(-9223372036854775808)),
		)
		expectPresentMapOf(
			t,
			int64(9223372036854775807),
			"max",
		)(
			m.Load(int64(9223372036854775807)),
		)
	})

	t.Run("Uint32Keys", func(t *testing.T) {
		m := NewMapOf[uint32, string]()
		m.Store(uint32(0xFFFFFFFF), "max32")
		m.Store(uint32(0x12345678), "mid32")

		expectPresentMapOf(
			t,
			uint32(0xFFFFFFFF),
			"max32",
		)(
			m.Load(uint32(0xFFFFFFFF)),
		)
		expectPresentMapOf(
			t,
			uint32(0x12345678),
			"mid32",
		)(
			m.Load(uint32(0x12345678)),
		)
	})

	t.Run("Int32Keys", func(t *testing.T) {
		m := NewMapOf[int32, string]()
		m.Store(int32(-2147483648), "min32")
		m.Store(int32(2147483647), "max32")

		expectPresentMapOf(
			t,
			int32(-2147483648),
			"min32",
		)(
			m.Load(int32(-2147483648)),
		)
		expectPresentMapOf(
			t,
			int32(2147483647),
			"max32",
		)(
			m.Load(int32(2147483647)),
		)
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

// TestMapOfDefaultHasherComprehensive tests all branches of defaultHasher
// function
func TestMapOfDefaultHasherComprehensive(t *testing.T) {
	t.Run("Float32Keys", func(t *testing.T) {
		m := &MapOf[float32, string]{}
		keys := []float32{1.1, 2.2, 3.3, 0.0, -1.1}
		for i, key := range keys {
			m.Store(key, fmt.Sprintf("value%d", i))
		}
		for i, key := range keys {
			if val, found := m.Load(key); !found ||
				val != fmt.Sprintf("value%d", i) {
				t.Fatalf(
					"Expected to find key %v with value value%d, got found=%v, val=%s",
					key,
					i,
					found,
					val,
				)
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
			if val, found := m.Load(key); !found ||
				val != fmt.Sprintf("val%d", i) {
				t.Fatalf(
					"Expected to find key %v with value val%d, got found=%v, val=%s",
					key,
					i,
					found,
					val,
				)
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
			if val, found := m.Load(key); !found ||
				val != fmt.Sprintf("complex%d", i) {
				t.Fatalf(
					"Expected to find key %v with value complex%d, got found=%v, val=%s",
					key,
					i,
					found,
					val,
				)
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
			if val, found := m.Load(key); !found ||
				val != fmt.Sprintf("c128_%d", i) {
				t.Fatalf(
					"Expected to find key %v with value c128_%d, got found=%v, val=%s",
					key,
					i,
					found,
					val,
				)
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
			if val, found := m.Load(key); !found ||
				val != fmt.Sprintf("array%d", i) {
				t.Fatalf(
					"Expected to find key %v with value array%d, got found=%v, val=%s",
					key,
					i,
					found,
					val,
				)
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
				t.Fatalf(
					"Expected to find key %v with value %d, got found=%v, val=%d",
					key,
					i*100,
					found,
					val,
				)
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
				t.Fatal(
					"CompareAndDelete should panic for non-comparable values",
				)
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
		t.Errorf(
			"Expected Load to return false for non-existent key, got true with value %v",
			value,
		)
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
		t.Errorf(
			"LoadEntry and Load returned different values: %v vs %v",
			entry.Value,
			value,
		)
	}

	// Test non-existent key with both functions
	entry = m.LoadEntry("key2")
	value, ok = m.Load("key2")
	if entry != nil {
		t.Errorf(
			"Expected LoadEntry to return nil for non-existent key, got %v",
			entry,
		)
	}
	if ok {
		t.Errorf(
			"Expected Load to return false for non-existent key, got true with value %v",
			value,
		)
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
		t.Errorf(
			"LoadEntry and Load returned different values for key2: %v vs %v",
			entry.Value,
			value,
		)
	}

	// Test key3
	entry = m.LoadEntry("key3")
	value, ok = m.Load("key3")
	if entry == nil || !ok {
		t.Error("Both LoadEntry and Load should find key3")
	}
	if entry != nil && entry.Value != value {
		t.Errorf(
			"LoadEntry and Load returned different values for key3: %v vs %v",
			entry.Value,
			value,
		)
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
			t.Fatalf(
				"expected process count to be 0 for empty iterator, got: %d",
				processCount,
			)
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
					t.Fatalf(
						"expected loaded to be nil for new key, got: %v",
						loaded,
					)
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
					t.Fatalf(
						"expected loaded to be nil for new key %s, got: %v",
						key,
						loaded,
					)
				}
				return &EntryOf[string, int]{Value: value}, value, false
			},
		)

		if processCount != len(expectedItems) {
			t.Fatalf(
				"expected process count to be %d, got: %d",
				len(expectedItems),
				processCount,
			)
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
						t.Fatalf(
							"expected loaded to be nil for new key %s, got: %v",
							key,
							loaded,
						)
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
					t.Fatalf(
						"expected loaded to be non-nil for existing key %s",
						key,
					)
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

// TestMapOf_InitWithOptions tests the InitWithOptions function
func TestMapOf_InitWithOptions(t *testing.T) {
	t.Run("BasicInitialization", func(t *testing.T) {
		var m MapOf[string, int]
		m.InitWithOptions()

		// Test basic operations
		m.Store("key1", 100)
		if val, ok := m.Load("key1"); !ok || val != 100 {
			t.Errorf("Expected key1=100, got %d, exists=%v", val, ok)
		}
	})

	t.Run("WithPresize", func(t *testing.T) {
		var m MapOf[string, int]
		m.InitWithOptions(WithPresize(1000))

		// Verify the map works correctly
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%d", i)
			m.Store(key, i)
		}

		if m.Size() != 100 {
			t.Errorf("Expected size 100, got %d", m.Size())
		}
	})

	t.Run("WithShrinkEnabled", func(t *testing.T) {
		var m MapOf[string, int]
		m.InitWithOptions(WithShrinkEnabled())

		// Add and remove items to test shrinking
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%d", i)
			m.Store(key, i)
		}

		// Remove most items
		for i := 0; i < 90; i++ {
			key := fmt.Sprintf("key%d", i)
			m.Delete(key)
		}

		if m.Size() != 10 {
			t.Errorf("Expected size 10, got %d", m.Size())
		}
	})

	t.Run("WithKeyHasher", func(t *testing.T) {
		var m MapOf[string, int]

		// Custom hash function only
		customHash := func(key string, seed uintptr) uintptr {
			return uintptr(len(key))
		}

		m.InitWithOptions(WithKeyHasher(customHash))

		// Test operations
		m.Store("hello", 123)
		if val, ok := m.Load("hello"); !ok || val != 123 {
			t.Errorf("Expected hello=123, got %d, exists=%v", val, ok)
		}
	})

	t.Run("WithValueEqual", func(t *testing.T) {
		var m MapOf[string, int]

		// Custom equality function only
		customEqual := func(val1, val2 int) bool {
			return val1 == val2
		}

		m.InitWithOptions(WithValueEqual(customEqual))

		// Test CompareAndSwap with custom equality
		m.Store("key", 100)
		if !m.CompareAndSwap("key", 100, 200) {
			t.Error("CompareAndSwap should have succeeded")
		}

		if val, ok := m.Load("key"); !ok || val != 200 {
			t.Errorf("Expected key=200, got %d, exists=%v", val, ok)
		}
	})
	t.Run("MultipleOptions", func(t *testing.T) {
		var m MapOf[string, int]

		customHash := func(key string, seed uintptr) uintptr {
			return uintptr(len(key))
		}

		m.InitWithOptions(
			WithPresize(500),
			WithShrinkEnabled(),
			WithKeyHasher(customHash),
		)

		// Test that all options work together
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("key%d", i)
			m.Store(key, i)
		}

		if m.Size() != 50 {
			t.Errorf("Expected size 50, got %d", m.Size())
		}
	})
}

// TestMapOf_init tests the init function
func TestMapOf_init(t *testing.T) {
	t.Run("BasicConfig", func(t *testing.T) {
		var m MapOf[string, int]
		config := &MapConfig{
			SizeHint:      100,
			ShrinkEnabled: false,
		}

		m.init(config)

		// Test basic operations
		m.Store("key1", 100)
		if val, ok := m.Load("key1"); !ok || val != 100 {
			t.Errorf("Expected key1=100, got %d, exists=%v", val, ok)
		}
	})

	t.Run("ConfigWithCustomHasher", func(t *testing.T) {
		var m MapOf[string, int]

		// Create custom hash and equal functions
		customHash := func(ptr unsafe.Pointer, seed uintptr) uintptr {
			key := *(*string)(ptr)
			return uintptr(len(key))
		}

		customEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
			val1 := *(*int)(ptr1)
			val2 := *(*int)(ptr2)
			return val1 == val2
		}

		config := &MapConfig{
			KeyHash:       customHash,
			ValEqual:      customEqual,
			SizeHint:      200,
			ShrinkEnabled: true,
		}

		m.init(config)

		// Test operations
		m.Store("test", 42)
		if val, ok := m.Load("test"); !ok || val != 42 {
			t.Errorf("Expected test=42, got %d, exists=%v", val, ok)
		}

		// Test CompareAndSwap with custom equality
		if !m.CompareAndSwap("test", 42, 84) {
			t.Error("CompareAndSwap should have succeeded")
		}

		if val, ok := m.Load("test"); !ok || val != 84 {
			t.Errorf("Expected test=84, got %d, exists=%v", val, ok)
		}
	})

	t.Run("ConfigReuse", func(t *testing.T) {
		// Test that the same config can be used for multiple maps
		config := &MapConfig{
			SizeHint:      50,
			ShrinkEnabled: true,
		}

		var m1, m2 MapOf[string, int]
		m1.init(config)
		m2.init(config)

		// Test that both maps work independently
		m1.Store("key1", 100)
		m2.Store("key2", 200)

		if val, ok := m1.Load("key1"); !ok || val != 100 {
			t.Errorf("m1: Expected key1=100, got %d, exists=%v", val, ok)
		}

		if val, ok := m2.Load("key2"); !ok || val != 200 {
			t.Errorf("m2: Expected key2=200, got %d, exists=%v", val, ok)
		}

		// Verify they don't interfere with each other
		if _, ok := m1.Load("key2"); ok {
			t.Error("m1 should not contain key2")
		}

		if _, ok := m2.Load("key1"); ok {
			t.Error("m2 should not contain key1")
		}
	})

	t.Run("EmptyConfig", func(t *testing.T) {
		var m MapOf[string, int]
		config := &MapConfig{} // Empty config, should use defaults

		m.init(config)

		// Test basic operations with default settings
		m.Store("default", 999)
		if val, ok := m.Load("default"); !ok || val != 999 {
			t.Errorf("Expected default=999, got %d, exists=%v", val, ok)
		}
	})

	t.Run("ConfigWithOnlyKeyHash", func(t *testing.T) {
		var m MapOf[string, int]

		customHash := func(ptr unsafe.Pointer, seed uintptr) uintptr {
			key := *(*string)(ptr)
			return uintptr(len(key)) * 31 // Simple hash
		}

		config := &MapConfig{
			KeyHash: customHash,
			// ValEqual is nil, should use default
			SizeHint: 100,
		}

		m.init(config)

		// Test operations
		m.Store("hash", 123)
		if val, ok := m.Load("hash"); !ok || val != 123 {
			t.Errorf("Expected hash=123, got %d, exists=%v", val, ok)
		}
	})

	t.Run("ConfigWithOnlyValEqual", func(t *testing.T) {
		var m MapOf[string, int]

		customEqual := func(ptr1, ptr2 unsafe.Pointer) bool {
			val1 := *(*int)(ptr1)
			val2 := *(*int)(ptr2)
			return val1 == val2
		}

		config := &MapConfig{
			// KeyHash is nil, should use default
			ValEqual: customEqual,
			SizeHint: 100,
		}

		m.init(config)

		// Test operations
		m.Store("equal", 456)
		if val, ok := m.Load("equal"); !ok || val != 456 {
			t.Errorf("Expected equal=456, got %d, exists=%v", val, ok)
		}

		// Test CompareAndSwap with custom equality
		if !m.CompareAndSwap("equal", 456, 789) {
			t.Error("CompareAndSwap should have succeeded")
		}
	})
}

// TestMapOf_SetDefaultJSONMarshal tests the SetDefaultJSONMarshal function
func TestMapOf_SetDefaultJSONMarshal(t *testing.T) {
	// Save original functions
	originalMarshal := jsonMarshal
	originalUnmarshal := jsonUnmarshal
	defer func() {
		jsonMarshal = originalMarshal
		jsonUnmarshal = originalUnmarshal
	}()

	t.Run("DefaultBehavior", func(t *testing.T) {
		// Reset to default
		SetDefaultJSONMarshal(nil, nil)

		m := NewMapOf[string, int]()
		m.Store("key1", 100)
		m.Store("key2", 200)

		// Test MarshalJSON with default behavior
		data, err := m.MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON failed: %v", err)
		}

		// Verify it's valid JSON
		var result map[string]int
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Failed to unmarshal result: %v", err)
		}

		if len(result) != 2 {
			t.Errorf("Expected 2 items, got %d", len(result))
		}
		if result["key1"] != 100 || result["key2"] != 200 {
			t.Errorf("Unexpected values: %v", result)
		}

		// Test UnmarshalJSON with default behavior
		m2 := NewMapOf[string, int]()
		if err := m2.UnmarshalJSON(data); err != nil {
			t.Fatalf("UnmarshalJSON failed: %v", err)
		}

		if val, ok := m2.Load("key1"); !ok || val != 100 {
			t.Errorf("Expected key1=100, got %d, exists=%v", val, ok)
		}
		if val, ok := m2.Load("key2"); !ok || val != 200 {
			t.Errorf("Expected key2=200, got %d, exists=%v", val, ok)
		}
	})

	t.Run("CustomMarshalFunctions", func(t *testing.T) {
		// Custom marshal function that adds a prefix
		customMarshal := func(v any) ([]byte, error) {
			data, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			// Add a custom prefix to identify custom marshaling
			return append(
				[]byte(`{"custom":true,"data":`),
				append(data, '}')...), nil
		}

		// Custom unmarshal function that handles the prefix
		customUnmarshal := func(data []byte, v any) error {
			var wrapper struct {
				Custom bool        `json:"custom"`
				Data   interface{} `json:"data"`
			}
			if err := json.Unmarshal(data, &wrapper); err != nil {
				return err
			}
			if !wrapper.Custom {
				return json.Unmarshal(data, v)
			}
			// Re-marshal the data part and unmarshal into target
			dataBytes, err := json.Marshal(wrapper.Data)
			if err != nil {
				return err
			}
			return json.Unmarshal(dataBytes, v)
		}

		// Set custom functions
		SetDefaultJSONMarshal(customMarshal, customUnmarshal)

		m := NewMapOf[string, int]()
		m.Store("test", 42)

		// Test custom marshal
		data, err := m.MarshalJSON()
		if err != nil {
			t.Fatalf("Custom MarshalJSON failed: %v", err)
		}

		// Verify custom format
		if !strings.Contains(string(data), `"custom":true`) {
			t.Errorf("Custom marshal format not detected in: %s", string(data))
		}

		// Test custom unmarshal
		m2 := NewMapOf[string, int]()
		if err := m2.UnmarshalJSON(data); err != nil {
			t.Fatalf("Custom UnmarshalJSON failed: %v", err)
		}

		if val, ok := m2.Load("test"); !ok || val != 42 {
			t.Errorf("Expected test=42, got %d, exists=%v", val, ok)
		}
	})

	t.Run("MarshalError", func(t *testing.T) {
		// Custom marshal that always returns an error
		errorMarshal := func(v any) ([]byte, error) {
			return nil, fmt.Errorf("custom marshal error")
		}

		SetDefaultJSONMarshal(errorMarshal, nil)

		m := NewMapOf[string, int]()
		m.Store("key", 123)

		_, err := m.MarshalJSON()
		if err == nil {
			t.Error("Expected marshal error, got nil")
		}
		if !strings.Contains(err.Error(), "custom marshal error") {
			t.Errorf("Expected custom error message, got: %v", err)
		}
	})

	t.Run("UnmarshalError", func(t *testing.T) {
		// Custom unmarshal that always returns an error
		errorUnmarshal := func(data []byte, v any) error {
			return fmt.Errorf("custom unmarshal error")
		}

		SetDefaultJSONMarshal(nil, errorUnmarshal)

		m := NewMapOf[string, int]()
		data := []byte(`{"key":123}`)

		err := m.UnmarshalJSON(data)
		if err == nil {
			t.Error("Expected unmarshal error, got nil")
		}
		if !strings.Contains(err.Error(), "custom unmarshal error") {
			t.Errorf("Expected custom error message, got: %v", err)
		}
	})

	t.Run("NilFunctionsResetToDefault", func(t *testing.T) {
		// Set custom functions first
		customMarshal := func(v any) ([]byte, error) {
			return []byte(`{"custom":true}`), nil
		}
		SetDefaultJSONMarshal(customMarshal, nil)

		// Reset to default by passing nil
		SetDefaultJSONMarshal(nil, nil)

		m := NewMapOf[string, int]()
		m.Store("key", 123)

		// Should use standard library now
		data, err := m.MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON failed: %v", err)
		}

		// Should not contain custom format
		if strings.Contains(string(data), `"custom":true`) {
			t.Errorf(
				"Should use default marshal, but got custom format: %s",
				string(data),
			)
		}

		// Verify it's valid standard JSON
		var result map[string]int
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Failed to unmarshal with standard library: %v", err)
		}
		if result["key"] != 123 {
			t.Errorf("Expected key=123, got %d", result["key"])
		}
	})
}

func TestMapOf_HashUint64On32Bit(t *testing.T) {
	val := uint64(0x123456789ABCDEF0)
	hash := hashUint64On32Bit(unsafe.Pointer(&val), 0)
	// The function XORs the lower 32 bits with the upper 32 bits
	expected := uint32(0x9ABCDEF0) ^ uint32(0x12345678)
	if uint32(hash) != expected {
		t.Errorf("Expected hash %x, got %x", expected, hash)
	}
}

func TestMapOf_UnlockWithMeta(t *testing.T) {
	t.Run("MapOf", func(t *testing.T) {
		m := NewMapOf[string, int]()

		// Get a bucket to test UnlockWithMeta
		key := "test"
		hash := m.keyHash(unsafe.Pointer(&key), m.seed)
		table := (*mapOfTable)(atomic.LoadPointer(&m.table))
		bucketIdx := int(hash) & table.mask
		bucket := table.buckets.At(bucketIdx)

		// Lock the bucket first
		bucket.Lock()

		// Test UnlockWithMeta with different meta values
		testMetas := []uint64{
			0,
			0x123456789ABCDEF0,
			^uint64(0), // max uint64
		}

		for _, meta := range testMetas {
			bucket.UnlockWithMeta(meta)
			// Verify the meta was set correctly (without lock bits)
			storedMeta := atomic.LoadUint64(&bucket.meta)
			expectedMeta := meta &^ opLockMask
			if storedMeta != expectedMeta {
				t.Errorf("UnlockWithMeta(%x): stored meta = %x, want %x", meta, storedMeta, expectedMeta)
			}

			// Lock again for next iteration
			if meta != testMetas[len(testMetas)-1] {
				bucket.Lock()
			}
		}
	})

	t.Run("FlatMapOf", func(t *testing.T) {
		m := NewFlatMapOf[string, int]()

		// Get a bucket to test UnlockWithMeta
		key := "test"
		hash := m.keyHash(unsafe.Pointer(&key), m.seed)
		table := m.table.SeqLoad()
		bucketIdx := int(hash) & table.mask
		bucket := table.buckets.At(bucketIdx)

		// Lock the bucket first
		bucket.Lock()

		// Test UnlockWithMeta with different meta values
		testMetas := []uint64{
			0,
			0x123456789ABCDEF0,
			^uint64(0), // max uint64
		}

		for _, meta := range testMetas {
			bucket.UnlockWithMeta(meta)
			// Verify the meta was set correctly (without lock bits)
			storedMeta := bucket.meta.Load()
			expectedMeta := meta &^ opLockMask
			if storedMeta != expectedMeta {
				t.Errorf("UnlockWithMeta(%x): stored meta = %x, want %x", meta, storedMeta, expectedMeta)
			}

			// Lock again for next iteration
			if meta != testMetas[len(testMetas)-1] {
				bucket.Lock()
			}
		}
	})
}

func TestMapOf_EmbeddedHashOff(t *testing.T) {
	// These functions are no-ops when mapof_opt_embeddedhash build tag is not set
	// But we still need to call them to get coverage

	t.Run("EntryOf", func(t *testing.T) {
		var entry EntryOf[string, int]

		// Test getHash - should return 0
		hash := entry.getHash()
		if hash != 0 {
			t.Errorf("EntryOf.getHash() = %d, want 0", hash)
		}

		// Test setHash - should be a no-op
		entry.setHash(0x12345678)
		// Verify it's still 0 since setHash is a no-op
		hash = entry.getHash()
		if hash != 0 {
			t.Errorf("After setHash, EntryOf.getHash() = %d, want 0", hash)
		}
	})
}

// TestDefaultHasherEdgeCases tests edge cases for defaultHasher to improve coverage
func TestMapOf_DefaultHasherEdgeCases(t *testing.T) {
	keyHash, _, _ := defaultHasher[string, int]()

	// Test with empty string
	emptyStr := ""
	result1 := keyHash(unsafe.Pointer(&emptyStr), 0)
	if result1 == 0 {
		t.Logf("Hash for empty string: %x", result1)
	}

	// Test with different seeds
	testStr := "test"
	result2 := keyHash(unsafe.Pointer(&testStr), 12345)
	result3 := keyHash(unsafe.Pointer(&testStr), 67890)
	if result2 == result3 {
		t.Errorf("Expected different hashes for different seeds")
	}
}

func TestMapOf_IsZero(t *testing.T) {
	t.Run("IsZero", func(t *testing.T) {
		m := NewMapOf[string, int]()
		if !m.IsZero() {
			t.Errorf("New map should be zero")
		}

		m.Store("key", 1)
		if m.IsZero() {
			t.Errorf("Map with elements should not be zero")
		}
	})
}

func TestMapOf_GCAndWeakReferences(t *testing.T) {
	// Force garbage collection to test weak reference cleanup paths
	runtime.GC()
	runtime.GC() // Call twice to ensure cleanup

	// Test with maps that might trigger resize operations
	m := NewMapOf[int, string]()

	// Add many elements to trigger resize
	for i := 0; i < 1000; i++ {
		m.Store(i, "value")
	}

	// Force GC again
	runtime.GC()

	// Verify map still works
	if val, ok := m.Load(500); !ok || val != "value" {
		t.Errorf("Map should still work after GC")
	}
}

// TestMapOf_LoadAndDelete tests the LoadAndDelete method comprehensively
func TestMapOf_LoadAndDelete(t *testing.T) {
	t.Run("LoadAndDelete_ExistingKey", func(t *testing.T) {
		m := NewMapOf[string, int]()
		
		// Store a value
		m.Store("key1", 100)
		
		// LoadAndDelete should return the value and true
		value, loaded := m.LoadAndDelete("key1")
		if !loaded || value != 100 {
			t.Errorf("LoadAndDelete(key1) = (%v, %v), want (100, true)", value, loaded)
		}
		
		// Key should no longer exist
		if _, ok := m.Load("key1"); ok {
			t.Error("Key should be deleted after LoadAndDelete")
		}
	})
	
	t.Run("LoadAndDelete_NonExistentKey", func(t *testing.T) {
		m := NewMapOf[string, int]()
		
		// LoadAndDelete on non-existent key should return zero value and false
		value, loaded := m.LoadAndDelete("nonexistent")
		if loaded || value != 0 {
			t.Errorf("LoadAndDelete(nonexistent) = (%v, %v), want (0, false)", value, loaded)
		}
	})
	
	t.Run("LoadAndDelete_MultipleKeys", func(t *testing.T) {
		m := NewMapOf[int, string]()
		
		// Store multiple values
		for i := 0; i < 10; i++ {
			m.Store(i, fmt.Sprintf("value_%d", i))
		}
		
		// Delete even keys using LoadAndDelete
		for i := 0; i < 10; i += 2 {
			expected := fmt.Sprintf("value_%d", i)
			value, loaded := m.LoadAndDelete(i)
			if !loaded || value != expected {
				t.Errorf("LoadAndDelete(%d) = (%v, %v), want (%s, true)", i, value, loaded, expected)
			}
		}
		
		// Verify even keys are deleted and odd keys remain
		for i := 0; i < 10; i++ {
			value, ok := m.Load(i)
			if i%2 == 0 {
				// Even keys should be deleted
				if ok {
					t.Errorf("Key %d should be deleted, but found value %v", i, value)
				}
			} else {
				// Odd keys should remain
				expected := fmt.Sprintf("value_%d", i)
				if !ok || value != expected {
					t.Errorf("Key %d: expected (%s, true), got (%v, %v)", i, expected, value, ok)
				}
			}
		}
	})
	
	t.Run("LoadAndDelete_Concurrent", func(t *testing.T) {
		m := NewMapOf[int, int]()
		const numKeys = 1000
		
		// Store initial values
		for i := 0; i < numKeys; i++ {
			m.Store(i, i*10)
		}
		
		var wg sync.WaitGroup
		deletedKeys := make(map[int]bool)
		var mu sync.Mutex
		
		// Concurrent LoadAndDelete operations
		numWorkers := 10
		keysPerWorker := numKeys / numWorkers
		
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				start := workerID * keysPerWorker
				end := start + keysPerWorker
				
				for i := start; i < end; i++ {
					value, loaded := m.LoadAndDelete(i)
					if loaded {
						expectedValue := i * 10
						if value != expectedValue {
							t.Errorf("Worker %d: LoadAndDelete(%d) = %v, want %v", workerID, i, value, expectedValue)
						}
						
						mu.Lock()
						deletedKeys[i] = true
						mu.Unlock()
					}
				}
			}(w)
		}
		
		wg.Wait()
		
		// Verify all keys were deleted exactly once
		mu.Lock()
		if len(deletedKeys) != numKeys {
			t.Errorf("Expected %d keys to be deleted, got %d", numKeys, len(deletedKeys))
		}
		mu.Unlock()
		
		// Verify map is empty
		if size := m.Size(); size != 0 {
			t.Errorf("Expected map size 0 after all deletions, got %d", size)
		}
	})
}

// TestMapOf_LoadOrStoreFn tests the LoadOrStoreFn method comprehensively
func TestMapOf_LoadOrStoreFn(t *testing.T) {
	t.Run("LoadOrStoreFn_NewKey", func(t *testing.T) {
		m := NewMapOf[string, int]()
		
		called := false
		value, loaded := m.LoadOrStoreFn("key1", func() int {
			called = true
			return 42
		})
		
		if loaded {
			t.Error("LoadOrStoreFn should return loaded=false for new key")
		}
		if value != 42 {
			t.Errorf("LoadOrStoreFn returned value %v, want 42", value)
		}
		if !called {
			t.Error("Function should be called for new key")
		}
		
		// Verify value was stored
		if storedValue, ok := m.Load("key1"); !ok || storedValue != 42 {
			t.Errorf("Stored value = (%v, %v), want (42, true)", storedValue, ok)
		}
	})
	
	t.Run("LoadOrStoreFn_ExistingKey", func(t *testing.T) {
		m := NewMapOf[string, int]()
		
		// Store initial value
		m.Store("key1", 100)
		
		called := false
		value, loaded := m.LoadOrStoreFn("key1", func() int {
			called = true
			return 200
		})
		
		if !loaded {
			t.Error("LoadOrStoreFn should return loaded=true for existing key")
		}
		if value != 100 {
			t.Errorf("LoadOrStoreFn returned value %v, want 100", value)
		}
		if called {
			t.Error("Function should not be called for existing key")
		}
		
		// Verify original value is unchanged
		if storedValue, ok := m.Load("key1"); !ok || storedValue != 100 {
			t.Errorf("Stored value = (%v, %v), want (100, true)", storedValue, ok)
		}
	})
	
	t.Run("LoadOrStoreFn_FunctionPanic", func(t *testing.T) {
		m := NewMapOf[string, int]()
		
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic from function, but didn't get one")
			}
		}()
		
		m.LoadOrStoreFn("key1", func() int {
			panic("test panic")
		})
	})
	
	t.Run("LoadOrStoreFn_ZeroValue", func(t *testing.T) {
		m := NewMapOf[string, int]()
		
		// Test storing zero value
		value, loaded := m.LoadOrStoreFn("key1", func() int {
			return 0
		})
		
		if loaded {
			t.Error("LoadOrStoreFn should return loaded=false for new key")
		}
		if value != 0 {
			t.Errorf("LoadOrStoreFn returned value %v, want 0", value)
		}
		
		// Verify zero value was stored
		if storedValue, ok := m.Load("key1"); !ok || storedValue != 0 {
			t.Errorf("Stored value = (%v, %v), want (0, true)", storedValue, ok)
		}
	})
	
	t.Run("LoadOrStoreFn_Concurrent", func(t *testing.T) {
		m := NewMapOf[int, int]()
		const numKeys = 100
		const numWorkers = 10
		
		var wg sync.WaitGroup
		callCounts := make([]int32, numKeys)
		
		// Concurrent LoadOrStoreFn operations on the same keys
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				
				for i := 0; i < numKeys; i++ {
					m.LoadOrStoreFn(i, func() int {
						atomic.AddInt32(&callCounts[i], 1)
						return i * 100
					})
				}
			}(w)
		}
		
		wg.Wait()
		
		// Verify each function was called exactly once per key
		for i := 0; i < numKeys; i++ {
			count := atomic.LoadInt32(&callCounts[i])
			if count != 1 {
				t.Errorf("Key %d: function called %d times, want 1", i, count)
			}
			
			// Verify correct value was stored
			if value, ok := m.Load(i); !ok || value != i*100 {
				t.Errorf("Key %d: stored value = (%v, %v), want (%d, true)", i, value, ok, i*100)
			}
		}
	})
	
	t.Run("LoadOrStoreFn_PointerTypes", func(t *testing.T) {
		m := NewMapOf[string, *string]()
		
		// Test with pointer types
		value, loaded := m.LoadOrStoreFn("key1", func() *string {
			s := "hello"
			return &s
		})
		
		if loaded {
			t.Error("LoadOrStoreFn should return loaded=false for new key")
		}
		if value == nil || *value != "hello" {
			t.Errorf("LoadOrStoreFn returned unexpected value: %v", value)
		}
		
		// Test loading existing pointer
		value2, loaded2 := m.LoadOrStoreFn("key1", func() *string {
			s := "world"
			return &s
		})
		
		if !loaded2 {
			t.Error("LoadOrStoreFn should return loaded=true for existing key")
		}
		if value2 != value {
			t.Error("LoadOrStoreFn should return same pointer for existing key")
		}
	})
	
	t.Run("LoadOrStoreFn_ComplexTypes", func(t *testing.T) {
		type ComplexValue struct {
			ID   int
			Name string
			Data []int
		}
		
		m := NewMapOf[int, ComplexValue]()
		
		expected := ComplexValue{
			ID:   1,
			Name: "test",
			Data: []int{1, 2, 3},
		}
		
		value, loaded := m.LoadOrStoreFn(1, func() ComplexValue {
			return expected
		})
		
		if loaded {
			t.Error("LoadOrStoreFn should return loaded=false for new key")
		}
		if !reflect.DeepEqual(value, expected) {
			t.Errorf("LoadOrStoreFn returned %+v, want %+v", value, expected)
		}
	})
}
