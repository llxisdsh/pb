[![Go Reference](https://pkg.go.dev/badge/github.com/llxisdsh/pb.svg)](https://pkg.go.dev/github.com/llxisdsh/pb)[![Go Report Card](https://goreportcard.com/badge/github.com/llxisdsh/pb)](https://goreportcard.com/report/github.com/llxisdsh/pb)[![Codecov](https://codecov.io/gh/llxisdsh/pb/branch/main/graph/badge.svg)](https://codecov.io/gh/llxisdsh/pb)[![GitHub Actions](https://github.com/llxisdsh/pb/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/llxisdsh/pb/actions)

# MapOf

MapOf is a high-performance concurrent map optimized for read-dominant and mixed read/write workloads, outperforming sync.Map in many common scenarios. Its core design combines lock-free reads with bucket-level fine-grained synchronization on writes, together with cache-line–aware layout, parallel resizing, and pluggable hashing/equality. This yields high throughput with markedly improved tail-latency stability.

Design highlights
- Lock-free read path; the write path employs bucket-level fine-grained synchronization and backoff to ensure forward progress under high contention.
- Cache-line alignment and suppression of false sharing to reduce cross-line accesses and hot-spot interference.
- CPU cache-line size adaptation (optionally specified at build time), with counter padding to reduce contention overhead.
- Supports preallocation (WithPresize) and automatic shrinking (WithShrinkEnabled) to enhance memory elasticity and cold-start performance.
- Zero-value usability and lazy initialization to reduce integration complexity.
- Full sync.Map API compatibility, plus extensions: LoadOrStoreFn, ProcessEntry, Size, IsZero, Clone, and batch operations.
- Parallel resizing to lower resize tail latency and improve scalability.
- Pluggable hashing and equality: WithKeyHasher / IHashCode / IHashOpts / WithValueEqual / IEqual, enabling domain-specific tuning for keys/values.
- Defaults to Go’s built-in hashing; when hash computation is expensive, embedded hash caching can be enabled via mapof_opt_embeddedhash.

Applicability (overview)
- Suitable for read-intensive caches, online low-latency key–value access, and shared-state management requiring atomic conditional updates; under resource constraints, shrinking and custom hashing can further improve space efficiency and stability.

Validated across multiple platforms with adaptive optimizations for strong memory models (e.g., x86 TSO). See the benchmarks below for details.

## 📊 Comprehensive Benchmarks

Benchmark results consistently show MapOf outperforms other implementations with the fastest operations across Store, 
LoadOrStore, Load, and Mixed workloads.

```
goos: windows
goarch: amd64
pkg: github.com/llxisdsh/pb
cpu: AMD Ryzen Threadripper 3970X 32-Core Processor 
```


<details>
<summary> Benchmark Test (09/01/2025) </summary>

```go
const countStore = 1_000_000
const countLoadOrStore = countStore
const countLoad = min(1_000_000, countStore)

func mixRand(i int) int {
	return i & (8 - 1)
}

func BenchmarkStore_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_original_syncMap(b *testing.B) {

	b.ReportAllocs()
	var m sync.Map
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r := mixRand(i)
			if r == 0 {
				m.Store(i, i)
			} else if r == 1 {
				m.Delete(i)
			} else if r == 2 {
				_, _ = m.LoadOrStore(i, i)
			} else {
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

```
</details>

| Implementation               | Operation   |       Ops/sec |   ns/op | B/op | Allocs/op |
|------------------------------|-------------|--------------:|--------:|-----:|----------:|
| original_syncMap             | Store       |    43,408,107 |   26.24 |   64 |         3 |
|                              | LoadOrStore |    64,053,295 |   19.23 |   17 |         2 |
|                              | Load        |   387,208,314 |    3.80 |    0 |         0 |
|                              | Mixed       |   153,055,444 |    8.24 |   10 |         0 |
| pb_MapOf 🏆                  | Store       | 1,000,000,000 |    0.58 |    0 |         0 |
|                              | LoadOrStore | 1,000,000,000 |    0.47 |    0 |         0 |
|                              | Load        | 1,000,000,000 |    0.18 |    0 |         0 |
|                              | Mixed       | 1,000,000,000 |    0.42 |    0 |         0 |
| pb_FlatMapOf 🥈              | Store       | 1,000,000,000 |    0.93 |    0 |         0 |
|                              | LoadOrStore | 1,000,000,000 |    0.61 |    0 |         0 |
|                              | Load        | 1,000,000,000 |    0.22 |    0 |         0 |
|                              | Mixed       | 1,000,000,000 |    0.65 |    0 |         0 |
| xsync_MapOf                  | Store       |   136,297,424 |    8.47 |   16 |         1 |
|                              | LoadOrStore |   27,398,7921 |    4.00 |    0 |         0 |
|                              | Load        |   74,548,1913 |    1.59 |    0 |         0 |
|                              | Mixed       |   47,340,9570 |    2.27 |    2 |         0 |
| pb_HashTrieMap               | Store       |    62,662,484 |   18.30 |   48 |         1 |
|                              | LoadOrStore |    86,982,994 |   11.95 |    1 |         0 |
|                              | Load        |   463,348,550 |    3.58 |    0 |         0 |
|                              | Mixed       |   198,404,397 |    6.16 |    6 |         0 |
| alphadose_haxmap             | Store       |    80,418,174 |   14.01 |    9 |         1 |
|                              | LoadOrStore |    75,698,793 |   14.39 |    9 |         1 |
|                              | Load        |   767,338,982 |    1.92 |    0 |         0 |
|                              | Mixed       |   186,024,159 |    5.52 |    2 |         0 |
| zhangyunhao116_skipmap       | Store       |    29,587,840 |   39.17 |    9 |         1 |
|                              | LoadOrStore |    37,524,507 |   30.38 |    1 |         0 |
|                              | Load        |   527,466,034 |    2.27 |    0 |         0 |
|                              | Mixed       |   183,319,764 |    6.22 |    1 |         0 |
| riraccuia_ash                | Store       |    51,692,203 |   21.81 |   62 |         4 |
|                              | LoadOrStore |    28,938,676 |   41.67 |   94 |         3 |
|                              | Load        |   269,917,735 |    4.64 |    7 |         0 |
|                              | Mixed       |   124,839,320 |    9.94 |   18 |         1 |
| fufuok_cmap                  | Store       |    29,590,686 |   38.21 |    1 |         0 |
|                              | LoadOrStore |    43,682,427 |   27.92 |    0 |         0 |
|                              | Load        |   151,875,836 |    7.74 |    0 |         0 |
|                              | Mixed       |    25,265,125 |   46.51 |    0 |         0 |
| mhmtszr_concurrent_swiss_map | Store       |    29,849,112 |   40.21 |    1 |         0 |
|                              | LoadOrStore |    27,447,705 |   45.02 |    0 |         0 |
|                              | Load        |   163,669,803 |    7.66 |    0 |         0 |
|                              | Mixed       |    30,439,160 |   36.53 |    0 |         0 |
| orcaman_concurrent_map       | Store       |    34,420,544 |   33.46 |    1 |         0 |
|                              | LoadOrStore |    50,175,614 |   25.41 |    1 |         0 |
|                              | Load        |   145,646,887 |    8.70 |    0 |         0 |
|                              | Mixed       |    48,974,994 |   26.18 |    0 |         0 |
| RWLockShardedMap_256         | Store       |    69,052,428 |   15.79 |    1 |         0 |
|                              | LoadOrStore |   119,077,255 |    9.45 |    0 |         0 |
|                              | Load        |   287,863,598 |    4.17 |    0 |         0 |
|                              | Mixed       |   172,448,331 |    6.98 |    0 |         0 |
| RWLockMap                    | Store       |     4,238,468 |  269.00 |    1 |         0 |
|                              | LoadOrStore |     9,066,169 |  163.10 |    1 |         0 |
|                              | Load        |    33,225,810 |   36.11 |    0 |         0 |
|                              | Mixed       |     9,591,573 |  127.70 |    0 |         0 |
| snawoot_lfmap                | Store       |       364,705 | 3153.00 | 7754 |        48 |
|                              | LoadOrStore |     8,887,498 |  205.60 |  518 |         2 |
|                              | Load        |   281,609,623 |    4.22 |    0 |         0 |
|                              | Mixed       |     1,899,039 |  630.00 | 2453 |        10 |


- RWLockShardedMap_256: A 256-shard concurrent map using Go's native map + RWMutex per shard (benchmark reference). 
  Sharding theoretically boosts performance/throughput for any map.


<details>
<summary> Store Throughput Test (09/23/2025) </summary>

```go

const total = 100_000_000

func testInsert_pb_MapOf(t *testing.T, total int, numCPU int, preSize bool) {
	time.Sleep(2 * time.Second)
	runtime.GC()
	
	var m *MapOf[int, int]
	if preSize {
		m = NewMapOf[int, int](WithPresize(total))
	} else {
		m = NewMapOf[int, int]()
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := total / numCPU

	for i := 0; i < numCPU; i++ {
		go func(start, end int) {
			//defer wg.Done()

			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)

	size := m.Size()
	if size != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}

	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf("Throughput: %.2f million ops/sec", float64(total)/(elapsed.Seconds()*1000000))

	// rand check
	for i := 0; i < 1000; i++ {
		idx := i * (total / 1000)
		if val, ok := m.Load(idx); !ok || val != idx {
			t.Errorf("Expected value %d at key %d, got %d, exists: %v", idx, idx, val, ok)
		}
	}
}

func TestInsert_pb_MapOf(t *testing.T) {
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_pb_MapOf(t, total, 1, false)
	})

	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsert_pb_MapOf(t, total, runtime.GOMAXPROCS(0), false)
	})
	t.Run("1 pre_size", func(t *testing.T) {
		testInsert_pb_MapOf(t, total, 1, true)
	})

	t.Run("64 pre_size", func(t *testing.T) {
		testInsert_pb_MapOf(t, total, runtime.GOMAXPROCS(0), true)
	})
}
```
</details>

| Implementation & Case       | Throughput<br>(M ops/s) | Performance Scale        |
|-----------------------------|------------------------:|--------------------------|
| pb_FlatMapOf (64/pre)       |                  262.42 | ━━━━━━━━━━━━━━━━━━━━━━━━ |
| pb_FlatMapOf (64)           |                  108.37 | ━━━━━━━━━━━━━━━          |
| pb_FlatMapOf (1/pre)        |                   31.44 | ━━━━━━━━                 |
| pb_FlatMapOf (1)            |                   22.93 | ━━━━━━                   |
| pb_MapOf (64/pre)           |                  178.09 | ━━━━━━━━━━━━━━━━━        |
| pb_MapOf (64)               |                   94.21 | ━━━━━━━━━━━              |
| pb_MapOf (1/pre)            |                   26.83 | ━━━━━━━                  |
| pb_MapOf (1)                |                   21.45 | ━━━━━                    |
| xsync_MapV4 (64/pre)        |                   91.83 | ━━━━━━━━━━━              |
| xsync_MapV4 (64)            |                   25.47 | ━━━━━━━                  |
| xsync_MapV4 (1/pre)         |                    5.86 | ━━                       |
| xsync_MapV4 (1)             |                    3.23 | ━━                       |
| pb_HashTrieMap (64)         |                   25.54 | ━━━━━━━                  |
| pb_HashTrieMap (1)          |                    1.73 | ━                        |
| zhangyunhao116_skipmap (64) |                   25.37 | ━━━━━━━                  |
| zhangyunhao116_skipmap (1)  |                    3.38 | ━━                       |
| RWLockShardedMap_256 (64)   |                   21.92 | ━━━━━                    |
| RWLockShardedMap_256 (1)    |                    3.47 | ━━                       |
| original_syncMap (64)       |                   21.84 | ━━━━━                    |
| original_syncMap (1)        |                    1.42 | ━                        |
| alphadose_haxmap (64/pre)   |                    2.88 | ━                        |
| alphadose_haxmap (64)       |                    0.57 |                          |
| alphadose_haxmap (1/pre)    |                    1.00 | ━                        |
| alphadose_haxmap (1)        |                    0.92 | ━                        |


- (1): 1 goroutine without pre-allocation
- (1/pre): 1 goroutine with pre-allocation
- (64): 64 goroutines without pre-allocation
- (64/pre): 64 goroutines with pre-allocation

<details>
<summary> Memory Usage Test (08/04/2025) </summary>

```go
func Test_MemoryPeakReduction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}
	
	const numItems = 100000
	
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	
	m := NewMapOf[int, int]()
	
	for i := 0; i < numItems; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	
	peak := m2.Alloc - m1.Alloc
	t.Logf("pb_MapOf memory usage: %d bytes, items: %d", peak, m.Size())
}
```
</details>

Benchmark results indicate that MapOf matches the native Go map in memory efficiency, and even outperforms it in most scenarios.
![Memory Usage Comparison (log scale)](./res/memory_usage_comparison_log_scale.png)
![Memory Usage Comparison (Linear scale)](./res/memory_usage_comparison_linear_scale.png)

## ⚡ Performance Tips

To achieve exceptional performance, MapOf employs multiple optimization strategies:

### Optimization Strategies

#### 1. Built-in Optimizations (Automatic)
- **Integer Key Optimization**: Uses raw key values as hash for integer types, delivering 4-6x performance improvement
- **String Key Optimization**: Automatically enabled for short string and []byte keys (≤12 bytes), achieving 2-3x performance improvement
- **Memory Layout Optimization**: Cache-line aligned structures prevent false sharing

**Note**: Default optimizations may increase hash collision rates in some scenarios. For domain-specific optimizations, see custom hash functions in the Usage section.

#### 2. Configurable Optimizations
- **Pre-sizing**: Use `WithPresize(n)` to avoid rehashing during initial population
- **Shrink Control**: Use `WithShrinkEnabled()` to automatically reduce memory usage when map size decreases
- **Custom Hash Functions**: Implement `IHashCode` interface or use `WithKeyHasher()` for specialized key types (see Usage section for examples)

#### 3. Performance Guidelines
- Custom hash functions provide significant gains for domain-specific key types
- Interface implementations are automatically detected at initial time
- `With*` functions take precedence over interface implementations
- Well-designed custom hash functions typically outweigh collision risks

## 🚀 Quick Start

### Installation

```bash
go get github.com/llxisdsh/pb@latest
```


### Prerequisites

The MapOf implementation uses `golang.org/x/sys` to determine the system's `CacheLineSize`.
For optimal performance, ensure your build environment has the latest version of this dependency:
```
go get golang.org/x/sys@latest
```

### Usage

#### 1. Basic Initialization

```go
package main

import (
    "github.com/llxisdsh/pb"
)

func main() {
    // Zero-value initialization with lazy loading
    var cache pb.MapOf[string, int]

    // Optional initialization, Must be called once before use.
    cache.InitWithOptions(pb.WithPresize(1000000))

    // Direct initialization with options
    cache2 := pb.NewMapOf[string, int](pb.WithPresize(1000000), pb.WithShrinkEnabled())
}
```

#### 2. Basic Read/Write Operations

```go
import (
    "fmt"
    "github.com/llxisdsh/pb"
)

func basicOperations() {
    cache := pb.NewMapOf[string, int]()

    // Store and load values
    cache.Store("key1", 100)
    cache.Store("key2", 200)

    value, exists := cache.Load("key1")
    if exists {
        fmt.Printf("key1: %d\n", value)
    }

    // Load or store atomically
    actual, loaded := cache.LoadOrStore("key3", 300)
    if loaded {
        fmt.Printf("key3 already exists: %d\n", actual)
    } else {
        fmt.Printf("key3 stored: %d\n", actual)
    }

    cache.Delete("key2")
}
```

#### 3. Atomic Operations

```go
import (
    "fmt"
    "github.com/llxisdsh/pb"
)

func atomicOperations() {
    cache := pb.NewMapOf[string, int]()
    cache.Store("key1", 100)

    // Atomic swap
    oldValue, swapped := cache.Swap("key1", 150)
    if swapped {
        fmt.Printf("Swapped key1 from %d to 150\n", oldValue)
    }

    // Compare and swap
    swapped = cache.CompareAndSwap("key1", 150, 175)
    if swapped {
        fmt.Println("Successfully swapped key1 from 150 to 175")
    }

    // Compare and delete
    deleted := cache.CompareAndDelete("key1", 175)
    if deleted {
        fmt.Println("Successfully deleted key1 with value 175")
    }

    // Lazy value generation
    result, loaded := cache.LoadOrStoreFn("computed:key", func() int {
        fmt.Println("Computing expensive value...")
        return 43
    })
    fmt.Printf("Result: %d, Loaded: %t\n", result, loaded)

    // ProcessEntry: Atomic conditional processing with complex business logic
    cache.ProcessEntry("user:789", func(entry *pb.EntryOf[string, int]) (*pb.EntryOf[string, int], int, bool) {
        if entry != nil {
            // Update existing entry
            newEntry := &pb.EntryOf[string, int]{Value: entry.Value + 1}
            return newEntry, entry.Value, true
        }
        // Create new entry
        return &pb.EntryOf[string, int]{Value: 1}, 0, false
    })

    // LoadAndDelete: Atomic read-and-delete operation
    deletedValue, wasPresent := cache.LoadAndDelete("user:456")
    fmt.Printf("Deleted value: %d, Was present: %t\n", deletedValue, wasPresent)

    // LoadAndUpdate: Atomic read-and-update operation
    previousValue, wasUpdated := cache.LoadAndUpdate("counter", 1)
    fmt.Printf("Previous: %d, Updated: %t\n", previousValue, wasUpdated)
}
```

#### 4. Custom Hash Functions and Equality

```go
import (
    "slices"
    "github.com/llxisdsh/pb"
)

func customOptimizations() {
    // Configuration Priority (highest to lowest):
    //   - Explicit With* functions (WithKeyHasher, WithValueEqual)
    //   - Interface implementations (IHashCode, IHashOpts, IEqual)
    //   - Default built-in implementations (defaultHasher) - fallback
    
    // Method 1: Using With* functions for runtime configuration

    // Built-in hasher: better for long strings (lower collisions) but slower for short strings (2-3x)
    stringCache := pb.NewMapOf[string, int](pb.WithBuiltInHasher[string]())

    // Custom hash function for pointer keys
    type UserID struct {
        UserID   int64
        TenantID int64
    }
    userCache := pb.NewMapOf[UserID, string](pb.WithKeyHasher(func(key UserID, seed uintptr) uintptr {
        return uintptr(key.UserID) ^ seed
    }))

    // Method 2: Interface-based (runtime init check)
    type CustomKey struct {
        ID   int64
        Name string
    }

    // Implement IHashCode for custom hashing
    func (c *CustomKey) HashCode(uintptr) uintptr {
        return uintptr(c.ID)
    }

    // Custom value type with IEqual for non-comparable types
    type UserProfile struct {
        Name string
        Tags []string // slice makes this non-comparable
    }

    func (u *UserProfile) Equal(other UserProfile) bool {
        return u.Name == other.Name && slices.Equal(u.Tags, other.Tags)
    }

    // Automatically detects and uses interfaces
    var interfaceCache MapOf[CustomKey, UserProfile]
    // Or
    interfaceCache2 := pb.NewMapOf[CustomKey, UserProfile]()
}
```

#### 5. Iteration Operations

```go
import (
    "fmt"
    "github.com/llxisdsh/pb"
)

func iterationOperations() {
    cache := pb.NewMapOf[string, int]()
    cache.Store("key1", 100)
    cache.Store("key2", 200)

    // Range over all key-value pairs
    cache.Range(func(key string, value int) bool {
        fmt.Printf("Key: %s, Value: %d\n", key, value)
        return true // continue iteration
    })

    // RangeEntry: Iterate over all entry pointers (more efficient)
    // Note: loaded parameter is guaranteed to be non-nil during iteration
    cache.RangeEntry(func(loaded *pb.EntryOf[string, int]) bool {
        fmt.Printf("%s: %d\n", loaded.Key, loaded.Value)
        return true
    })

    // All: Go 1.23+ iterator support
    for key, value := range cache.All() {
        fmt.Printf("%s: %d\n", key, value)
    }
}
```

#### 6. Batch Operations

```go
import (
    "fmt"
    "maps"
    "github.com/llxisdsh/pb"
)

func batchOperations() {
    cache := pb.NewMapOf[string, int]()

    // Store multiple values
    batchData := map[string]int{
        "batch1": 1000,
        "batch2": 2000,
        "batch3": 3000,
    }
    cache.FromMap(batchData)

    // Load multiple values
    keys := []string{"batch1", "batch2", "nonexistent"}
    for _, key := range keys {
        if value, ok := cache.Load(key); ok {
            fmt.Printf("%s: %d\n", key, value)
        } else {
            fmt.Printf("%s: not found\n", key)
        }
    }

    // Delete multiple values using BatchDelete
    cache.BatchDelete([]string{"batch1", "batch2"})

    // Batch process all entries
    // Note: loaded parameter is guaranteed to be non-nil during iteration
    cache.RangeProcessEntry(func(loaded *pb.EntryOf[string, int]) *pb.EntryOf[string, int] {
        if loaded.Value < 100 {
            // Double all values less than 100
            return &pb.EntryOf[string, int]{Value: loaded.Value * 2}
        }
        return loaded // Keep unchanged
    })

    // Process all entries with callback (Go 1.23+ iterator support)
    for e := range cache.ProcessAll() {
        switch e.Key() {
            case "batch1":
                e.Update(e.Value() + 500)
            case "batch2":
                e.Delete()
            default:
                // no-op
        }
    }

    // Process entries with specified keys (Go 1.23+ iterator support)
    for e := range cache.ProcessSpecified("batch1", "batch4") {
        if e.Loaded() && e.Value() < 1000 {
            e.Delete()
        }
    }

    // BatchProcess: Batch process iterator data
    data := map[string]int{"a": 1, "b": 2, "c": 3}
    cache.BatchProcess(maps.All(data), func(_ string, v int, loaded *pb.EntryOf[string, int]) (*pb.EntryOf[string, int], int, bool) {
        return &pb.EntryOf[string, int]{Value: v * 10}, v, true
    })
}
```

#### 7. Capacity Management

```go
import (
    "fmt"
    "github.com/llxisdsh/pb"
)

func capacityManagement() {
    cache := pb.NewMapOf[string, int]()

    // Get current size
    size := cache.Size()
    fmt.Printf("Current size: %d\n", size)

    // Check if map is zero-value
    if cache.IsZero() {
        fmt.Println("Cache is empty")
    }

    // Pre-allocate capacity to avoid frequent resizing
    cache.Grow(10000)

    // Manual shrink
    cache.Shrink()

    // Clear all entries
    cache.Clear()
}
```

#### 8. Data Conversion and Serialization

```go
import (
    "encoding/json"
    "fmt"
    "github.com/llxisdsh/pb"
)

func dataConversionAndSerialization() {
    cache := pb.NewMapOf[string, int]()
    cache.Store("key1", 100)
    cache.Store("key2", 200)

    // Clone: Deep copy the entire map
    clonedCache := cache.Clone()
    fmt.Printf("Cloned size: %d\n", clonedCache.Size())

    // Convert to Go map
    goMap := cache.ToMap()
    fmt.Printf("Go map: %+v\n", goMap)

    // Load from Go map
    newData := map[string]int{
        "new1": 100,
        "new2": 200,
    }
    cache.FromMap(newData)

    // JSON serialization
    jsonData, err := json.Marshal(&cache)
    if err == nil {
        fmt.Printf("JSON: %s\n", jsonData)
    }

    // JSON deserialization
    var newCache pb.MapOf[string, int]
    err = json.Unmarshal(jsonData, &newCache)
    if err == nil {
        fmt.Println("Successfully deserialized from JSON")
    }

    // String representation
    fmt.Printf("Cache contents: %s\n", cache.String())

    // Stats: Get detailed performance statistics
    stats := cache.Stats()
    fmt.Printf("Stats: Buckets=%d, Growths=%d, Shrinks=%d\n",
        stats.RootBuckets, stats.TotalGrowths, stats.TotalShrinks)
}
```


### Compile-Time Optimizations

Optimize performance for specific use cases with build tags. Warning: Incorrect optimization choices may degrade performance. Choose carefully based on your actual scenario.

```bash
# === Cache Line Size Optimization ===
# Optimize based on target CPU's cache line size
# Auto-detection is default, but manual specification may be needed for cross-compilation
go build -tags mapof_opt_cachelinesize_32   # For some embedded systems
go build -tags mapof_opt_cachelinesize_64   # For most modern CPUs
go build -tags mapof_opt_cachelinesize_128  # For some high-end server CPUs
go build -tags mapof_opt_cachelinesize_256  # For some specialized architectures

# === Counter Performance Optimization ===
# Add padding around counters to reduce false sharing in high-concurrency scenarios
# Note: Increases memory usage - suitable for memory-abundant, high-concurrency environments
go build -tags mapof_opt_enablepadding

# === Hash Caching Optimization ===
# Cache hash values in entries, suitable for expensive hash computation scenarios
# Note: Go's built-in hashing is usually fast; this may increase memory overhead without performance gain
go build -tags mapof_opt_embeddedhash

# === Combined Optimization Examples ===

# Build with optimizations for FUJITSU A64FX's CMG-based ccNUMA architecture:
go build -tags "mapof_opt_cachelinesize_256 mapof_opt_enablepadding"

# Complex key types configuration:
go build -tags "mapof_opt_embeddedhash"
```

Optimization Selection Guide:

- Default Configuration : Suitable for most scenarios, no additional tags needed
- NUMA architecture: Consider mapof_opt_enablepadding
- Complex Key Types : If hash computation is expensive, consider mapof_opt_embeddedhash
- Cross-Compilation : May need manual mapof_opt_cachelinesize_* specification
  Performance Testing Recommendation: Before production use, benchmark different optimization combinations against your specific workload to determine the best configuration.

## 📚 Documentation

- Complete API documentation is available at [pkg.go.dev/github.com/llxisdsh/pb](https://pkg.go.dev/github.com/llxisdsh/pb)

- See [mapof flow](mapof_flow.md) for implementation details.


## 🙏 Acknowledgments

pb.MapOf builds upon the excellent work of [xsync](https://github.com/puzpuzpuz/xsync)(v3, MIT licensed). We extend our gratitude to the xsync authors and the broader Go community.
Reproduce its introduction below:

```
MapOf is like a Go map[K]V but is safe for concurrent
use by multiple goroutines without additional locking or
coordination. It follows the interface of sync.Map with
a number of valuable extensions like Compute or Size.

A MapOf must not be copied after first use.

MapOf uses a modified version of Cache-Line Hash Table (CLHT)
data structure: https://github.com/LPD-EPFL/CLHT

CLHT is built around idea to organize the hash table in
cache-line-sized buckets, so that on all modern CPUs update
operations complete with at most one cache-line transfer.
Also, Get operations involve no write to memory, as well as no
mutexes or any other sort of locks. Due to this design, in all
considered scenarios MapOf outperforms sync.Map.

MapOf also borrows ideas from Java's j.u.c.ConcurrentHashMap
(immutable K/V pair structs instead of atomic snapshots)
and C++'s absl::flat_hash_map (meta memory and SWAR-based lookups).
```

---

# FlatMapOf

FlatMapOf is a seqlock-based, flat-layout concurrent hash table. The table and key/value entries are stored inline to minimize pointer chasing and cache misses, providing more stable latency and throughput even for cold working sets. Prefer creating instances via `NewFlatMapOf` and configure via options.

Design and concurrency semantics (overview)
- Read path (seqlock validation): Each bucket maintains a sequence. Readers load s1; if s1 is even, they read metadata/entries and then load s2. If s1==s2 (and even), the read is consistent; otherwise, readers spin and retry, and, if needed, fall back to a short locked slow path.
- Write path (ordered publication): While holding the root-bucket lock (opLock), flip the bucket sequence to odd (enter write state) → apply modifications → flip back to even (publish a consistent view), and finally release the root lock.
- Resizing (parallel copy): A help–copy–publish protocol partitions the table and migrates buckets in parallel. When migration completes, the new table is atomically published. Ongoing reads/writes are minimally affected.

Memory layout and cache behavior
- Inlined K/V entries: Entries are laid out contiguously with their bucket, reducing extra pointer dereferences and cross-line accesses. Under cold data and low cache-hit scenarios, this significantly reduces p99/p999 latency jitter.
- Value size: V is not limited by machine word size; any value type is supported. Note that large V increases the per-bucket footprint (see “Limitations”).

Time and space characteristics (intuition)
- Expected complexity: Load/Store are amortized O(1) under uniform hashing and a reasonable load factor.
- Progress guarantees: Reads are lock-free in the absence of write contention; under contention, they use spin + backoff and fall back to a short locked slow path to avoid livelock. Writes are protected by fine-grained, bucket-level mutual exclusion.

Systematic comparison with MapOf
- Concurrency control model:
  - FlatMapOf: bucket-level seqlock + root-bucket mutual exclusion; reads are “optimistic + sequence validation,” writes are “odd/even sequence toggle + ordered publication.”
  - MapOf: CLHT-inspired design; read path performs no writes (lock-free) and uses SWAR metadata for fast matching; writes use fine-grained synchronization at the bucket level.
- Read latency and cold data:
  - FlatMapOf: the inline layout avoids multi-hop pointer chasing under cold states, yielding more predictable hit latency and smoother tail latency.
  - MapOf: extremely fast for hot data; with colder data and more cross-object pointer traversals, latency stability can be slightly worse than FlatMapOf.
- Writes and contention:
  - FlatMapOf: writes toggle bucket sequences and hold the root lock; under sustained high write contention, reads may experience bounded retries or slow-path fallbacks, and throughput may degrade earlier than in a read-optimized MapOf.
  - MapOf: pointer-level updates with a mature write path; often easier to sustain throughput in write-heavy scenarios.
- Memory footprint and density:
  - FlatMapOf: inline entries avoid “one allocation per entry,” but incur “hole cost” (reserved space in partially filled buckets); when V is large or occupancy is sparse, bucket size inflation increases baseline memory usage.
  - MapOf: buckets store pointers and entries are separate objects; with automatic shrinking (WithShrinkEnabled), MapOf is typically more memory-elastic and efficient, especially for large V or underutilized tables.
- API capabilities and ecosystem:
  - FlatMapOf: provides core operations (Load/Store/Delete/LoadOrStore/LoadOrStoreFn/Range/Process/RangeProcess/Size/IsZero). It does not currently offer dedicated convenience APIs such as CompareAndSwap/CompareAndDelete/LoadAndUpdate/Swap, but such semantics can be expressed using Process.
  - MapOf: a more complete API surface including multiple CompareAnd*, Swap, and batch operations, plus richer customization of equality and hashing, making it more mature for migration and production use.
- Hashing and optimizations:
  - Both support WithKeyHasher/IHashCode/IHashOpts; FlatMapOf also supports WithPresize and WithShrinkEnabled.

Advantages (FlatMapOf)
- More stable hit latency and tail behavior in cold working sets and random-access workloads.
- Inline layout reduces pointer chasing and cache misses; Range/scan-style operations benefit from better sequential locality.
- Parallel resize support; arbitrary value sizes are supported.

Limitations and caveats (FlatMapOf)
- Large V or sparse occupancy amplifies the “hole cost,” increasing memory consumption.
- Under sustained write contention with longer critical sections, reads may retry or fall back to the slow path, reducing throughput.
- Dedicated CompareAnd* convenience APIs are currently unavailable; use Process to compose equivalent atomic semantics.
- Experimental implementation; future APIs and semantics may be refined. Prefer NewFlatMapOf for initialization and explicit configuration.

Usage guidance and boundaries
- Prefer FlatMapOf for:
  - Read-dominant or mixed workloads with strong p99/p999 latency requirements on the online path;
  - Cold or low-cache-hit distributions where predictable hit latency is critical;
  - Small-to-medium value types where modest space trade-offs improve latency and throughput;
  - Range/scan tasks that benefit from improved sequential locality.
- Prefer MapOf for:
  - Memory efficiency under resource constraints, or large value types with sparse occupancy;
  - A full set of CompareAnd* / Swap / LoadAndUpdate convenience APIs;
  - Write-heavy or high-contention workloads where a more mature write path is desirable.

API overview
- Provided: Load, Store, Delete, LoadOrStore, LoadOrStoreFn, Range, Process, RangeProcess, Size, IsZero.
- Compositional semantics: Process can express atomic read-modify-write, conditional updates, and conditional deletes.
- Construction and configuration: supports WithPresize and WithShrinkEnabled for capacity and shrinking; supports custom hashing and distribution via IHashCode/IHashOpts/WithKeyHasher.

---

# HashTrieMap


**HashTrieMap** is a highly optimized implementation of Go's built-in `HashTrieMap`, delivering **50%+ performance improvements** while maintaining full compatibility.

## 🎯 Key Improvements

- **Lazy Value Generation**: `LoadOrStoreFn` support for expensive computations
- **50%+ Performance Gain**: Optimized initialization and atomic operations
- **Full Compatibility**: Drop-in replacement for built-in HashTrieMap
- **Comprehensive Testing**: All original tests pass plus additional validation

## 🔧 Technical Optimizations

- **Eliminated `inited` Field**: Uses root pointer directly for state management
- **Lazy Initialization**: Defers initialization until first write operation
- **Hash Caching**: Caches hash values to accelerate expand operations

---

# 📄 License

Licensed under [MIT License](LICENSE).
