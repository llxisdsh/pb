<!--
[![GitHub Actions](https://github.com/llxisdsh/pb/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/llxisdsh/pb/actions)
[![Go Reference](https://pkg.go.dev/badge/github.com/llxisdsh/pb.svg)](https://pkg.go.dev/github.com/llxisdsh/pb)
[![Go Report Card](https://goreportcard.com/badge/github.com/llxisdsh/pb)](https://goreportcard.com/report/github.com/llxisdsh/pb)
[![Codecov](https://codecov.io/gh/llxisdsh/pb/branch/main/graph/badge.svg)](https://codecov.io/gh/llxisdsh/pb)
-->

# pb.MapOf

MapOf is a high-performance concurrent map implementation that offers significant
performance improvements over sync.Map in many common scenarios.

Benchmarks show that MapOf can achieve several times better read performance
compared to sync.Map in read-heavy workloads, while also providing competitive
write performance. The actual performance gain varies depending on workload
characteristics, key types, and concurrency patterns.

MapOf is particularly well-suited for scenarios with:

- High read-to-write ratios
- Frequent lookups of existing keys
- Need for atomic operations on values

Key features of pb.MapOf:

- Uses cache-line aligned structures to prevent false sharing
- Automatic CPU Cache Line Size Adaptation.  
  The library automatically adapts to the CPU's cache line size.
  You can also manually specify it using compile-time options like `mapof_opt_cachelinesize_64, 128, 256` etc.
- Enhanced the lock mechanism to ensure consistent performance under highly contended concurrent access
- Counter Performance Optimization.  
  For improved counter performance, use `mapof_opt_enablepadding` to force padding around counters, reducing false sharing.
- Compatibility
  - Already optimized for strong memory models (e.g., x86 TSO, Apple Silicon).
  - Already optimized for Weak memory models.
  - Tested/Validated CPUs with default configurations:
    AMD Ryzen Threadripper 3970X, ARM Neoverse-N2, Apple M3 Ultra, Qualcomm Snapdragon 636 (32bit)
  - Enable `mapof_opt_atomiclevel_1` or `mapof_opt_atomiclevel_2` for better performance on strong memory models.
- Implements zero-value usability for convenient initialization
- Provides lazy initialization for better performance
- Defaults to Go's built-in hash function, customizable on creation or initialization.  
  For high hash computation costs, use `mapof_opt_embeddedhash` to enable hash caching (disabled by default as Go's built-in hashing is generally efficient)
- Offers complete sync.Map API compatibility
- Specially optimized for read operations
- Supports parallel resizing for better scalability
- Includes rich functional extensions such as LoadOrStoreFn, ProcessEntry, Size, IsZero,
  Clone, and batch processing functions
- Thoroughly tested with comprehensive test coverage
- Delivers exceptional performance (see benchmark results below)

pb.MapOf is built upon xsync.MapOf. We extend our thanks to the authors of [xsync](https://github.com/puzpuzpuz/xsync) and reproduce its introduction below:

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

## Benchmarks

Benchmark results (1,000,000 records) show `pb.MapOf` consistently outperforms other implementations, 
achieving the fastest operations for Store (0.5893 ns/op), LoadOrStore (0.4760 ns/op), Load (0.1987 ns/op) 
and Mixed (0.4467 ns/op)

```
goos: windows
goarch: amd64
pkg: github.com/llxisdsh/pb
cpu: AMD Ryzen Threadripper 3970X 32-Core Processor 
```


<details>
<summary> Benchmark test (19/06/2025) </summary>

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

| Implementation                 | Operation   |       Ops/sec |   ns/op | B/op | Allocs/op |
|--------------------------------|-------------|--------------:|--------:|-----:|----------:|
| `original_syncMap`             | Store       |    47,236,741 |   24.27 |   64 |         3 |
|                                | LoadOrStore |    56,023,192 |   19.03 |   17 |         2 |
|                                | Load        |   404,771,172 |    3.66 |    0 |         0 |
|                                | Mixed       |   142,293,642 |    8.60 |   10 |         0 |
| `pb_MapOf` 🏆                  | Store       | 1,000,000,000 |    0.59 |    0 |         0 |
|                                | LoadOrStore | 1,000,000,000 |    0.48 |    0 |         0 |
|                                | Load        | 1,000,000,000 |    0.20 |    0 |         0 |
|                                | Mixed       | 1,000,000,000 |    0.45 |    0 |         0 |
| `xsync_MapOf`                  | Store       |   142,851,074 |    7.60 |   16 |         1 |
|                                | LoadOrStore |   293,945,829 |    3.64 |    0 |         0 |
|                                | Load        |   729,350,713 |    1.64 |    0 |         0 |
|                                | Mixed       |   491,628,387 |    2.20 |    2 |         0 |
| `pb_HashTrieMap`               | Store       |    62,662,484 |   18.30 |   48 |         1 |
|                                | LoadOrStore |    86,982,994 |   11.95 |    1 |         0 |
|                                | Load        |   463,348,550 |    3.58 |    0 |         0 |
|                                | Mixed       |   198,404,397 |    6.16 |    6 |         0 |
| `alphadose_haxmap`             | Store       |    80,418,174 |   14.01 |    9 |         1 |
|                                | LoadOrStore |    75,698,793 |   14.39 |    9 |         1 |
|                                | Load        |   767,338,982 |    1.92 |    0 |         0 |
|                                | Mixed       |   186,024,159 |    5.52 |    2 |         0 |
| `zhangyunhao116_skipmap`       | Store       |    29,587,840 |   39.17 |    9 |         1 |
|                                | LoadOrStore |    37,524,507 |   30.38 |    1 |         0 |
|                                | Load        |   527,466,034 |    2.27 |    0 |         0 |
|                                | Mixed       |   183,319,764 |    6.22 |    1 |         0 |
| `riraccuia_ash`                | Store       |    51,692,203 |   21.81 |   62 |         4 |
|                                | LoadOrStore |    28,938,676 |   41.67 |   94 |         3 |
|                                | Load        |   269,917,735 |    4.64 |    7 |         0 |
|                                | Mixed       |   124,839,320 |    9.94 |   18 |         1 |
| `fufuok_cmap`                  | Store       |    29,590,686 |   38.21 |    1 |         0 |
|                                | LoadOrStore |    43,682,427 |   27.92 |    0 |         0 |
|                                | Load        |   151,875,836 |    7.74 |    0 |         0 |
|                                | Mixed       |    25,265,125 |   46.51 |    0 |         0 |
| `mhmtszr_concurrent_swiss_map` | Store       |    29,849,112 |   40.21 |    1 |         0 |
|                                | LoadOrStore |    27,447,705 |   45.02 |    0 |         0 |
|                                | Load        |   163,669,803 |    7.66 |    0 |         0 |
|                                | Mixed       |    30,439,160 |   36.53 |    0 |         0 |
| `orcaman_concurrent_map`       | Store       |    34,420,544 |   33.46 |    1 |         0 |
|                                | LoadOrStore |    50,175,614 |   25.41 |    1 |         0 |
|                                | Load        |   145,646,887 |    8.70 |    0 |         0 |
|                                | Mixed       |    48,974,994 |   26.18 |    0 |         0 |
| `RWLockShardedMap_256`         | Store       |    69,052,428 |   15.79 |    1 |         0 |
|                                | LoadOrStore |   119,077,255 |    9.45 |    0 |         0 |
|                                | Load        |   287,863,598 |    4.17 |    0 |         0 |
|                                | Mixed       |   172,448,331 |    6.98 |    0 |         0 |
| `RWLockMap`                    | Store       |     4,238,468 |  269.00 |    1 |         0 |
|                                | LoadOrStore |     9,066,169 |  163.10 |    1 |         0 |
|                                | Load        |    33,225,810 |   36.11 |    0 |         0 |
|                                | Mixed       |     9,591,573 |  127.70 |    0 |         0 |
| `snawoot_lfmap`                | Store       |       364,705 | 3153.00 | 7754 |        48 |
|                                | LoadOrStore |     8,887,498 |  205.60 |  518 |         2 |
|                                | Load        |   281,609,623 |    4.22 |    0 |         0 |
|                                | Mixed       |     1,899,039 |  630.00 | 2453 |        10 |


- RWLockShardedMap_256: A 256-shard concurrent map using Go's native map + RWMutex per shard (benchmark reference). 
  Sharding theoretically boosts performance/throughput for any map.


<details>
<summary> Store throughput test (19/06/2025) </summary>

```go

const total = 100_000_000

func testInsert_pb_MapOf(t *testing.T, total int, numCPU int, preSize bool) {
	time.Sleep(1 * time.Second)
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

| Implementation & Case       | Throughput<br>(M ops/s) | Performance Scale      |
|-----------------------------|------------------------:|------------------------|
| pb_MapOf (64/pre)           |                  147.93 | ━━━━━━━━━━━━━━━━━━━━━━ |
| xsync_MapV4 (64/pre)        |                   92.51 | ━━━━━━━━━━━━━          |
| pb_MapOf (64)               |                   83.40 | ━━━━━━━━━━━━           |
| RWLockShardedMap_256 (64)   |                   40.77 | ━━━━━━━━               |
| pb_HashTrieMap (64)         |                   25.54 | ━━━━━                  |
| zhangyunhao116_skipmap (64) |                   25.37 | ━━━━━                  |
| xsync_MapV4 (64)            |                   23.40 | ━━━━━                  |
| original_syncMap (64)       |                   21.84 | ━━━━                   |
| pb_MapOf (1/pre)            |                   20.87 | ━━━━                   |
| pb_MapOf (1)                |                   17.79 | ━━━━                   |
| xsync_MapV4 (1/pre)         |                    6.06 | ━                      |
| xsync_MapV4 (1)             |                    4.89 | ━                      |
| RWLockShardedMap_256 (1)    |                    3.47 | ━                      |
| zhangyunhao116_skipmap (1)  |                    3.38 | ━                      |
| alphadose_haxmap (64/pre)   |                    2.88 | ━                      |
| pb_HashTrieMap (1)          |                    1.73 | ━                      |
| alphadose_haxmap (1/pre)    |                    1.00 | ━                      |
| original_syncMap (1)        |                    1.42 | ━                      |
| alphadose_haxmap (1)        |                    0.92 | ━                      |
| alphadose_haxmap (64)       |                    0.57 | ━                      |

- (1): 1 goroutine without pre-allocation
- (1/pre): 1 goroutine with pre-allocation
- (64): 64 goroutines without pre-allocation
- (64/pre): 64 goroutines with pre-allocation

## Usage

Doc [here](https://pkg.go.dev/github.com/llxisdsh/pb)

```go
import (
    "github.com/llxisdsh/pb"
    "math/rand/v2"
    "testing"
)

func TestMapOf(t *testing.T) {

    var m pb.MapOf[string, int]

    m.LoadOrStore("a", 1)
    m.Load("a")
    m.LoadOrStoreFn("b", func() int {
        return 2
    })
  
    m.ProcessEntry("a", func(e *pb.EntryOf[string, int]) (*pb.EntryOf[string, int], int, bool) {
        if e != nil {
            return nil, e.Value, true
        }
        return &pb.EntryOf[string, int]{Value: rand.Int()}, 0, false
    })
  
    t.Log(m.Size())
    t.Log(m.IsZero())
    t.Log(m.ToMap())
    t.Log(&m)
  
    m.Range(func(k string, v int) bool {
        t.Log(k, v)
        return true
    })
  
    // need go >= 1.23
    for k, v := range m.All() {
        t.Log(k, v)
    }
}
```

## Implementation details

See [mapof flow](mapof_flow.md) for implementation details.

# pb.HashTrieMap

The HashTrieMap is an optimization of the built-in HashTrieMap.

- Supports lazy value generation with LoadOrStoreFn
- Faster than the built-in HashTrieMap by more than 50%
- All tests passed

The optimization for the built-in HashTrieMap is:

- Remove inited field, use root instead
- Lazy initialization, only init on write, and use the resulting root for subsequent logic to reduce atomic calls

## License

Licensed under MIT.
