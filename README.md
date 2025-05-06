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
- Automatic CPU Cache Line Size Adaptation
  The library automatically adapts to the CPU's cache line size.
  You can also manually specify it using compile-time options like `mapof_opt_cachelinesize_64, 128, 256` etc.
- Enhanced the lock mechanism to ensure consistent performance under highly contended concurrent access
- Counter Performance Optimization
  For improved counter performance, use `mapof_opt_enablepadding` to force padding around counters, reducing false sharing.
- Compatibility
  - Already optimized for strong memory models (e.g., x86 TSO, Apple Silicon).
  - Already optimized for Weak memory models.
  - Tested/Validated CPUs with default configurations:
    AMD Ryzen Threadripper 3970X, ARM Neoverse-N2, Apple M3 Ultra, Qualcomm Snapdragon 636 (32bit)
  - Enable `mapof_opt_atomiclevel_1` or `mapof_opt_atomiclevel_2` for better performance on strong memory models.
- Implements zero-value usability for convenient initialization
- Provides lazy initialization for better performance
- Defaults to Go's built-in hash function, customizable on creation or initialization
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
achieving the fastest operations for Store (2.189 ns/op), LoadOrStore (1.814 ns/op), Load (0.8523 ns/op) 
and Mixed (1.234 ns/op)

```
goos: windows
goarch: amd64
pkg: github.com/llxisdsh/pb
cpu: AMD Ryzen Threadripper 3970X 32-Core Processor 
```


<details>
<summary> Benchmark test (06/05/2025) </summary>

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
| `original_syncMap`             | Store       |    46,531,014 |   24.41 |   64 |         3 |
|                                | LoadOrStore |    60,441,524 |   19.30 |   17 |         2 |
|                                | Load        |   375,209,530 |    2.89 |    0 |         0 |
|                                | Mixed       |   164,278,850 |    7.21 |   10 |         0 |
| `pb_MapOf` 🏆                  | Store       |   467,693,181 |    2.19 |    0 |         0 |
|                                | LoadOrStore |   635,808,417 |    1.81 |    0 |         0 |
|                                | Load        | 1,000,000,000 |    0.85 |    0 |         0 |
|                                | Mixed       |   968,326,521 |    1.23 |    0 |         0 |
| `xsync_MapOf`                  | Store       |   142,139,265 |    7.51 |   16 |         1 |
|                                | LoadOrStore |   292,532,157 |    3.63 |    0 |         0 |
|                                | Load        |   729,079,518 |    1.63 |    0 |         0 |
|                                | Mixed       |   489,392,817 |    2.18 |    2 |         0 |
| `pb_HashTrieMap`               | Store       |    62,482,783 |   18.93 |   48 |         1 |
|                                | LoadOrStore |    90,458,169 |   12.24 |    1 |         0 |
|                                | Load        |   456,243,578 |    3.40 |    0 |         0 |
|                                | Mixed       |   208,924,814 |    5.51 |    6 |         0 |
| `alphadose_haxmap`             | Store       |    79,409,194 |   14.24 |    9 |         1 |
|                                | LoadOrStore |    76,111,706 |   14.94 |    9 |         1 |
|                                | Load        |   787,876,678 |    1.70 |    0 |         0 |
|                                | Mixed       |   186,593,911 |    6.36 |    2 |         0 |
| `zhangyunhao116_skipmap`       | Store       |    28,405,459 |   38.19 |    9 |         1 |
|                                | LoadOrStore |    37,777,665 |   30.03 |    1 |         0 |
|                                | Load        |   542,767,107 |    2.24 |    0 |         0 |
|                                | Mixed       |   189,910,978 |    6.33 |    1 |         0 |
| `riraccuia_ash`                | Store       |    48,641,658 |   22.64 |   64 |         4 |
|                                | LoadOrStore |    25,341,742 |   41.41 |   93 |         3 |
|                                | Load        |   285,900,188 |    4.67 |    7 |         0 |
|                                | Mixed       |   126,458,580 |   10.34 |   18 |         1 |
| `fufuok_cmap`                  | Store       |    29,800,485 |   38.57 |    1 |         0 |
|                                | LoadOrStore |    43,924,989 |   28.24 |    0 |         0 |
|                                | Load        |   160,612,963 |    7.45 |    0 |         0 |
|                                | Mixed       |    24,816,204 |   46.44 |    0 |         0 |
| `mhmtszr_concurrent_swiss_map` | Store       |    29,914,742 |   40.02 |    1 |         0 |
|                                | LoadOrStore |    26,722,189 |   44.93 |    0 |         0 |
|                                | Load        |   161,653,869 |    7.34 |    0 |         0 |
|                                | Mixed       |    31,748,383 |   35.99 |    0 |         0 |
| `orcaman_concurrent_map`       | Store       |    34,855,551 |   33.92 |    1 |         0 |
|                                | LoadOrStore |    50,066,128 |   25.31 |    1 |         0 |
|                                | Load        |   143,455,968 |    8.71 |    0 |         0 |
|                                | Mixed       |    47,188,359 |   26.23 |    0 |         0 |
| `RWLockShardedMap`             | Store       |    27,887,066 |   41.01 |    1 |         0 |
|                                | LoadOrStore |    34,666,850 |   35.13 |    1 |         0 |
|                                | Load        |   123,064,906 |    9.51 |    0 |         0 |
|                                | Mixed       |    45,097,692 |   27.04 |    0 |         0 |
| `RWLockMap`                    | Store       |     4,296,895 |  267.70 |    1 |         0 |
|                                | LoadOrStore |     9,053,979 |  166.30 |    1 |         0 |
|                                | Load        |    33,420,969 |   36.34 |    0 |         0 |
|                                | Mixed       |     9,865,677 |  153.70 |    0 |         0 |
| `snawoot_lfmap`                | Store       |       421,611 | 2540.00 | 7975 |        49 |
|                                | LoadOrStore |     7,585,742 |  218.00 |  494 |         2 |
|                                | Load        |   277,771,539 |    4.24 |    0 |         0 |
|                                | Mixed       |     1,885,026 |  645.00 | 2467 |        10 |




<details>
<summary> Store throughput test (06/05/2025) </summary>

```go

const total = 100_000_000

func testInsert_pb_MapOf(t *testing.T, total int, numCPU int, preSize bool) {

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

| Implementation & Test Case  | Throughput (M ops/sec) | Chart                         |
|-----------------------------|-----------------------:|-------------------------------|
| pb_MapOf (64/pre)           |                 106.67 | █████████████████████████████ |
| xsync_MapV4 (64/pre)        |                  89.48 | ████████████████████████      |
| pb_MapOf (64)               |                  26.56 | ████████▌                     |
| zhangyunhao116_skipmap (64) |                  22.58 | ████████                      |
| xsync_MapV4 (64)            |                  20.36 | ███████                       |
| pb_HashTrieMap (64)         |                  15.53 | ██████                        |
| original_syncMap (64)       |                  13.94 | █████▌                        |
| pb_MapOf (1/pre)            |                   5.51 | ██                            |
| xsync_MapV4 (1/pre)         |                   5.42 | ██                            |
| xsync_MapV4 (1)             |                   4.62 | █▌                            |
| pb_MapOf (1)                |                   4.67 | █▌                            |
| pb_HashTrieMap (1)          |                   1.66 | ▌                             |
| original_syncMap (1)        |                   1.34 | ▎                             |
| alphadose_haxmap (64/pre)   |                   2.43 | █                             |
| alphadose_haxmap (1/pre)    |                   1.00 | ▏                             |
| alphadose_haxmap (64)       |                   0.55 | ▏                             |
| alphadose_haxmap (1)        |                   0.89 | ▏                             |

 - (64/pre) = 64 goroutine with pre-allocation
 - (64) = 64 goroutine without pre-allocation
 - (1/pre) = 1 goroutine with pre-allocation
 - (1) = 1 goroutine without pre-allocation

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
