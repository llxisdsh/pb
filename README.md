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
achieving the fastest operations for Store (0.6740 ns/op), LoadOrStore (0.5898 ns/op), Load (0.1982 ns/op) 
and Mixed (0.4623 ns/op)

```
goos: windows
goarch: amd64
pkg: github.com/llxisdsh/pb
cpu: AMD Ryzen Threadripper 3970X 32-Core Processor 
```


<details>
<summary> Benchmark test (16/05/2025) </summary>

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
| `original_syncMap`             | Store       |    46,064,870 |   24.64 |   64 |         3 |
|                                | LoadOrStore |    64,949,474 |   18.79 |   17 |         2 |
|                                | Load        |   426,571,569 |    2.83 |    0 |         0 |
|                                | Mixed       |   168,495,805 |    6.96 |   10 |         0 |
| `pb_MapOf` 🏆                  | Store       | 1,000,000,000 |    0.67 |    0 |         0 |
|                                | LoadOrStore | 1,000,000,000 |    0.59 |    0 |         0 |
|                                | Load        | 1,000,000,000 |    0.20 |    0 |         0 |
|                                | Mixed       | 1,000,000,000 |    0.46 |    0 |         0 |
| `xsync_MapV4`                  | Store       |   137,536,370 |    7.66 |   16 |         1 |
|                                | LoadOrStore |   294,872,066 |    3.67 |    0 |         0 |
|                                | Load        |   737,239,308 |    1.61 |    0 |         0 |
|                                | Mixed       |   499,624,864 |    2.18 |    2 |         0 |
| `pb_HashTrieMap`               | Store       |    64,702,512 |   18.21 |   48 |         1 |
|                                | LoadOrStore |    87,431,056 |   11.85 |    1 |         0 |
|                                | Load        |   469,479,710 |    3.79 |    0 |         0 |
|                                | Mixed       |   179,121,828 |    7.62 |    6 |         0 |
| `alphadose_haxmap`             | Store       |    68,260,161 |   15.39 |    9 |         1 |
|                                | LoadOrStore |    73,429,525 |   14.81 |    9 |         1 |
|                                | Load        |   782,623,156 |    1.59 |    0 |         0 |
|                                | Mixed       |   184,823,894 |    6.47 |    2 |         0 |
| `zhangyunhao116_skipmap`       | Store       |    29,525,016 |   38.18 |    9 |         1 |
|                                | LoadOrStore |    36,366,831 |   30.25 |    1 |         0 |
|                                | Load        |   565,815,679 |    2.34 |    0 |         0 |
|                                | Mixed       |   189,834,188 |    6.18 |    1 |         0 |
| `riraccuia_ash`                | Store       |    48,614,984 |   22.39 |   64 |         4 |
|                                | LoadOrStore |    24,301,975 |   41.34 |   94 |         3 |
|                                | Load        |   291,879,900 |    4.65 |    7 |         0 |
|                                | Mixed       |   126,836,432 |   10.19 |   18 |         1 |
| `fufuok_cmap`                  | Store       |    30,101,139 |   38.04 |    1 |         0 |
|                                | LoadOrStore |    45,212,043 |   27.30 |    0 |         0 |
|                                | Load        |   159,030,338 |    7.27 |    0 |         0 |
|                                | Mixed       |    25,381,407 |   45.83 |    0 |         0 |
| `mhmtszr_concurrent_swiss_map` | Store       |    30,092,307 |   40.08 |    1 |         0 |
|                                | LoadOrStore |    26,714,038 |   44.41 |    0 |         0 |
|                                | Load        |   159,262,423 |    7.48 |    0 |         0 |
|                                | Mixed       |    32,010,584 |   35.94 |    0 |         0 |
| `orcaman_concurrent_mapV2`     | Store       |    35,815,976 |   33.40 |    1 |         0 |
|                                | LoadOrStore |    49,316,352 |   25.95 |    1 |         0 |
|                                | Load        |   153,549,606 |    8.36 |    0 |         0 |
|                                | Mixed       |    45,057,391 |   25.40 |    0 |         0 |
| `RWLockShardedMap`             | Store       |    33,360,577 |   33.84 |    1 |         0 |
|                                | LoadOrStore |    47,403,661 |   26.01 |    1 |         0 |
|                                | Load        |   134,923,510 |    8.82 |    0 |         0 |
|                                | Mixed       |    43,701,996 |   26.59 |    0 |         0 |
| `RWLockMap`                    | Store       |     3,882,283 |  309.00 |    1 |         0 |
|                                | LoadOrStore |     9,064,360 |  164.00 |    1 |         0 |
|                                | Load        |    33,687,709 |   36.27 |    0 |         0 |
|                                | Mixed       |     7,556,793 |  156.60 |    0 |         0 |
| `snawoot_lfmap`                | Store       |       404,527 | 2689.00 | 8030 |        50 |
|                                | LoadOrStore |     7,413,974 |  222.80 |  507 |         2 |
|                                | Load        |   281,330,618 |    4.21 |    0 |         0 |
|                                | Mixed       |     1,897,257 |  645.00 | 2513 |        10 |



<details>
<summary> Store throughput test (16/05/2025) </summary>

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

| Implementation & Case       | Throughput<br>(M ops/s) | Performance Scale      |
|-----------------------------|------------------------:|------------------------|
| pb_MapOf (64/pre)           |                  146.41 | ━━━━━━━━━━━━━━━━━━━━━━ |
| xsync_MapV4 (64/pre)        |                   65.00 | ━━━━━━━━━              |
| pb_MapOf (64)               |                   34.88 | ━━━━━━                 |
| zhangyunhao116_skipmap (64) |                   23.40 | ━━━━━                  |
| xsync_MapV4 (64)            |                   22.83 | ━━━━━                  |
| pb_MapOf (1/pre)            |                   20.11 | ━━━━                   |
| pb_HashTrieMap (64)         |                   18.57 | ━━━                    |
| original_syncMap (64)       |                   13.55 | ━━                     |
| pb_MapOf (1)                |                   13.39 | ━━                     |
| xsync_MapV4 (1/pre)         |                    5.80 | ━                      |
| xsync_MapV4 (1)             |                    4.82 | ━                      |
| zhangyunhao116_skipmap (1)  |                    3.56 | ━                      |
| alphadose_haxmap (64/pre)   |                    2.91 | ━                      |
| pb_HashTrieMap (1)          |                    1.74 | ━                      |
| original_syncMap (1)        |                    1.43 | ━                      |
| alphadose_haxmap (1/pre)    |                    1.02 | ━                      |
| alphadose_haxmap (1)        |                    0.94 | ━                      |
| alphadose_haxmap (64)       |                    0.55 | ━                      |

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
