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
achieving the fastest operations for Store (1.673 ns/op), LoadOrStore (1.651 ns/op), and Load (0.8714 ns/op)

```
goos: windows
goarch: amd64
pkg: github.com/llxisdsh/pb
cpu: AMD Ryzen Threadripper 3970X 32-Core Processor 
```
| Implementation           | Operation   |       Ops/sec |   ns/op | B/op | Allocs/op |
|--------------------------|-------------|--------------:|--------:|-----:|----------:|
| original_syncMap         | Store       |    45,891,687 |   25.53 |   64 |         3 |
|                         | LoadOrStore |    57,935,468 |   19.67 |   17 |         2 |
|                         | Load        |   381,669,920 |    4.20 |    0 |         0 |
| pb_HashTrieMap           | Store       |    62,957,955 |   18.00 |   48 |         1 |
|                         | LoadOrStore |    82,953,130 |   12.17 |    1 |         0 |
|                         | Load        |   433,199,233 |    3.42 |    0 |         0 |
| xsync_MapOf              | Store       |   107,316,170 |   10.22 |   16 |         1 |
|                         | LoadOrStore |   136,323,968 |    7.63 |    0 |         0 |
|                         | Load        |   722,827,120 |    1.68 |    0 |         0 |
| pb_MapOf                 | Store       |   632,699,145 |    1.67 |    0 |         0 |
|                         | LoadOrStore |   664,129,014 |    1.65 |    0 |         0 |
|                         | Load        | 1,000,000,000 |    0.87 |    0 |         0 |
| alphadose_haxmap         | Store       |    81,804,051 |   13.28 |    9 |         1 |
|                         | LoadOrStore |    89,922,666 |   12.98 |    8 |         1 |
|                         | Load        |   774,015,774 |    2.37 |    0 |         0 |
| zhangyunhao116_skipmap   | Store       |    28,586,810 |   38.81 |    9 |         1 |
|                         | LoadOrStore |    35,641,729 |   30.43 |    1 |         0 |
|                         | Load        |   557,247,420 |    2.56 |    0 |         0 |
| riraccuia_ash            | Store       |    49,511,977 |   21.44 |   63 |         4 |
|                         | LoadOrStore |    28,693,774 |   41.53 |   93 |         3 |
|                         | Load        |   280,137,154 |    4.87 |    7 |         0 |
| fufuok_cmap              | Store       |    29,450,960 |   37.94 |    1 |         0 |
|                         | LoadOrStore |    43,640,012 |   28.23 |    0 |         0 |
|                         | Load        |   152,759,056 |    8.61 |    0 |         0 |
| mhmtszr_swiss_map        | Store       |    28,391,749 |   39.85 |    0 |         0 |
|                         | LoadOrStore |    26,447,445 |   45.81 |    0 |         0 |
|                         | Load        |   142,755,394 |    9.76 |    0 |         0 |
| easierway_concurrent_map | Store       |    26,942,739 |   44.64 |   27 |         2 |
|                         | LoadOrStore |    21,814,651 |   52.80 |   18 |         2 |
|                         | Load        |   100,000,000 |   14.10 |   15 |         1 |
| orcaman_concurrent_map   | Store       |    28,951,729 |   42.02 |    9 |         0 |
|                         | LoadOrStore |    44,986,953 |   30.47 |    8 |         0 |
|                         | Load        |   100,000,000 |   10.61 |    7 |         0 |
| snawoot_lfmap            | Store       |       459,609 | 2468.00 | 8106 |        50 |
|                         | LoadOrStore |     8,150,860 |  230.70 |  498 |         2 |
|                         | Load        |   276,581,752 |    4.52 |    0 |         0 |
| RWLockMap                | Store       |     4,273,980 |  276.90 |    1 |         0 |
|                         | LoadOrStore |     9,315,150 |  169.20 |    1 |         0 |
|                         | Load        |    32,947,572 |   36.09 |    0 |         0 |
| RWLockShardedMap         | Store       |    34,804,095 |   34.50 |    1 |         0 |
|                         | LoadOrStore |    48,219,494 |   26.34 |    1 |         0 |
|                         | Load        |   100,000,000 |   10.31 |    0 |         0 |

Benchmarks code referenced from:

```go
const MapElementCount = 1_000_000

func BenchmarkLoad_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	for i := 0; i < MapElementCount; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= MapElementCount {
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
			if i >= MapElementCount {
				i = 0
			}
		}
	})
}
```

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
