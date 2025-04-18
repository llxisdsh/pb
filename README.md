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
achieving the fastest operations for Store (2.489 ns/op), LoadOrStore (2.137 ns/op), and Load (0.8733 ns/op)

```
goos: windows
goarch: amd64
pkg: github.com/llxisdsh/pb
cpu: AMD Ryzen Threadripper 3970X 32-Core Processor 
```

| Implementation                  | Operation       | Ops/sec      | ns/op   | B/op | Allocs/op |
|----------------------------------|-----------------|-------------:|--------:|-----:|----------:|
| sync.Map                         | Store           | 45,115,838   | 25.13   | 64   | 3         |
| sync.Map                         | LoadOrStore     | 53,157,558   | 18.93   | 17   | 2         |
| sync.Map                         | Load            | 414,168,854  | 3.915   | 0    | 0         |
| pb.HashTrieMap                   | Store           | 62,276,613   | 18.90   | 48   | 1         |
| pb.HashTrieMap                   | LoadOrStore     | 88,600,117   | 12.24   | 1    | 0         |
| pb.HashTrieMap                   | Load            | 459,646,837  | 2.980   | 0    | 0         |
| xsync.MapOf                      | Store           | 105,317,431  | 10.18   | 16   | 1         |
| xsync.MapOf                      | LoadOrStore     | 144,292,809  | 7.172   | 0    | 0         |
| xsync.MapOf                      | Load            | 720,721,586  | 1.627   | 0    | 0         |
| pb.MapOf                         | Store           | 455,289,295  | 2.489   | 0    | 0         |
| pb.MapOf                         | LoadOrStore     | 508,923,932  | 2.137   | 0    | 0         |
| pb.MapOf                         | Load            | 1,000,000,000| 0.8733  | 0    | 0         |
| alphadose/haxmap                 | Store           | 84,333,030   | 14.53   | 8    | 1         |
| alphadose/haxmap                 | LoadOrStore     | 82,503,711   | 13.00   | 9    | 1         |
| alphadose/haxmap                 | Load            | 787,015,812  | 1.946   | 0    | 0         |
| zhangyunhao116/skipmap           | Store           | 28,972,909   | 38.62   | 9    | 1         |
| zhangyunhao116/skipmap           | LoadOrStore     | 36,957,987   | 30.66   | 1    | 0         |
| zhangyunhao116/skipmap           | Load            | 527,618,869  | 2.306   | 0    | 0         |
| riraccuia/ash                    | Store           | 45,966,105   | 21.96   | 63   | 4         |
| riraccuia/ash                    | LoadOrStore     | 29,512,130   | 41.47   | 94   | 3         |
| riraccuia/ash                    | Load            | 270,116,048  | 5.059   | 7    | 0         |
| fufuok/cmap                      | Store           | 27,902,758   | 39.85   | 0    | 0         |
| fufuok/cmap                      | LoadOrStore     | 44,959,647   | 28.03   | 0    | 0         |
| fufuok/cmap                      | Load            | 161,841,147  | 7.608   | 0    | 0         |
| mhmtszr/concurrent_swiss_map     | Store           | 26,874,194   | 39.50   | 0    | 0         |
| mhmtszr/concurrent_swiss_map     | LoadOrStore     | 25,488,638   | 45.19   | 0    | 0         |
| mhmtszr/concurrent_swiss_map     | Load            | 129,942,864  | 8.868   | 0    | 0         |
| easierway/concurrent_map         | Store           | 27,365,209   | 43.84   | 27   | 2         |
| easierway/concurrent_map         | LoadOrStore     | 22,033,428   | 53.00   | 18   | 2         |
| easierway/concurrent_map         | Load            | 100,000,000  | 13.77   | 15   | 1         |
| orcaman/concurrent_map           | Store           | 30,959,991   | 42.23   | 9    | 0         |
| orcaman/concurrent_map           | LoadOrStore     | 43,439,228   | 31.51   | 8    | 0         |
| orcaman/concurrent_map           | Load            | 100,000,000  | 10.12   | 7    | 0         |
| snawoot/lfmap                    | Store           | 435,790      | 2604    | 8166 | 51        |
| snawoot/lfmap                    | LoadOrStore     | 8,361,651    | 213.8   | 518  | 2         |
| snawoot/lfmap                    | Load            | 282,367,756  | 4.213   | 0    | 0         |
| RWLockMap                        | Store           | 4,504,072    | 256.6   | 1    | 0         |
| RWLockMap                        | LoadOrStore     | 9,235,543    | 164.4   | 1    | 0         |
| RWLockMap                        | Load            | 33,700,858   | 35.46   | 0    | 0         |
| RWLockShardedMap                 | Store           | 34,991,032   | 34.66   | 1    | 0         |
| RWLockShardedMap                 | LoadOrStore     | 48,624,336   | 26.68   | 1    | 0         |
| RWLockShardedMap                 | Load            | 121,242,320  | 10.11   | 0    | 0         |
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
