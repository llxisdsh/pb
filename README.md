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
- Includes rich functional extensions such as LoadOrCompute, ProcessEntry, Size, IsZero,
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

Below are my test results, in the benchmark tests for `Load`, `LoadOrStore`, and `Store` methods, `pb.MapOf` is always the fastest.

```
goos: windows
goarch: amd64
cpu: AMD Ryzen Threadripper 3970X 32-Core Processor 
BenchmarkLoad_original_syncMap
BenchmarkLoad_original_syncMap-64                       	1000000000	         1.115 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_original_syncMap
BenchmarkLoadOrStore_original_syncMap-64                	109891101	        10.91 ns/op	      16 B/op	       1 allocs/op
BenchmarkStore_original_syncMap
BenchmarkStore_original_syncMap-64                      	40714401	        28.73 ns/op	      64 B/op	       2 allocs/op
BenchmarkLoad_pb_HashTrieMap
BenchmarkLoad_pb_HashTrieMap-64                         	1000000000	         0.7153 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_pb_HashTrieMap
BenchmarkLoadOrStore_pb_HashTrieMap-64                  	289147808	         3.954 ns/op	       0 B/op	       0 allocs/op
BenchmarkStore_pb_HashTrieMap
BenchmarkStore_pb_HashTrieMap-64                        	60955131	        19.44 ns/op	      48 B/op	       1 allocs/op
BenchmarkLoad_xsync_MapOf
BenchmarkLoad_xsync_MapOf-64                            	1000000000	         0.3870 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_xsync_MapOf
BenchmarkLoadOrStore_xsync_MapOf-64                     	1000000000	         1.111 ns/op	       0 B/op	       0 allocs/op
BenchmarkStore_xsync_MapOf
BenchmarkStore_xsync_MapOf-64                           	136627312	         8.624 ns/op	      16 B/op	       1 allocs/op
BenchmarkLoad_pb_MapOf
BenchmarkLoad_pb_MapOf-64                               	1000000000	         0.3026 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_pb_MapOf
BenchmarkLoadOrStore_pb_MapOf-64                        	1000000000	         0.6842 ns/op	       0 B/op	       0 allocs/op
BenchmarkStore_pb_MapOf
BenchmarkStore_pb_MapOf-64                              	288284617	         3.729 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_alphadose_haxmap
BenchmarkLoad_alphadose_haxmap-64                       	1000000000	         0.4076 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_alphadose_haxmap
BenchmarkLoadOrStore_alphadose_haxmap-64                	274286868	         4.008 ns/op	       8 B/op	       1 allocs/op
BenchmarkStore_alphadose_haxmap
BenchmarkStore_alphadose_haxmap-64                      	149792583	         7.658 ns/op	       8 B/op	       1 allocs/op
BenchmarkLoad_zhangyunhao116_skipmap
BenchmarkLoad_zhangyunhao116_skipmap-64                 	677391657	         1.656 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_zhangyunhao116_skipmap
BenchmarkLoadOrStore_zhangyunhao116_skipmap-64          	280333525	         3.727 ns/op	       0 B/op	       0 allocs/op
BenchmarkStore_zhangyunhao116_skipmap
BenchmarkStore_zhangyunhao116_skipmap-64                	99588476	        10.88 ns/op	       8 B/op	       1 allocs/op
BenchmarkLoad_fufuok_cmap
BenchmarkLoad_fufuok_cmap-64                            	168652123	         7.096 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_fufuok_cmap
BenchmarkLoadOrStore_fufuok_cmap-64                     	44925816	        22.60 ns/op	       0 B/op	       0 allocs/op
BenchmarkStore_fufuok_cmap
BenchmarkStore_fufuok_cmap-64                           	31872086	        33.80 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_mhmtszr_concurrent_swiss_map
BenchmarkLoad_mhmtszr_concurrent_swiss_map-64           	148250055	         8.037 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_mhmtszr_concurrent_swiss_map
BenchmarkLoadOrStore_mhmtszr_concurrent_swiss_map-64    	99853716	        10.81 ns/op	       0 B/op	       0 allocs/op
BenchmarkStore_mhmtszr_concurrent_swiss_map
BenchmarkStore_mhmtszr_concurrent_swiss_map-64          	29640973	        35.48 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_easierway_concurrent_map
BenchmarkLoad_easierway_concurrent_map-64               	108911221	        13.78 ns/op	      15 B/op	       1 allocs/op
BenchmarkLoadOrStore_easierway_concurrent_map
BenchmarkLoadOrStore_easierway_concurrent_map-64        	65951138	        17.64 ns/op	      16 B/op	       2 allocs/op
BenchmarkStore_easierway_concurrent_map
BenchmarkStore_easierway_concurrent_map-64              	31173343	        36.01 ns/op	      24 B/op	       2 allocs/op
BenchmarkLoad_orcaman_concurrent_map
BenchmarkLoad_orcaman_concurrent_map-64                 	126582919	         9.576 ns/op	       5 B/op	       0 allocs/op
BenchmarkLoadOrStore_orcaman_concurrent_map
BenchmarkLoadOrStore_orcaman_concurrent_map-64          	42145189	        25.01 ns/op	       5 B/op	       0 allocs/op
BenchmarkStore_orcaman_concurrent_map
BenchmarkStore_orcaman_concurrent_map-64                	31977316	        35.69 ns/op	       5 B/op	       0 allocs/op
BenchmarkLoad_RWLockMap
BenchmarkLoad_RWLockMap-64                              	34047672	        34.71 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_RWLockMap
BenchmarkLoadOrStore_RWLockMap-64                       	 9410553	       148.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkStore_RWLockMap
BenchmarkStore_RWLockMap-64                             	 4871845	       260.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_RWLockShardedMap
BenchmarkLoad_RWLockShardedMap-64                       	121969340	         9.902 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_RWLockShardedMap
BenchmarkLoadOrStore_RWLockShardedMap-64                	48799134	        29.82 ns/op	       0 B/op	       0 allocs/op
BenchmarkStore_RWLockShardedMap
BenchmarkStore_RWLockShardedMap-64                      	31306334	        38.76 ns/op	       0 B/op	       0 allocs/op
PASS
```

Benchmarks code referenced from:

```go
const MapElementCount = 100000

func BenchmarkLoad_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	for i := 0; i < MapElementCount; i++ {
		m.Store(i, i)
	}
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

- Supports lazy value generation with LoadOrCompute
- Faster than the built-in HashTrieMap by more than 50%
- All tests passed

The optimization for the built-in HashTrieMap is:

- Remove inited field, use root instead
- Lazy initialization, only init on write, and use the resulting root for subsequent logic to reduce atomic calls

## License

Licensed under MIT.
