
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
  - Counter Performance Optimization
    For improved counter performance, use `mapof_opt_enablepadding` to force padding around counters, reducing false sharing.
  - Compatibility
    - Already optimized for strong memory models (e.g., x86 TSO, Apple Silicon).
    - Already optimized for Weak memory models.
    - Tested/Validated CPUs with default configurations: 
      AMD Ryzen Threadripper 3970X, ARM Neoverse-N2, Apple M3 Ultra, Qualcomm Snapdragon 636 (32bit)
	- Enable `mapof_opt_atomic_loads` or `mapof_opt_atomic_stores` to strengthen load/store
	  ordering on weakly-ordered architectures (e.g., traditional ARM).
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

Below are my test results, focusing on `pb_MapOf`

```
goos: windows
goarch: amd64
cpu: AMD Ryzen Threadripper 3970X 32-Core Processor
BenchmarkLoad_original_syncMap
BenchmarkLoad_original_syncMap-64                       	1000000000	         1.051 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_original_syncMap
BenchmarkLoadOrStore_original_syncMap-64                	135233187	         8.029 ns/op	      16 B/op	       1 allocs/op
BenchmarkLoad_yyle88_syncmap
BenchmarkLoad_yyle88_syncmap-64                         	1000000000	         1.096 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_yyle88_syncmap
BenchmarkLoadOrStore_yyle88_syncmap-64                  	137798134	         8.077 ns/op	      16 B/op	       1 allocs/op
BenchmarkLoad_pb_HashTrieMap
BenchmarkLoad_pb_HashTrieMap-64                         	1000000000	         0.7279 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_pb_HashTrieMap
BenchmarkLoadOrStore_pb_HashTrieMap-64                  	258239502	         4.074 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrCompute_pb_HashTrieMap
BenchmarkLoadOrCompute_pb_HashTrieMap-64                	306349586	         3.787 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_xsync_MapOf
BenchmarkLoad_xsync_MapOf-64                            	1000000000	         0.3865 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_xsync_MapOf
BenchmarkLoadOrStore_xsync_MapOf-64                     	1000000000	         0.9971 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrCompute_xsync_MapOf
BenchmarkLoadOrCompute_xsync_MapOf-64                   	1000000000	         1.257 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_pb_MapOf
BenchmarkLoad_pb_MapOf-64                               	1000000000	         0.3266 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_pb_MapOf
BenchmarkLoadOrStore_pb_MapOf-64                        	1000000000	         0.7637 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrCompute_pb_MapOf
BenchmarkLoadOrCompute_pb_MapOf-64                      	1000000000	         0.7664 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_alphadose_haxmap
BenchmarkLoad_alphadose_haxmap-64                       	1000000000	         0.4163 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_alphadose_haxmap
BenchmarkLoadOrStore_alphadose_haxmap-64                	269435372	         3.857 ns/op	       8 B/op	       1 allocs/op
BenchmarkLoad_zhangyunhao116_skipmap
BenchmarkLoad_zhangyunhao116_skipmap-64                 	706450005	         1.745 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_zhangyunhao116_skipmap
BenchmarkLoadOrStore_zhangyunhao116_skipmap-64          	286135125	         3.782 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_fufuok_cmap
BenchmarkLoad_fufuok_cmap-64                            	172777294	         7.003 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_fufuok_cmap
BenchmarkLoadOrStore_fufuok_cmap-64                     	53934360	        22.57 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_mhmtszr_concurrent_swiss_map
BenchmarkLoad_mhmtszr_concurrent_swiss_map-64           	147992079	         8.216 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_mhmtszr_concurrent_swiss_map
BenchmarkLoadOrStore_mhmtszr_concurrent_swiss_map-64    	29085973	        36.12 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_easierway_concurrent_map
BenchmarkLoad_easierway_concurrent_map-64               	109092314	        10.93 ns/op	      15 B/op	       1 allocs/op
BenchmarkLoadOrStore_easierway_concurrent_map
BenchmarkLoadOrStore_easierway_concurrent_map-64        	25202724	        41.18 ns/op	      24 B/op	       2 allocs/op
BenchmarkLoad_orcaman_concurrent_map
BenchmarkLoad_orcaman_concurrent_map-64                 	100000000	        10.27 ns/op	       5 B/op	       0 allocs/op
BenchmarkLoadOrStore_orcaman_concurrent_map
BenchmarkLoadOrStore_orcaman_concurrent_map-64          	45814640	        25.66 ns/op	       5 B/op	       0 allocs/op
BenchmarkLoad_RWLockMap
BenchmarkLoad_RWLockMap-64                              	34058785	        34.70 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_RWLockMap
BenchmarkLoadOrStore_RWLockMap-64                       	 8990635	       153.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoad_RWLockShardedMap
BenchmarkLoad_RWLockShardedMap-64                       	100000000	        10.20 ns/op	       0 B/op	       0 allocs/op
BenchmarkLoadOrStore_RWLockShardedMap
BenchmarkLoadOrStore_RWLockShardedMap-64                	36439827	        30.12 ns/op	       0 B/op	       0 allocs/op
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
    m.LoadOrCompute("b", func() int {
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
    for k, v := range m.Range {
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
