package pb

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkFlatMapOf_Load benchmarks read-only operations
func BenchmarkFlatMapOf_Load(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			m := NewFlatMapOf[int, int]()

			// Pre-populate
			for i := 0; i < size; i++ {
				m.Process(
					i,
					func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return i * 2, UpdateOp, i * 2, false
					},
				)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					key := rand.Intn(size)
					m.Load(key)
				}
			})
		})
	}
}

// BenchmarkFlatMapOf_ProcessEntry benchmarks write operations
func BenchmarkFlatMapOf_ProcessEntry(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			m := NewFlatMapOf[int, int]()

			// Pre-populate
			for i := 0; i < size; i++ {
				m.Process(
					i,
					func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return i, UpdateOp, i, false
					},
				)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					key := rand.Intn(size)
					m.Process(
						key,
						func(old int, loaded bool) (int, ComputeOp, int, bool) {
							return old + 1, UpdateOp, old + 1, false
						},
					)
				}
			})
		})
	}
}

// BenchmarkFlatMapOf_MixedWorkload benchmarks mixed read/write workload
func BenchmarkFlatMapOf_MixedWorkload(b *testing.B) {
	readRatios := []int{50, 80, 95} // Percentage of reads

	for _, readRatio := range readRatios {
		b.Run(fmt.Sprintf("read_%d_percent", readRatio), func(b *testing.B) {
			m := NewFlatMapOf[int, int]()
			const size = 10000

			// Pre-populate
			for i := 0; i < size; i++ {
				m.Process(
					i,
					func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return i, UpdateOp, i, false
					},
				)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					key := rand.Intn(size)
					if rand.Intn(100) < readRatio {
						// Read operation
						m.Load(key)
					} else {
						// Write operation
						m.Process(key, func(old int, loaded bool) (int, ComputeOp, int, bool) {
							return old + 1, UpdateOp, old + 1, false
						})
					}
				}
			})
		})
	}
}

// BenchmarkFlatMapOf_HighContention tests performance under high contention
func BenchmarkFlatMapOf_HighContention(b *testing.B) {
	contentionLevels := []int{1, 10, 100} // Number of hot keys

	for _, hotKeys := range contentionLevels {
		b.Run(fmt.Sprintf("hot_keys_%d", hotKeys), func(b *testing.B) {
			m := NewFlatMapOf[int, int]()

			// Pre-populate hot keys
			for i := 0; i < hotKeys; i++ {
				m.Process(
					i,
					func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return i, UpdateOp, i, false
					},
				)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					key := rand.Intn(hotKeys)
					if rand.Intn(2) == 0 {
						m.Load(key)
					} else {
						m.Process(key, func(old int, loaded bool) (int, ComputeOp, int, bool) {
							return old + 1, UpdateOp, old + 1, false
						})
					}
				}
			})
		})
	}
}

// BenchmarkFlatMapOf_Resize benchmarks auto-resize via insertions
func BenchmarkFlatMapOf_Resize(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				m := NewFlatMapOf[int, int](WithPresize(size))

				// Pre-populate up to size
				for j := 0; j < size; j++ {
					m.Process(
						j,
						func(old int, loaded bool) (int, ComputeOp, int, bool) {
							return j, UpdateOp, j, false
						},
					)
				}
				// capture initial table length
				getLen := func() int {
					table := (*flatTable[int, int])(
						atomic.LoadPointer(&m.table),
					)
					if table == nil {
						return 0
					}
					return table.mask + 1
				}
				initialLen := getLen()

				b.StartTimer()
				// Insert new keys until auto-grow happens once
				k := size
				for {
					m.Process(
						k,
						func(old int, loaded bool) (int, ComputeOp, int, bool) {
							return k, UpdateOp, k, false
						},
					)
					k++
					if getLen() > initialLen {
						break
					}
				}
			}
		})
	}
}

// BenchmarkFlatMapOf_Memory measures memory usage
func BenchmarkFlatMapOf_Memory(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)

			m := NewFlatMapOf[int, int]()
			for i := 0; i < size; i++ {
				m.Process(
					i,
					func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return i, UpdateOp, i, false
					},
				)
			}

			runtime.GC()
			runtime.ReadMemStats(&m2)

			allocated := m2.Alloc - m1.Alloc
			b.ReportMetric(float64(allocated)/float64(size), "bytes/entry")
			b.ReportMetric(float64(allocated), "total_bytes")
		})
	}
}

// BenchmarkFlatMapOf_StringKeys tests performance with string keys
func BenchmarkFlatMapOf_StringKeys(b *testing.B) {
	m := NewFlatMapOf[string, int]()
	const size = 10000

	// Generate string keys
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = fmt.Sprintf("key_%d_%d", i, rand.Intn(1000))
		m.Process(
			keys[i],
			func(old int, loaded bool) (int, ComputeOp, int, bool) {
				return i, UpdateOp, i, false
			},
		)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(size)]
			m.Load(key)
		}
	})
}

// BenchmarkFlatMapOf_ConcurrentResize tests auto-resize under concurrent load
func BenchmarkFlatMapOf_ConcurrentResize(b *testing.B) {
	b.Run("concurrent_resize", func(b *testing.B) {
		m := NewFlatMapOf[int, int]()
		const initialSize = 1000

		// Pre-populate
		for i := 0; i < initialSize; i++ {
			m.Process(
				i,
				func(old int, loaded bool) (int, ComputeOp, int, bool) {
					return i, UpdateOp, i, false
				},
			)
		}

		getLen := func() int {
			table := (*flatTable[int, int])(atomic.LoadPointer(&m.table))
			if table == nil {
				return 0
			}
			return table.mask + 1
		}

		b.ResetTimer()

		var wg sync.WaitGroup
		stop := make(chan struct{})

		// Background readers
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						key := rand.Intn(initialSize)
						m.Load(key)
					}
				}
			}()
		}

		// Perform auto-grow triggering writes with timeout protection
		for i := 0; i < b.N; i++ {
			initialLen := getLen()
			k := initialSize + i*initialSize

			// Add timeout protection to prevent infinite loops
			timeout := time.After(time.Second)
			maxInserts := initialSize * 2 // Limit maximum insertions
			insertCount := 0

		insertLoop:
			for {
				select {
				case <-timeout:
					b.Logf("Timeout waiting for resize, breaking out of loop")
					break insertLoop
				default:
					m.Process(k, func(old int, loaded bool) (int, ComputeOp, int, bool) {
						return k, UpdateOp, k, false
					})
					k++
					insertCount++

					// Check if table grew or if we've inserted too many items
					if getLen() > initialLen || insertCount >= maxInserts {
						break insertLoop
					}

					// Give other goroutines a chance to run
					if insertCount%100 == 0 {
						runtime.Gosched()
					}
				}
			}
		}

		close(stop)
		wg.Wait()
	})
}

// BenchmarkFlatMapOf_LargeValues tests performance with larger value types
type LargeValue struct {
	Data [64]byte
	ID   int64
	Name string
}

//
//func BenchmarkFlatMapOf_LargeValues(b *testing.B) {
//	m := NewFlatMapOf[int, LargeValue]()
//	const size = 1000
//
//	// Pre-populate
//	for i := 0; i < size; i++ {
//		m.Process(
//			i,
// 			func(old LargeValue, loaded bool) (LargeValue, ComputeOp, LargeValue,
// bool) {
//				newV := LargeValue{
//					ID:   int64(i),
//					Name: fmt.Sprintf("item_%d", i),
//				}
//				return newV, UpdateOp, newV, false
//			},
//		)
//	}
//
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		for pb.Next() {
//			key := rand.Intn(size)
//			m.Load(key)
//		}
//	})
//}
