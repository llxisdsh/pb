package pb

import (
	"testing"
)

func BenchmarkMapOfLoadSmall(b *testing.B) {
	benchmarkMapOfLoad(b, testDataSmall[:])
}

func BenchmarkMapOfLoad(b *testing.B) {
	benchmarkMapOfLoad(b, testData[:])
}

func BenchmarkMapOfLoadLarge(b *testing.B) {
	benchmarkMapOfLoad(b, testDataLarge[:])
}

func benchmarkMapOfLoad(b *testing.B, data []string) {
	b.ReportAllocs()
	var m MapOf[string, int]
	for i := range data {
		m.LoadOrStore(data[i], i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(data[i])
			i++
			if i >= len(data) {
				i = 0
			}
		}
	})
}

func BenchmarkMapOfLoadOrStore(b *testing.B) {
	benchmarkMapOfLoadOrStore(b, testData[:])
}
func BenchmarkMapOfLoadOrStoreLarge(b *testing.B) {
	benchmarkMapOfLoadOrStore(b, testDataLarge[:])
}

func benchmarkMapOfLoadOrStore(b *testing.B, data []string) {
	b.ReportAllocs()
	var m MapOf[string, int]
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(data[i], i)
			i++
			if i >= len(data) {
				i = 0
			}
		}
	})
}

func BenchmarkMapOfLoadOrStoreFn(b *testing.B) {
	benchmarkMapOfLoadOrStoreFn(b, testData[:])
}
func BenchmarkMapOfLoadOrStoreFnLarge(b *testing.B) {
	benchmarkMapOfLoadOrStoreFn(b, testDataLarge[:])
}
func benchmarkMapOfLoadOrStoreFn(b *testing.B, data []string) {
	b.ReportAllocs()
	var m MapOf[string, int]
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrCompute(data[i], func() int {
				return i
			})
			i++
			if i >= len(data) {
				i = 0
			}
		}
	})
}

func BenchmarkMapOfStore(b *testing.B) {
	benchmarkMapOfStore(b, testData[:])
}
func BenchmarkMapOfStoreLarge(b *testing.B) {
	benchmarkMapOfStore(b, testDataLarge[:])
}
func benchmarkMapOfStore(b *testing.B, data []string) {
	b.ReportAllocs()
	var m MapOf[string, int]
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(data[i], i)
			i++
			if i >= len(data) {
				i = 0
			}
		}
	})
}
func BenchmarkMapOfLoadOrStoreInt(b *testing.B) {
	benchmarkMapOfLoadOrStoreInt(b, testDataInt[:])
}
func BenchmarkMapOfLoadOrStoreIntLarge(b *testing.B) {
	benchmarkMapOfLoadOrStoreInt(b, testDataIntLarge[:])
}
func benchmarkMapOfLoadOrStoreInt(b *testing.B, data []int) {
	b.ReportAllocs()
	var m MapOf[int, int]
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(data[i], i)
			i++
			if i >= len(data) {
				i = 0
			}
		}
	})
}
