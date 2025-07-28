package pb

import (
	"testing"
)

func BenchmarkHashTrieMapLoadSmall(b *testing.B) {
	benchmarkHashTrieMapLoad(b, testDataSmall[:])
}

func BenchmarkHashTrieMapLoad(b *testing.B) {
	benchmarkHashTrieMapLoad(b, testData[:])
}

func BenchmarkHashTrieMapLoadLarge(b *testing.B) {
	benchmarkHashTrieMapLoad(b, testDataLarge[:])
}

func benchmarkHashTrieMapLoad(b *testing.B, data []string) {
	b.ReportAllocs()
	var m HashTrieMap[string, int]
	m.init()

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

func BenchmarkHashTrieMapLoadOrCompute(b *testing.B) {
	benchmarkHashTrieMapLoadOrCompute(b, testData[:])
}

func BenchmarkHashTrieMapLoadOrComputeLarge(b *testing.B) {
	benchmarkHashTrieMapLoadOrCompute(b, testDataLarge[:])
}

func benchmarkHashTrieMapLoadOrCompute(b *testing.B, data []string) {
	b.ReportAllocs()
	var m HashTrieMap[string, int]
	m.init()
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

func BenchmarkHashTrieMapLoadOrStoreInt(b *testing.B) {
	benchmarkHashTrieMapLoadOrStoreInt(b, testDataInt[:])
}

func BenchmarkHashTrieMapLoadOrStoreIntLarge(b *testing.B) {
	benchmarkHashTrieMapLoadOrStoreInt(b, testDataIntLarge[:])
}

func benchmarkHashTrieMapLoadOrStoreInt(b *testing.B, data []int) {
	b.ReportAllocs()
	var m HashTrieMap[int, int]
	m.init()
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
