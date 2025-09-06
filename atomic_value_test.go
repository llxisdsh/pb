package pb

import (
	"testing"
	"unsafe"
)

// 测试 bool 类型
func TestAtomicValue_Bool(t *testing.T) {
	var av atomicValue[bool]

	// 测试存储和加载 true
	av.Store(true)
	if !av.Load() {
		t.Errorf("Expected true, got %v", av.Load())
	}

	// 测试存储和加载 false
	av.Store(false)
	if av.Load() {
		t.Errorf("Expected false, got %v", av.Load())
	}
}

// 测试 int8 类型
func TestAtomicValue_Int8(t *testing.T) {
	var av atomicValue[int8]

	testValues := []int8{-128, -1, 0, 1, 127}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 uint8 类型
func TestAtomicValue_Uint8(t *testing.T) {
	var av atomicValue[uint8]

	testValues := []uint8{0, 1, 128, 255}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 int16 类型
func TestAtomicValue_Int16(t *testing.T) {
	var av atomicValue[int16]

	testValues := []int16{-32768, -1, 0, 1, 32767}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 uint16 类型
func TestAtomicValue_Uint16(t *testing.T) {
	var av atomicValue[uint16]

	testValues := []uint16{0, 1, 32768, 65535}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 int32 类型
func TestAtomicValue_Int32(t *testing.T) {
	var av atomicValue[int32]

	testValues := []int32{-2147483648, -1, 0, 1, 2147483647}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 uint32 类型
func TestAtomicValue_Uint32(t *testing.T) {
	var av atomicValue[uint32]

	testValues := []uint32{0, 1, 2147483648, 4294967295}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 int64 类型
func TestAtomicValue_Int64(t *testing.T) {
	var av atomicValue[int64]

	testValues := []int64{-9223372036854775808, -1, 0, 1, 9223372036854775807}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 uint64 类型
func TestAtomicValue_Uint64(t *testing.T) {
	var av atomicValue[uint64]

	testValues := []uint64{0, 1, 9223372036854775808, 18446744073709551615}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 float32 类型
func TestAtomicValue_Float32(t *testing.T) {
	var av atomicValue[float32]

	testValues := []float32{-3.14, 0.0, 3.14, 1.23e10}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试 float64 类型
func TestAtomicValue_Float64(t *testing.T) {
	var av atomicValue[float64]

	testValues := []float64{-3.14159265359, 0.0, 3.14159265359, 1.23e100}
	for _, val := range testValues {
		av.Store(val)
		if av.Load() != val {
			t.Errorf("Expected %v, got %v", val, av.Load())
		}
	}
}

// 测试指针类型
func TestAtomicValue_Pointer(t *testing.T) {
	var av atomicValue[*int]

	// 测试 nil 指针
	av.Store(nil)
	if av.Load() != nil {
		t.Errorf("Expected nil, got %v", av.Load())
	}

	// 测试非 nil 指针
	val := 42
	ptr := &val
	av.Store(ptr)
	loadedPtr := av.Load()
	if loadedPtr != ptr {
		t.Errorf("Expected %p, got %p", ptr, loadedPtr)
	}
	if *loadedPtr != 42 {
		t.Errorf("Expected 42, got %v", *loadedPtr)
	}
}

// 测试自定义结构体（小于等于 uint64 大小）
type SmallStruct struct {
	A uint32
	B uint32
}

func TestAtomicValue_SmallStruct(t *testing.T) {
	// 确保结构体大小不超过 uint64
	if unsafe.Sizeof(SmallStruct{}) > unsafe.Sizeof(uint64(0)) {
		t.Skipf("SmallStruct size %d exceeds uint64 size %d",
			unsafe.Sizeof(SmallStruct{}), unsafe.Sizeof(uint64(0)))
	}

	var av atomicValue[SmallStruct]

	testValues := []SmallStruct{
		{A: 0, B: 0},
		{A: 1, B: 2},
		{A: 4294967295, B: 4294967295},
	}

	for _, val := range testValues {
		av.Store(val)
		loaded := av.Load()
		if loaded.A != val.A || loaded.B != val.B {
			t.Errorf("Expected %+v, got %+v", val, loaded)
		}
	}
}

// 测试类型大小验证
func TestTypeSizes(t *testing.T) {
	tests := []struct {
		name string
		size uintptr
	}{
		{"bool", unsafe.Sizeof(bool(false))},
		{"int8", unsafe.Sizeof(int8(0))},
		{"uint8", unsafe.Sizeof(uint8(0))},
		{"int16", unsafe.Sizeof(int16(0))},
		{"uint16", unsafe.Sizeof(uint16(0))},
		{"int32", unsafe.Sizeof(int32(0))},
		{"uint32", unsafe.Sizeof(uint32(0))},
		{"int64", unsafe.Sizeof(int64(0))},
		{"uint64", unsafe.Sizeof(uint64(0))},
		{"float32", unsafe.Sizeof(float32(0))},
		{"float64", unsafe.Sizeof(float64(0))},
		{"*int", unsafe.Sizeof((*int)(nil))},
		{"SmallStruct", unsafe.Sizeof(SmallStruct{})},
	}

	uint64Size := unsafe.Sizeof(uint64(0))
	t.Logf("uint64 size: %d bytes", uint64Size)

	for _, test := range tests {
		t.Logf("%s size: %d bytes", test.name, test.size)
		if test.size > uint64Size {
			t.Errorf("Type %s size %d exceeds uint64 size %d",
				test.name, test.size, uint64Size)
		}
	}
}
func TestAtomicValue_Sizes(t *testing.T) {
	tests := []struct {
		name string
		size uintptr
	}{
		{"atomicValue[bool]", unsafe.Sizeof(*new(atomicValue[bool]))},
		{"atomicValue[int8]", unsafe.Sizeof(*new(atomicValue[int8]))},
		{"atomicValue[uint8]", unsafe.Sizeof(*new(atomicValue[uint8]))},
		{"atomicValue[int16]", unsafe.Sizeof(*new(atomicValue[int16]))},
		{"atomicValue[uint16]", unsafe.Sizeof(*new(atomicValue[uint16]))},
		{"atomicValue[int32]", unsafe.Sizeof(*new(atomicValue[int32]))},
		{"atomicValue[uint32]", unsafe.Sizeof(*new(atomicValue[uint32]))},
		{"atomicValue[int64]", unsafe.Sizeof(*new(atomicValue[int64]))},
		{"atomicValue[uint64]", unsafe.Sizeof(*new(atomicValue[uint64]))},
		{"atomicValue[float32]", unsafe.Sizeof(*new(atomicValue[float32]))},
		{"atomicValue[float64]", unsafe.Sizeof(*new(atomicValue[float64]))},
		{"atomicValue[*int]", unsafe.Sizeof(*new(atomicValue[*int]))},
		{"atomicValue[SmallStruct]", unsafe.Sizeof(*new(atomicValue[SmallStruct]))},
	}

	uint64Size := unsafe.Sizeof(uint64(0))
	t.Logf("uint64 size: %d bytes", uint64Size)

	for _, test := range tests {
		t.Logf("%s size: %d bytes", test.name, test.size)
		if test.size > uint64Size {
			t.Errorf("Type %s size %d exceeds uint64 size %d",
				test.name, test.size, uint64Size)
		}
	}
}

// 并发测试
func TestAtomicValue_Concurrent(t *testing.T) {
	var av atomicValue[int64]

	// 启动多个 goroutine 进行并发读写
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(val int64) {
			for j := 0; j < 1000; j++ {
				av.Store(val)
				loaded := av.Load()
				// 验证加载的值是某个有效值（可能不是刚存储的值，因为并发）
				if loaded < 0 || loaded >= 10 {
					t.Errorf("Loaded invalid value: %v", loaded)
					return
				}
			}
			done <- true
		}(int64(i))
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}
}
