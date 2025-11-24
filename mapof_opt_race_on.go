//go:build race

package pb

import (
	"sync/atomic"
	"unsafe"
)

// Under race detector, disable TSO optimizations and use conservative
// atomic loads/stores
const isTSO = false

// Conservative: atomic pointer load to satisfy race detector
//
//go:nosplit
func loadPtr(addr *unsafe.Pointer) unsafe.Pointer {
	return atomic.LoadPointer(addr)
}

// Conservative: atomic pointer store to satisfy race detector
//
//go:nosplit
func storePtr(addr *unsafe.Pointer, val unsafe.Pointer) {
	atomic.StorePointer(addr, val)
}

// Conservative: atomic integer load to satisfy race detector
//
//go:nosplit
func loadInt[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(uint32(0)) {
		return T(atomic.LoadUint32((*uint32)(unsafe.Pointer(addr))))
	} else {
		return T(atomic.LoadUint64((*uint64)(unsafe.Pointer(addr))))
	}
}

// Conservative: atomic integer store to satisfy race detector
//
//go:nosplit
func storeInt[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(uint32(0)) {
		atomic.StoreUint32((*uint32)(unsafe.Pointer(addr)), uint32(val))
	} else {
		atomic.StoreUint64((*uint64)(unsafe.Pointer(addr)), uint64(val))
	}
}

// Under race, fast load delegates to atomic load for consistency
//
//go:nosplit
func loadIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	return loadInt(addr)
}

// Under race, fast store delegates to atomic store for consistency
//
//go:nosplit
func storeIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	storeInt(addr, val)
}
