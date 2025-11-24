//go:build !race

package pb

import (
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// Detect TSO architectures; on TSO, plain reads/writes are safe for
// pointers and native word-sized integers
const isTSO = runtime.GOARCH == "amd64" ||
	runtime.GOARCH == "386" ||
	runtime.GOARCH == "s390x"

// TSO: plain pointer load; non-TSO: use atomic.LoadPointer
//
//go:nosplit
func loadPtr(addr *unsafe.Pointer) unsafe.Pointer {
	//goland:noinspection ALL
	if isTSO {
		return *addr
	} else {
		return atomic.LoadPointer(addr)
	}
}

// TSO: plain pointer store; non-TSO: use atomic.StorePointer
//
//go:nosplit
func storePtr(addr *unsafe.Pointer, val unsafe.Pointer) {
	//goland:noinspection ALL
	if isTSO {
		*addr = val
	} else {
		atomic.StorePointer(addr, val)
	}
}

// Aligned integer load; plain on TSO when width matches, otherwise atomic
//
//go:nosplit
func loadInt[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(uint32(0)) {
		//goland:noinspection ALL
		if isTSO {
			return *addr
		} else {
			return T(atomic.LoadUint32((*uint32)(unsafe.Pointer(addr))))
		}
	} else {
		//goland:noinspection ALL
		if isTSO && bits.UintSize >= 64 {
			return *addr
		} else {
			return T(atomic.LoadUint64((*uint64)(unsafe.Pointer(addr))))
		}
	}
}

// Aligned integer store; plain on TSO when width matches, otherwise atomic
//
//go:nosplit
func storeInt[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	if unsafe.Sizeof(T(0)) == unsafe.Sizeof(uint32(0)) {
		//goland:noinspection ALL
		if isTSO {
			*addr = val
		} else {
			atomic.StoreUint32((*uint32)(unsafe.Pointer(addr)), uint32(val))
		}
	} else {
		//goland:noinspection ALL
		if isTSO && bits.UintSize >= 64 {
			*addr = val
		} else {
			atomic.StoreUint64((*uint64)(unsafe.Pointer(addr)), uint64(val))
		}
	}
}

// Lock-held read path; avoid unnecessary atomic load overhead
//
//go:nosplit
func loadIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T) T {
	return *addr
}

// Write to unpublished memory; atomic store not needed in thread-private phase
//
//go:nosplit
func storeIntFast[T ~uint32 | ~uint64 | ~uintptr](addr *T, val T) {
	*addr = val
}
