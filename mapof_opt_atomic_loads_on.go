//go:build mapof_opt_atomic_loads

package pb

import (
	"sync/atomic"
	"unsafe"
)

func loadPointer(addr *unsafe.Pointer) unsafe.Pointer {
	return atomic.LoadPointer(addr)
}

func loadUint64(addr *uint64) uint64 {
	return atomic.LoadUint64(addr)
}

func loadPointerStrict(addr *unsafe.Pointer) unsafe.Pointer {
	return atomic.LoadPointer(addr)
}

func loadUint64Strict(addr *uint64) uint64 {
	return atomic.LoadUint64(addr)
}
