//go:build !mapof_opt_atomic_loads

package pb

import (
	"sync/atomic"
	"unsafe"
)

func loadPointer(addr *unsafe.Pointer) unsafe.Pointer {
	return *addr
}

func loadUint64(addr *uint64) uint64 {
	return *addr
}

func atomicLoadPointer(addr *unsafe.Pointer) unsafe.Pointer {
	return atomic.LoadPointer(addr)
}

func atomicLoadUint64(addr *uint64) uint64 {
	return atomic.LoadUint64(addr)
}
