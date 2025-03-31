//go:build mapof_opt_atomic_stores

package pb

import (
	"sync/atomic"
	"unsafe"
)

func storePointer(addr *unsafe.Pointer, val unsafe.Pointer) {
	atomic.StorePointer(addr, val)
}

func storeUint64(addr *uint64, val uint64) {
	atomic.StoreUint64(addr, val)
}

func atomicStorePointer(addr *unsafe.Pointer, val unsafe.Pointer) {
	atomic.StorePointer(addr, val)
}

func atomicStoreUint64(addr *uint64, val uint64) {
	atomic.StoreUint64(addr, val)
}
