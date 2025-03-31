//go:build !mapof_opt_cachelinesize_32 && !mapof_opt_cachelinesize_64 && !mapof_opt_cachelinesize_128 && !mapof_opt_cachelinesize_256

package pb

import (
	"golang.org/x/sys/cpu"
	"unsafe"
)

// CacheLineSize is used in structure padding to prevent false sharing.
// It's automatically calculated using the `golang.org/x/sys/cpu` package.
const CacheLineSize = unsafe.Sizeof(cpu.CacheLinePad{})
