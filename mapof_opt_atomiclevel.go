//go:build !mapof_opt_atomiclevel_0 && !mapof_opt_atomiclevel_1 && !mapof_opt_atomiclevel_2

package pb

// atomicLevel:
//   - 0: Both reads and writes are atomic.
//   - 1: Reads are non-atomic, writes are atomic.
//   - 2: Neither reads nor writes are atomic (requires a strong memory model).
const atomicLevel = 0
