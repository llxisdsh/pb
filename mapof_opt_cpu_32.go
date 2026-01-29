//go:build !64bit

package pb

// hashPrime is the 32-bit Golden Ratio mixing constant.
// 0x9E3779B9 = floor(2^32 / φ), where φ is the golden ratio.
const hashPrime = 0x9E3779B9
