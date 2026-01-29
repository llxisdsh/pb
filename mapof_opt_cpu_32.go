//go:build !amd64 && !arm64 && !ppc64 && !ppc64le && !mips64 && !mips64le && !riscv64 && !s390x && !wasm

package pb

// hashPrime is the 32-bit Golden Ratio mixing constant.
// 0x9E3779B9 = floor(2^32 / φ), where φ is the golden ratio.
const hashPrime = 0x9E3779B9
