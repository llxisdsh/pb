//go:build amd64 || arm64 || ppc64 || ppc64le || mips64 || mips64le || riscv64 || s390x || wasm

package pb

// hashPrime is the 64-bit Golden Ratio mixing constant.
const hashPrime = 0x9E3779B185EBCA87
