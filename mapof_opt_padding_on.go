//go:build mapof_opt_enablepadding || (!mapof_opt_disablepadding && (arm64 || loong64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x))

package pb

const padding_ = 1
