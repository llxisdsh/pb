package pb

import "unsafe"

// NewMapOfWithHasher creates a MapOf with custom hash and equality functions.
//
// Parameters:
//   - keyHash: custom key hashing function (nil = use built-in hasher)
//   - valEqual: custom value equality function (nil = use built-in comparison)
//   - options: configuration options (WithPresize, WithShrinkEnabled, etc.)
//
// Notes:
//   - Using Compare* methods with non-comparable value types will panic
//     if valEqual is nil.
//
// Deprecated: Use NewMapOf with WithKeyHasher and WithValueEqual instead.
//
//goland:noinspection ALL
func NewMapOfWithHasher[K comparable, V any](
	keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig),
) *MapOf[K, V] {
	m := &MapOf[K, V]{}
	m.InitWithOptions(
		append(
			options,
			WithKeyHasher(keyHash),
			WithValueEqual(valEqual),
		)...,
	)
	return m
}

// NewMapOfWithHasherUnsafe provides functionality similar to
// NewMapOfWithHasher,
// but uses unsafe versions of keyHash and valEqual.
// The following example uses an unbalanced and unsafe version:
//
//	 m := NewMapOfWithHasherUnsafe[int, int](
//		func(ptr unsafe.Pointer, _ uintptr) uintptr {
//			return *(*uintptr)(ptr)
//		}, nil)
//
// Deprecated: Use NewMapOf with WithKeyHasherUnsafe and WithValueEqualUnsafe
// instead.
//
//goland:noinspection ALL
func NewMapOfWithHasherUnsafe[K comparable, V any](
	keyHash func(ptr unsafe.Pointer, seed uintptr) uintptr,
	valEqual func(ptr unsafe.Pointer, ptr2 unsafe.Pointer) bool,
	options ...func(*MapConfig),
) *MapOf[K, V] {
	m := &MapOf[K, V]{}
	m.InitWithOptions(
		append(
			options,
			WithKeyHasherUnsafe(keyHash),
			WithValueEqualUnsafe(valEqual),
		)...,
	)
	return m
}

// WithGrowOnly configures the map to be grow-only.
//
// Deprecated: This function is obsolete as grow-only is now the default
// behavior. Use WithShrinkEnabled() explicitly if automatic shrinking is
// needed.
//
//goland:noinspection ALL
func WithGrowOnly() func(*MapConfig) {
	return func(*MapConfig) {
	}
}

// Init the MapOf, Allows custom key hasher (keyHash)
// and value equality (valEqual) functions for compare-and-swap operations
//
// Parameters:
//   - keyHash: nil uses the built-in hasher
//   - valEqual: nil uses the built-in comparison, but if the value is not of a
//     comparable type, using the Compare series of functions will cause a panic
//   - options: configuration options (WithPresize, WithShrinkEnabled, etc.)
//
// Notes:
//   - This function is not thread-safe and can only be used before
//     the MapOf is utilized.
//   - If this function is not called, MapOf will use the default configuration.
//
// Deprecated: Use InitWithOptions with WithKeyHasher and WithValueEqual
// instead.
//
//goland:noinspection ALL
func (m *MapOf[K, V]) Init(
	keyHash func(key K, seed uintptr) uintptr,
	valEqual func(val, val2 V) bool,
	options ...func(*MapConfig),
) {
	m.InitWithOptions(
		append(
			options,
			WithKeyHasher(keyHash),
			WithValueEqual(valEqual),
		)...,
	)
}
