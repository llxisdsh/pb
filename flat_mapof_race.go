//go:build race

package pb

type FlatMapOf[K comparable, V any] struct {
	MapOf[K, V]
}

func NewFlatMapOf[K comparable, V any](options ...func(*MapConfig)) *FlatMapOf[K, V] {
	return &FlatMapOf[K, V]{MapOf: *NewMapOf[K, V](options...)}
}

type (
	flatRebuildState[K comparable, V any] struct{ _ [CacheLineSize]byte }
	flatTable[K comparable, V any]        struct{ _ [CacheLineSize]byte }
	flatBucket[K comparable, V any]       struct{ _ [CacheLineSize]byte }
)

func (m *FlatMapOf[K, V]) Process(
	key K,
	fn func(old V, loaded bool) (newV V, op ComputeOp, ret V, status bool),
) (V, bool) {
	return m.ProcessEntry(
		key,
		func(loaded *EntryOf[K, V]) (*EntryOf[K, V], V, bool) {
			var old V
			var ok bool
			if loaded != nil {
				old = loaded.Value
				ok = true
			}
			newV, op, ret, status := fn(old, ok)
			switch op {
			case UpdateOp:
				return &EntryOf[K, V]{Value: newV}, ret, status
			case DeleteOp:
				return nil, ret, status
			default:
				return loaded, ret, status
			}
		},
	)
}

func (m *FlatMapOf[K, V]) RangeProcess(
	fn func(key K, value V) (newV V, op ComputeOp),
	policyOpt ...WriterPolicy,
) {
	m.RangeProcessEntry(
		func(loaded *EntryOf[K, V]) *EntryOf[K, V] {
			newV, op := fn(loaded.Key, loaded.Value)
			switch op {
			case UpdateOp:
				return &EntryOf[K, V]{Value: newV}
			case DeleteOp:
				return nil
			default:
				return loaded
			}
		},
		policyOpt...,
	)
}
