package rxn

import (
	"iter"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
)

type MapSpec[K comparable, T any] interface {
	StateFor(subject *internal.Subject) MapState[K, T]
}

type MapState[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Delete(key K)
	All() iter.Seq2[K, V]
	Size() int
}

type MapCodec[K comparable, T any] = states.MapStateCodec[K, T]
