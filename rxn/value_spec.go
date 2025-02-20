package rxn

import (
	"reduction.dev/reduction-go/internal"
)

type ValueCodec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(b []byte) (T, error)
}

type ValueSpec[T any] interface {
	StateFor(subject *internal.Subject) ValueState[T]
}

type ValueState[T any] interface {
	Value() T
	Set(value T)
	Drop()
}

type ScalarCodec[T internal.ProtoScalar] struct{}

func (ScalarCodec[T]) Encode(value T) ([]byte, error) {
	return internal.EncodeScalar(value)
}

func (ScalarCodec[T]) Decode(b []byte) (T, error) {
	return internal.DecodeScalar[T](b)
}
