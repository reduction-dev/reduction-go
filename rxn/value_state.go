package rxn

import (
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
)

// ValueState provides access to a single value of type T
type ValueState[T any] struct {
	internal *states.ValueState[T]
}

// Value returns the current value stored in the state
func (s *ValueState[T]) Value() T {
	return s.internal.Value()
}

// Set updates the value stored in the state
func (s *ValueState[T]) Set(value T) {
	s.internal.Set(value)
}

// Drop removes the value from the state
func (s *ValueState[T]) Drop() {
	s.internal.Drop()
}

// NewValueState creates a new ValueState instance with the given name and codec
func NewValueState[T any](name string, codec ValueCodec[T]) *ValueState[T] {
	return &ValueState[T]{
		internal: states.NewValueState(name, codec),
	}
}

type ScalarCodec[T internal.ProtoScalar] struct{}

func (ScalarCodec[T]) Encode(value T) ([]byte, error) {
	return internal.EncodeScalar(value)
}

func (ScalarCodec[T]) Decode(b []byte) (T, error) {
	return internal.DecodeScalar[T](b)
}
