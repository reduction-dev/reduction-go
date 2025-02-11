package rxn

import (
	"fmt"

	"reduction.dev/reduction-go/internal"
)

type ValueState[T any] struct {
	name  string
	Value T
	codec ValueStateCodec[T]
}

func (s *ValueState[T]) Load(entries []StateEntry) error {
	// Use a single entry
	var entry StateEntry
	if len(entries) > 0 {
		entry = entries[0]
	}

	if len(entry.Value) == 0 {
		return nil
	}

	value, err := s.codec.DecodeValue(entry.Value)
	if err != nil {
		return fmt.Errorf("failed to decode value: %w", err)
	}
	s.Value = value
	return nil
}

func (s *ValueState[T]) Mutations() ([]StateMutation, error) {
	data, err := s.codec.EncodeValue(s.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}

	return []StateMutation{&PutMutation{
		Key:   []byte(s.Name()),
		Value: data,
	}}, nil
}

func (s *ValueState[T]) Name() string {
	return s.name
}

// NewValueState creates a new ValueState for either ProtoScalar or BinaryValue types
func NewValueState[T any](name string, codec ValueStateCodec[T]) *ValueState[T] {
	return &ValueState[T]{
		name:  name,
		codec: codec,
	}
}

var _ StateItem = (*ValueState[int])(nil)

type ProtoScalar = internal.ProtoScalar

type ScalarCodec[T ProtoScalar] struct{}

func (ScalarCodec[T]) EncodeValue(value T) ([]byte, error) {
	return internal.EncodeScalar(value)
}

func (ScalarCodec[T]) DecodeValue(b []byte) (T, error) {
	return internal.DecodeScalar[T](b)
}
