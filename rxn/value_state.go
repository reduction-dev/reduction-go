package rxn

import (
	"fmt"

	"reduction.dev/reduction-go/internal"
)

// valueStatus represents the sync state of a value.
type valueStatus int

const (
	statusInitial valueStatus = iota
	statusUpdated
	statusDeleted
)

type ValueState[T any] struct {
	name   string
	value  T
	status valueStatus
	codec  ValueCodec[T]
}

func (s *ValueState[T]) Load(entries []internal.StateEntry) error {
	// Use a single entry
	var entry internal.StateEntry
	if len(entries) > 0 {
		entry = entries[0]
	}

	if len(entry.Value) == 0 {
		return nil
	}

	value, err := s.codec.Decode(entry.Value)
	if err != nil {
		return fmt.Errorf("failed to decode value: %w", err)
	}
	s.value = value
	return nil
}

func (s *ValueState[T]) Mutations() ([]internal.StateMutation, error) {
	if s.status == statusInitial {
		return nil, nil
	}

	if s.status == statusDeleted {
		return []internal.StateMutation{&internal.DeleteMutation{
			Key: []byte(s.Name()),
		}}, nil
	}

	data, err := s.codec.Encode(s.value)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}

	return []internal.StateMutation{&internal.PutMutation{
		Key:   []byte(s.Name()),
		Value: data,
	}}, nil
}

func (s *ValueState[T]) Name() string {
	return s.name
}

func (s *ValueState[T]) Value() T {
	return s.value
}

func (s *ValueState[T]) Set(value T) {
	s.status = statusUpdated
	s.value = value
}

func (s *ValueState[T]) Drop() {
	s.status = statusDeleted
	var zero T
	s.value = zero
}

// NewValueState creates a new ValueState for either ProtoScalar or BinaryValue types
func NewValueState[T any](name string, codec ValueCodec[T]) *ValueState[T] {
	return &ValueState[T]{
		name:  name,
		codec: codec,
	}
}

var _ internal.StateItem = (*ValueState[int])(nil)

type ScalarCodec[T internal.ProtoScalar] struct{}

func (ScalarCodec[T]) Encode(value T) ([]byte, error) {
	return internal.EncodeScalar(value)
}

func (ScalarCodec[T]) Decode(b []byte) (T, error) {
	return internal.DecodeScalar[T](b)
}
