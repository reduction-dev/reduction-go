package states

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

type ValueCodec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

func (s *ValueState[T]) Load(entries []internal.StateEntry) error {
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

func NewValueState[T any](name string, codec ValueCodec[T]) *ValueState[T] {
	return &ValueState[T]{
		name:  name,
		codec: codec,
	}
}

// Ensure ValueState implements StateItem
var _ internal.StateItem = (*ValueState[int])(nil)

// ScalarValueCodec is a codec for simple scalar values using protobuf serialization
type ScalarValueCodec[T ProtoScalar] struct{}

func (c ScalarValueCodec[T]) Encode(value T) ([]byte, error) {
	return EncodeScalar(value)
}

func (c ScalarValueCodec[T]) Decode(b []byte) (T, error) {
	return DecodeScalar[T](b)
}
