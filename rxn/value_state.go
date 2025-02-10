package rxn

import (
	"fmt"

	"reduction.dev/reduction-go/internal"
)

type ProtoScalar = internal.ProtoScalar

type ValueState[T ProtoScalar] struct {
	name  string
	Value T
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

	value, err := internal.DecodeScalar[T](entry.Value)
	if err != nil {
		return fmt.Errorf("failed to decode value: %w", err)
	}
	s.Value = value
	return nil
}

func (s *ValueState[T]) Mutations() ([]StateMutation, error) {
	data, err := internal.EncodeScalar(s.Value)
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
func NewValueState[T ProtoScalar](name string) *ValueState[T] {
	return &ValueState[T]{name: name}
}

var _ StateItem = (*ValueState[int])(nil)
