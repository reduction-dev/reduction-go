package rxn

import (
	"encoding/binary"
	"fmt"
)

type IntState struct {
	name  string
	Value int
}

func (s *IntState) Load(entries []StateEntry) error {
	// Use a single entry
	var entry StateEntry
	if len(entries) > 0 {
		entry = entries[0]
	}

	if len(entry.Value) == 0 {
		return nil
	}

	if len(entry.Value) != 8 {
		return fmt.Errorf("invalid IntState data: %s", entry.Value)
	}
	s.Value = int(binary.BigEndian.Uint64(entry.Value))
	return nil
}

func (s *IntState) Mutations() ([]StateMutation, error) {
	buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(s.Value))

	return []StateMutation{&PutMutation{
		Key:   []byte(s.Name()),
		Value: buf,
	}}, nil
}

func NewIntState(name string) *IntState {
	return &IntState{name, 0}
}

func (s *IntState) Marshal() ([]byte, error) {
	buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(s.Value))
	return buf, nil
}

func (s *IntState) Name() string {
	return s.name
}

func (s *IntState) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if len(data) != 8 {
		return fmt.Errorf("invalid IntState data: %s", data)
	}
	s.Value = int(binary.BigEndian.Uint64(data))
	return nil
}

var _ StateItem = (*IntState)(nil)
