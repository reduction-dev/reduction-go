package rxn

import (
	"fmt"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
)

type Subject = internal.Subject

type OperatorHandler = types.OperatorHandler

// KeyedEvent has a Key used for partitioning data and a timestamp used for
// tracking time. It's value is arbitrary byte data.
type KeyedEvent = types.KeyedEvent

// StateSpec defines a schema for a specific keyed state.
type StateSpec[T any] struct {
	id        string
	query     types.QueryType
	load      func([]internal.StateEntry) (*T, error)
	mutations func(*T) ([]internal.StateMutation, error)
}

// StateFor retrieves an instance of state for the provided subject.
func (s *StateSpec[T]) StateFor(subject *Subject) *T {
	// Look up state in subject's loadedStates first
	if state := subject.LoadedState(s.id); state != nil {
		return state.(*T)
	}

	// Create new state instance
	state, err := s.load(subject.StateEntries(s.id))
	if err != nil {
		panic(fmt.Sprintf("failed to load state for %s: %v", s.id, err))
	}
	var mutations internal.LazyMutations = func() ([]internal.StateMutation, error) {
		return s.mutations(state)
	}
	subject.RegisterStateUse(s.id, mutations)
	subject.StoreLoadedState(s.id, state)
	return state
}
