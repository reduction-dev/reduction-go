package states

import (
	"fmt"

	"reduction.dev/reduction-go/internal"
)

type StateSpec[T any] struct {
	ID        string
	Query     internal.QueryType
	Load      func([]internal.StateEntry) (*T, error)
	Mutations func(*T) ([]internal.StateMutation, error)
}

func (s *StateSpec[T]) StateFor(subject *internal.Subject) *T {
	if state := subject.LoadedState(s.ID); state != nil {
		return state.(*T)
	}
	state, err := s.Load(subject.StateEntries(s.ID))
	if err != nil {
		panic(fmt.Sprintf("failed to load state for %s: %v", s.ID, err))
	}
	var mutations internal.LazyMutations = func() ([]internal.StateMutation, error) {
		return s.Mutations(state)
	}
	subject.RegisterStateUse(s.ID, mutations)
	subject.StoreLoadedState(s.ID, state)
	return state
}
