package topology

import (
	"fmt"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
)

type StateSpec[T any] struct {
	id        string
	query     types.QueryType
	load      func([]internal.StateEntry) (*T, error)
	mutations func(*T) ([]internal.StateMutation, error)
}

func (s *StateSpec[T]) StateFor(subject *internal.Subject) *T {
	if state := subject.LoadedState(s.id); state != nil {
		return state.(*T)
	}
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
