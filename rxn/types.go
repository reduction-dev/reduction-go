package rxn

import (
	"context"
	"fmt"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
)

type SourceConfig interface {
	IsSource()
	Construct() (string, types.Construct)
	KeyEvent(ctx context.Context, record []byte) ([]KeyedEvent, error)
}

type SinkConfig interface {
	IsSink()
	Construct() (string, types.Construct)
}

type Subject = internal.Subject
type StateEntry = internal.StateEntry
type StateMutation = internal.StateMutation
type PutMutation = internal.PutMutation
type DeleteMutation = internal.DeleteMutation
type StateItem = internal.StateItem
type OperatorHandler = types.OperatorHandler
type KeyedEvent = types.KeyedEvent
type ServerHandler = types.ServerHandler

var CurrentWatermark = internal.CurrentWatermark

type QueryType = types.QueryType

type StateSpec[T any] struct {
	ID        string
	Query     QueryType
	Load      func([]StateEntry) (*T, error)
	Mutations func(*T) ([]StateMutation, error)
}

func (s *StateSpec[T]) StateFor(subject *Subject) *T {
	// Look up state in subject's loadedStates first
	if state := subject.LoadedState(s.ID); state != nil {
		return state.(*T)
	}

	// Create new state instance
	state, err := s.Load(subject.StateEntries(s.ID))
	if err != nil {
		panic(fmt.Sprintf("failed to load state for %s: %v", s.ID, err))
	}
	var mutations internal.LazyMutations = func() ([]StateMutation, error) {
		return s.Mutations(state)
	}
	subject.RegisterStateUse(s.ID, mutations)
	subject.StoreLoadedState(s.ID, state)
	return state
}
