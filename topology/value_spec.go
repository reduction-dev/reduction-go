package topology

import (
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/rxn"
)

func NewValueSpec[T any](op *Operator, id string, codec rxn.ValueCodec[T]) rxn.ValueSpec[T] {
	ss := states.StateSpec[states.ValueState[T]]{
		ID:    id,
		Query: types.QueryTypeGet,
		Load: func(stateEntries []internal.StateEntry) (*states.ValueState[T], error) {
			internalState := states.NewValueState(id, codec)
			err := internalState.Load(stateEntries)
			if err != nil {
				return nil, err
			}
			return internalState, nil
		},
		Mutations: func(state *states.ValueState[T]) ([]internal.StateMutation, error) {
			return state.Mutations()
		},
	}
	op.RegisterSpec(ss.ID, ss.Query)

	return &valueSpec[T]{ss}
}

type valueSpec[T any] struct {
	spec states.StateSpec[states.ValueState[T]]
}

func (v *valueSpec[T]) StateFor(subject *internal.Subject) rxn.ValueState[T] {
	return v.spec.StateFor(subject)
}
