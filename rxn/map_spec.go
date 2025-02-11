package rxn

import (
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
)

type MapSpec[K comparable, T any] struct {
	StateSpec[MapState[K, T]]
}

func NewMapSpec[K comparable, T any](op *jobs.Operator, id string, codec MapStateCodec[K, T]) MapSpec[K, T] {
	ss := StateSpec[MapState[K, T]]{
		ID:    id,
		Query: types.QueryTypeScan,
		Load: func(stateEntries []StateEntry) (*MapState[K, T], error) {
			ms := NewMapState(id, WithCodec(codec))
			return ms, ms.Load(stateEntries)
		},
		Mutations: func(state *MapState[K, T]) ([]StateMutation, error) {
			return state.Mutations()
		},
	}
	op.RegisterSpec(ss.ID, ss.Query)

	return MapSpec[K, T]{ss}
}
