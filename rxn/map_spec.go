package rxn

import (
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
)

type MapSpec[K comparable, T any] struct {
	StateSpec[MapState[K, T]]
}

type MapState[K comparable, T any] = states.MapState[K, T]
type MapStateCodec[K comparable, T any] = states.MapStateCodec[K, T]
type ScalarMapStateCodec[K comparable, T any] = states.ScalarMapStateCodec[K, T]

func NewMapSpec[K comparable, T any](op *jobs.Operator, id string, codec states.MapStateCodec[K, T]) MapSpec[K, T] {
	ss := StateSpec[MapState[K, T]]{
		ID:    id,
		Query: types.QueryTypeScan,
		Load: func(stateEntries []StateEntry) (*MapState[K, T], error) {
			ms := states.NewMapState(id, states.WithCodec(codec))
			return ms, ms.Load(stateEntries)
		},
		Mutations: func(state *MapState[K, T]) ([]internal.StateMutation, error) {
			return state.Mutations()
		},
	}
	op.RegisterSpec(ss.ID, ss.Query)

	return MapSpec[K, T]{ss}
}
