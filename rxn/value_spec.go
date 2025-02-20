package rxn

import (
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
)

type ValueCodec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(b []byte) (T, error)
}

type ValueSpec[T any] struct {
	StateSpec[ValueState[T]]
}

func NewValueSpec[T any](op *jobs.Operator, id string, codec ValueCodec[T]) ValueSpec[T] {
	ss := StateSpec[ValueState[T]]{
		ID:    id,
		Query: types.QueryTypeGet,
		Load: func(stateEntries []internal.StateEntry) (*ValueState[T], error) {
			ms := &ValueState[T]{
				name:  id,
				codec: codec,
			}
			return ms, ms.Load(stateEntries)
		},
		Mutations: func(state *ValueState[T]) ([]internal.StateMutation, error) {
			return state.Mutations()
		},
	}
	op.RegisterSpec(ss.ID, ss.Query)

	return ValueSpec[T]{ss}
}
