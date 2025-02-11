package rxn

import (
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
)

type ValueStateCodec[T any] interface {
	EncodeValue(value T) ([]byte, error)
	DecodeValue(b []byte) (T, error)
}

type ValueSpec[T any] struct {
	StateSpec[ValueState[T]]
}

func NewValueSpec[T any](op *jobs.Operator, id string, codec ValueStateCodec[T]) ValueSpec[T] {
	ss := StateSpec[ValueState[T]]{
		ID:    id,
		Query: types.QueryTypeGet,
		Load: func(stateEntries []StateEntry) (*ValueState[T], error) {
			ms := &ValueState[T]{
				name:  id,
				codec: codec,
			}
			return ms, ms.Load(stateEntries)
		},
		Mutations: func(state *ValueState[T]) ([]StateMutation, error) {
			return state.Mutations()
		},
	}
	op.RegisterSpec(ss.ID, ss.Query)

	return ValueSpec[T]{ss}
}
