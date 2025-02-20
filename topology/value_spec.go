package topology

import (
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
	"reduction.dev/reduction-go/internal/types"
)

type ValueCodec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(b []byte) (T, error)
}

type ValueSpec[T any] struct {
	StateSpec[states.ValueState[T]]
}

func NewValueSpec[T any](op *Operator, id string, codec ValueCodec[T]) ValueSpec[T] {
	ss := StateSpec[states.ValueState[T]]{
		id:    id,
		query: types.QueryTypeGet,
		load: func(stateEntries []internal.StateEntry) (*states.ValueState[T], error) {
			internalState := states.NewValueState(id, codec)
			err := internalState.Load(stateEntries)
			if err != nil {
				return nil, err
			}
			return internalState, nil
		},
		mutations: func(state *states.ValueState[T]) ([]internal.StateMutation, error) {
			return state.Mutations()
		},
	}
	op.RegisterSpec(ss.id, ss.query)
	return ValueSpec[T]{ss}
}

type ScalarCodec[T internal.ProtoScalar] struct{}

func (ScalarCodec[T]) Encode(value T) ([]byte, error) {
	return internal.EncodeScalar(value)
}

func (ScalarCodec[T]) Decode(b []byte) (T, error) {
	return internal.DecodeScalar[T](b)
}
