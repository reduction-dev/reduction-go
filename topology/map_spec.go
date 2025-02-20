package topology

import (
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/rxn"
)

// NewMapSpec creates a [MapSpec], registering itself with the provided
// [topology.Operator]. The ID with the subject's key uniquely identifies the state
// for a key to DKV queries. The codec defines how data is parsed and serialized
// for network transport and storage.
func NewMapSpec[K comparable, T any](op *Operator, id string, codec states.MapStateCodec[K, T]) rxn.MapSpec[K, T] {
	ss := states.StateSpec[states.MapState[K, T]]{
		ID:    id,
		Query: types.QueryTypeScan,
		Load: func(stateEntries []internal.StateEntry) (*states.MapState[K, T], error) {
			internalState := states.NewMapState(id, codec)
			err := internalState.Load(stateEntries)
			if err != nil {
				return nil, err
			}
			return internalState, nil
		},
		Mutations: func(state *states.MapState[K, T]) ([]internal.StateMutation, error) {
			return state.Mutations()
		},
	}
	op.RegisterSpec(ss.ID, ss.Query)
	return &mapSpec[K, T]{ss}
}

type mapSpec[K comparable, T any] struct {
	spec states.StateSpec[states.MapState[K, T]]
}

func (m *mapSpec[K, T]) StateFor(subject *internal.Subject) rxn.MapState[K, T] {
	return m.spec.StateFor(subject)
}
