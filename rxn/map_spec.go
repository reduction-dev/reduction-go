package rxn

import (
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/topology"
)

// A MapSpec defines a schema for key-value state.
type MapSpec[K comparable, T any] struct {
	StateSpec[MapState[K, T]]
}

// MapCodec is the interface for encoding and decoding map entries.
type MapCodec[K comparable, T any] = states.MapStateCodec[K, T]

// ScalarMapCodec is a concrete [MapCodec] which uses Protobuf to
// serialize simple scalar values. Supported types are:
//   - int, int32, int64
//   - uint, uint32, uint64
//   - float32, float64
//   - string
//   - bool
//   - time.Time
type ScalarMapCodec[K comparable, T any] = states.ScalarMapCodec[K, T]

// NewMapSpec creates a [MapSpec], registering itself with the provided
// [topology.Operator]. The ID with the subject's key uniquely identifies the state
// for a key to DKV queries. The codec defines how data is parsed and serialized
// for network transport and storage.
func NewMapSpec[K comparable, T any](op *topology.Operator, id string, codec states.MapStateCodec[K, T]) MapSpec[K, T] {
	ss := StateSpec[MapState[K, T]]{
		id:    id,
		query: types.QueryTypeScan,
		load: func(stateEntries []internal.StateEntry) (*MapState[K, T], error) {
			internalState := states.NewMapState(id, codec)
			err := internalState.Load(stateEntries)
			if err != nil {
				return nil, err
			}
			return &MapState[K, T]{internal: internalState}, nil
		},
		mutations: func(state *MapState[K, T]) ([]internal.StateMutation, error) {
			return state.internal.Mutations()
		},
	}
	op.RegisterSpec(ss.id, ss.query)

	return MapSpec[K, T]{ss}
}
