package topology

import (
	"iter"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/states"
	"reduction.dev/reduction-go/internal/types"
)

// A MapSpec defines a schema for key-value state.
type MapSpec[K comparable, T any] struct {
	spec StateSpec[states.MapState[K, T]]
}

type MapState[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Delete(key K)
	All() iter.Seq2[K, V]
	Size() int
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
func NewMapSpec[K comparable, T any](op *Operator, id string, codec states.MapStateCodec[K, T]) MapSpec[K, T] {
	ss := StateSpec[states.MapState[K, T]]{
		id:    id,
		query: types.QueryTypeScan,
		load: func(stateEntries []internal.StateEntry) (*states.MapState[K, T], error) {
			internalState := states.NewMapState(id, codec)
			err := internalState.Load(stateEntries)
			if err != nil {
				return nil, err
			}
			return internalState, nil
		},
		mutations: func(state *states.MapState[K, T]) ([]internal.StateMutation, error) {
			return state.Mutations()
		},
	}
	op.RegisterSpec(ss.id, ss.query)
	return MapSpec[K, T]{ss}
}

func (m MapSpec[K, V]) StateFor(subject *internal.Subject) MapState[K, V] {
	return m.spec.StateFor(subject)
}
