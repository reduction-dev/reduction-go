package rxn

import (
	"context"

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

var CurrentWatermark = internal.CurrentWatermark
