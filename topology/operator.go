package topology

import (
	"context"
	"time"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/rxn"
)

type Operator = internal.Operator

type OperatorParams struct {
	Parallelism int
	Handler     HandlerFactory
}

type HandlerFactory = func(op *Operator) rxn.OperatorHandler

func NewOperator(job *Job, id string, params *OperatorParams) *Operator {
	operator := internal.NewOperator(id)
	operator.Handler = internalHandler{params.Handler(operator)}

	return operator
}

type internalHandler struct {
	handler rxn.OperatorHandler
}

func (a internalHandler) OnEvent(ctx context.Context, internalSubject *internal.Subject, event internal.KeyedEvent) error {
	return a.handler.OnEvent(ctx, rxn.Subject(internalSubject), event)
}

func (a internalHandler) OnTimerExpired(ctx context.Context, internalSubject *internal.Subject, ts time.Time) error {
	return a.handler.OnTimerExpired(ctx, rxn.Subject(internalSubject), ts)
}
