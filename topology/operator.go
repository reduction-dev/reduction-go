package topology

import "reduction.dev/reduction-go/internal/types"

type Operator = types.Operator

type OperatorParams struct {
	Parallelism int
	Handler     HandlerFactory
}

type HandlerFactory = func(op *Operator) types.OperatorHandler

func NewOperator(job *Job, id string, params *OperatorParams) *Operator {
	operator := types.NewOperator(id)
	operator.Handler = params.Handler(operator)

	return operator
}
