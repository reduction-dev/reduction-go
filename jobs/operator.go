package jobs

import "reduction.dev/reduction-go/internal/types"

type Operator = types.Operator

type OperatorParams struct {
	Parallelism int
	Handler     types.OperatorHandler
}

func NewOperator(job *Job, id string, params *OperatorParams) *Operator {
	operator := &types.Operator{
		ID:          id,
		Parallelism: params.Parallelism,
		Handler:     params.Handler,
	}

	return operator
}
