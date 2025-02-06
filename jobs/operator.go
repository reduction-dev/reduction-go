package jobs

import "reduction.dev/reduction-go/internal/types"

type Operator = types.Operator

type OperatorConfig struct {
	Parallelism int
	Handler     types.OperatorHandler
}

type OperatorContext = types.OperatorContext

type OperatorBuilder func(ctx *types.OperatorContext) *OperatorConfig

func NewOperator(ctx *JobContext, id string, builder OperatorBuilder) *Operator {
	opCtx := &types.OperatorContext{}
	config := builder(opCtx)

	operator := &types.Operator{
		ID:          id,
		Parallelism: config.Parallelism,
		Handler:     config.Handler,
		Sinks:       opCtx.Sinks,
	}

	return operator
}
