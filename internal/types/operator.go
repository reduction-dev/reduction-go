package types

type OperatorParams struct {
	Handler OperatorHandler
}

type Operator struct {
	ID          string
	Parallelism int
	Handler     OperatorHandler
	Sinks       []SinkSynthesizer
}

func NewOperator(id string, params *OperatorParams) *Operator {
	// ID is currently unused.
	return &Operator{Handler: params.Handler}
}

func (o *Operator) Synthesize() OperatorSynthesis {
	return OperatorSynthesis{
		Handler: o.Handler,
	}
}
