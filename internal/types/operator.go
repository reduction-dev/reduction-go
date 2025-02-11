package types

type OperatorParams struct {
	Handler OperatorHandler
}

type Operator struct {
	ID          string
	Parallelism int
	Handler     OperatorHandler
	Sinks       []SinkSynthesizer
	stateSpecs  map[string]QueryType
}

type QueryType = string

const QueryTypeGet QueryType = "get"
const QueryTypeScan QueryType = "scan"

func NewOperator(id string) *Operator {
	// ID is currently unused.
	return &Operator{
		stateSpecs: make(map[string]QueryType),
	}
}

func (o *Operator) Synthesize() OperatorSynthesis {
	return OperatorSynthesis{
		Handler: o.Handler,
	}
}

func (op *Operator) Connect(sink SinkSynthesizer) {
	op.Sinks = append(op.Sinks, sink)
}

func (op *Operator) RegisterSpec(id string, queryType QueryType) {
	op.stateSpecs[id] = queryType
}
