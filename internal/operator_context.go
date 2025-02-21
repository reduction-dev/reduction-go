package internal

type OperatorContext struct {
	Sinks []SinkSynthesizer
}

func (o *OperatorContext) RegisterSink(sink SinkSynthesizer) {
	o.Sinks = append(o.Sinks, sink)
}
