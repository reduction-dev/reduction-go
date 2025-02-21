package internal

type SinkSynthesizer interface {
	Synthesize() SinkSynthesis
}

type SinkSynthesis struct {
	Construct Construct
}
