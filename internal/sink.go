package internal

import "reduction.dev/reduction-protocol/jobconfigpb"

type SinkSynthesizer interface {
	Synthesize() SinkSynthesis
}

type SinkSynthesis struct {
	// TODO: Remove Construct after pb migration
	Construct Construct
	Config    *jobconfigpb.Sink
}
