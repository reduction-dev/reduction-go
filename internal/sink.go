package internal

import "reduction.dev/reduction-protocol/jobconfigpb"

type SinkSynthesizer interface {
	Synthesize() SinkSynthesis
}

type SinkSynthesis struct {
	Config *jobconfigpb.Sink
}
