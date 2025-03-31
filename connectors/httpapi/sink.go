package httpapi

import (
	"context"
	"encoding/json"
	"fmt"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

type Sink struct {
	id   string
	addr topology.ResolvableString
}

type SinkParams struct {
	Addr topology.ResolvableString
}

type SinkRecord struct {
	// A namespace for writing the record
	Topic string
	// Arbitrary data to send to the server
	Data []byte
}

func NewSink(job *topology.Job, id string, params *SinkParams) *Sink {
	sink := &Sink{
		id:   id,
		addr: params.Addr,
	}
	topology.InternalAccess(job).RegisterSink(sink)
	return sink
}

func (s *Sink) Synthesize() internal.SinkSynthesis {
	return internal.SinkSynthesis{
		Config: &jobconfigpb.Sink{
			Id: s.id,
			Config: &jobconfigpb.Sink_HttpApi{
				HttpApi: &jobconfigpb.HTTPAPISink{
					Addr: s.addr.Proto(),
				},
			},
		},
	}
}

func (s *Sink) Collect(ctx context.Context, value *SinkRecord) {
	subject := internal.SubjectFromContext(ctx)

	payload, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("httpapi Sink json.Marshal: %v", err))
	}
	subject.AddSinkRequest(s.id, payload)
}
