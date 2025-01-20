package connectors

import (
	"context"
	"encoding/json"
	"log"

	"reduction.dev/reduction-go/rxn"
)

type HTTPAPISink struct {
	ID string
}

type HTTPSinkEvent struct {
	// A namespace for writing the record
	Topic string
	// Arbitrary data to send to the server
	Data []byte
}

func (s *HTTPAPISink) Collect(ctx context.Context, event *HTTPSinkEvent) {
	subject, ok := ctx.Value(rxn.SubjectContextKey).(*rxn.Subject)
	if !ok {
		panic("must pass rxn context to sink.Collect")
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Fatal("httpapi Sink json.Marshal", "err", err)
	}
	subject.AddSinkRequest(s.ID, payload)
}
