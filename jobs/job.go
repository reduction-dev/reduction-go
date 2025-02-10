// The jobs package provides the serializable definition of a job.
package jobs

import (
	"encoding/json"
	"fmt"

	"reduction.dev/reduction-go/internal/types"
)

type Job struct {
	WorkerCount              int
	KeyGroupCount            int
	WorkingStorageLocation   string
	SavepointStorageLocation string

	sources []Source
	sinks   []types.SinkSynthesizer
	doc     document
}

func (j *Job) RegisterSource(source Source) {
	j.sources = append(j.sources, source)
}

func (j *Job) RegisterSink(sink types.SinkSynthesizer) {
	j.sinks = append(j.sinks, sink)
}

func (j *Job) Marshal() []byte {
	// Call Synthesize to ensure the document is up to date
	_, err := j.Synthesize()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal job: %v", err))
	}
	return j.doc.Marshal()
}

func (j *Job) Synthesize() (*types.SynthesizedHandler, error) {
	if len(j.sources) == 0 {
		return nil, fmt.Errorf("job is missing source")
	}
	if len(j.sources) > 1 {
		//lint:ignore ST1005 // capitalizing proper name
		return nil, fmt.Errorf("Reduction currently supports only one source per job but has %d configured", len(j.sources))
	}

	sourceConstructs := make(map[string]types.Construct, len(j.sources))
	sourceIDs := make([]string, len(j.sources))
	for i, s := range j.sources {
		synth := s.Synthesize()
		sourceConstructs[synth.Construct.ID] = synth.Construct
		sourceIDs[i] = synth.Construct.ID
	}

	sinkConstructs := make(map[string]types.Construct, len(j.sinks))
	sinkIDs := make([]string, len(j.sinks))
	for i, s := range j.sinks {
		synth := s.Synthesize()
		sinkConstructs[synth.Construct.ID] = synth.Construct
		sinkIDs[i] = synth.Construct.ID
	}

	// Update doc with current state
	j.doc.Sources = sourceConstructs
	j.doc.Sinks = sinkConstructs
	j.doc.Job.Params = map[string]any{
		"WorkerCount":              j.WorkerCount,
		"KeyGroupCount":            j.KeyGroupCount,
		"WorkingStorageLocation":   j.WorkingStorageLocation,
		"SavepointStorageLocation": j.SavepointStorageLocation,
		"SourceIDs":                sourceIDs,
		"SinkIDs":                  sinkIDs,
	}

	sourceSynth := j.sources[0].Synthesize()
	if len(sourceSynth.Operators) == 0 {
		return nil, fmt.Errorf("source is missing operator")
	}
	if len(sourceSynth.Operators) > 1 {
		//lint:ignore ST1005 // capitalizing proper name
		return nil, fmt.Errorf("Reduction currently supports only one operator per source but has %d configured", len(sourceSynth.Operators))
	}

	return &types.SynthesizedHandler{
		KeyEventFunc:    sourceSynth.KeyEventFunc,
		OperatorHandler: sourceSynth.Operators[0].Synthesize().Handler,
	}, nil
}

// A representation of the json document to produce when marshalling.
type document struct {
	Job     types.Construct
	Sources map[string]types.Construct
	Sinks   map[string]types.Construct
}

func (d *document) Marshal() []byte {
	bytes, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		panic(err)
	}

	return bytes
}

type Source = types.Source

type Sink = types.Sink

type KeyedEvent = types.KeyedEvent
