// The jobs package provides the serializable definition of a job.
package jobs

import (
	"encoding/json"
	"fmt"

	"reduction.dev/reduction-go/internal/types"
)

type Job struct {
	WorkerCount            int
	KeyGroupCount          int
	WorkingStorageLocation string

	sources []Source
	sinks   []SinkSynthesizer
	doc     document
}

type JobConfig struct {
	WorkerCount            int
	KeyGroupCount          int
	WorkingStorageLocation string
}

type JobContext struct {
	sources []Source
	sinks   []SinkSynthesizer
}

func (c *JobContext) RegisterSource(source Source) {
	c.sources = append(c.sources, source)
}

type SinkSynthesizer interface {
	Synthesize() types.SinkSynthesis
}

func (c *JobContext) RegisterSink(sink SinkSynthesizer) {
	c.sinks = append(c.sinks, sink)
}

type JobBuilder func(ctx *JobContext) *JobConfig

func NewJob(id string, builder JobBuilder) *Job {
	ctx := &JobContext{}
	config := builder(ctx)

	job := &Job{
		WorkerCount:            config.WorkerCount,
		KeyGroupCount:          config.KeyGroupCount,
		WorkingStorageLocation: config.WorkingStorageLocation,
	}

	// Transfer registrations from context to job
	job.sources = ctx.sources
	job.sinks = ctx.sinks

	job.doc = document{
		Job: types.Construct{
			Type: "Job",
			ID:   id,
		},
	}
	return job
}

func (j *Job) Marshal() []byte {
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
		"WorkerCount":            j.WorkerCount,
		"KeyGroupCount":          j.KeyGroupCount,
		"WorkingStorageLocation": j.WorkingStorageLocation,
		"SourceIDs":              sourceIDs,
		"SinkIDs":                sinkIDs,
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
