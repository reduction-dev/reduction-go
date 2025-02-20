// The topology package provides the serializable definition of a job.
package topology

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/rxnsvr"
)

type Job struct {
	WorkerCount              int
	KeyGroupCount            int
	WorkingStorageLocation   string
	SavepointStorageLocation string

	sources []types.Source
	sinks   []types.SinkSynthesizer
}

// RegisterSource adds a source to the job. Only intended for internal use.
func (j *Job) RegisterSource(source types.Source) {
	j.sources = append(j.sources, source)
}

func (j *Job) RegisterSink(sink types.SinkSynthesizer) {
	j.sinks = append(j.sinks, sink)
}

func (j *Job) Synthesize() (*types.JobSynthesis, error) {
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

	// Create doc of constructs
	doc := &document{
		Sources: sourceConstructs,
		Sinks:   sinkConstructs,
		Job: types.Construct{
			Params: map[string]any{
				"WorkerCount":              j.WorkerCount,
				"KeyGroupCount":            j.KeyGroupCount,
				"WorkingStorageLocation":   j.WorkingStorageLocation,
				"SavepointStorageLocation": j.SavepointStorageLocation,
				"SourceIDs":                sourceIDs,
				"SinkIDs":                  sinkIDs,
			},
		},
	}

	sourceSynth := j.sources[0].Synthesize()
	if len(sourceSynth.Operators) == 0 {
		return nil, fmt.Errorf("source is missing operator")
	}
	if len(sourceSynth.Operators) > 1 {
		//lint:ignore ST1005 // capitalizing proper name
		return nil, fmt.Errorf("Reduction currently supports only one operator per source but has %d configured", len(sourceSynth.Operators))
	}

	return &types.JobSynthesis{
		Handler: &types.SynthesizedHandler{
			KeyEventFunc:    sourceSynth.KeyEventFunc,
			OperatorHandler: sourceSynth.Operators[0].Synthesize().Handler,
		},
		Config: doc,
	}, nil
}

// A representation of the json document to produce when marshalling.
type document struct {
	Job     types.Construct
	Sources map[string]types.Construct
	Sinks   map[string]types.Construct
}

func (d *document) Marshal() []byte {
	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		panic(fmt.Sprintf("BUG: could not marhsal synthesized job: %v", err))
	}
	return data
}

// Run provides a CLI for the provided job configuration and handles either a
// "start" or "config" command. Config prints the job config to stdout. Start
// runs the job on ":8080".
func (j *Job) Run() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <command>", os.Args[0])
	}

	synth, err := j.Synthesize()
	if err != nil {
		log.Fatalf("invalid job configuration: %v", err)
	}

	switch os.Args[1] {
	case "start":
		listener, err := net.Listen("tcp", ":8080")
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		svr := rxnsvr.New(synth.Handler, rxnsvr.WithListener(listener))
		if err := svr.Start(); err != nil {
			log.Fatalf("server stopped with error: %v", err)
		}
	case "config":
		fmt.Printf("%s", synth.Config.Marshal())
	default:
		log.Fatalf("Unknown command: %s", os.Args[1])
	}
}
