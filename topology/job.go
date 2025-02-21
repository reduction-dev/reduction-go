// The topology package provides the serializable definition of a job.
package topology

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/rxnsvr"
)

type Job struct {
	WorkerCount              int
	KeyGroupCount            int
	WorkingStorageLocation   string
	SavepointStorageLocation string

	sources []internal.Source
	sinks   []internal.SinkSynthesizer
}

// registerSource adds a source to the job. Called via InternalAccess by
// connectors.
func (j *Job) registerSource(source internal.Source) {
	j.sources = append(j.sources, source)
}

// registerSink adds a sink to the job. Called via InternalAccess by
// connectors.
func (j *Job) registerSink(sink internal.SinkSynthesizer) {
	j.sinks = append(j.sinks, sink)
}

type jobSynthesis struct {
	Handler *internal.SynthesizedHandler
	Config  interface{ Marshal() []byte }
}

func (j *Job) Synthesize() (*jobSynthesis, error) {
	if len(j.sources) == 0 {
		return nil, fmt.Errorf("job is missing source")
	}
	if len(j.sources) > 1 {
		//lint:ignore ST1005 // capitalizing proper name
		return nil, fmt.Errorf("Reduction currently supports only one source per job but has %d configured", len(j.sources))
	}

	sourceConstructs := make(map[string]internal.Construct, len(j.sources))
	sourceIDs := make([]string, len(j.sources))
	for i, s := range j.sources {
		synth := s.Synthesize()
		sourceConstructs[synth.Construct.ID] = synth.Construct
		sourceIDs[i] = synth.Construct.ID
	}

	sinkConstructs := make(map[string]internal.Construct, len(j.sinks))
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
		Job: internal.Construct{
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

	return &jobSynthesis{
		Handler: &internal.SynthesizedHandler{
			KeyEventFunc:    sourceSynth.KeyEventFunc,
			OperatorHandler: sourceSynth.Operators[0].Synthesize().Handler,
		},
		Config: doc,
	}, nil
}

// A representation of the json document to produce when marshalling.
type document struct {
	Job     internal.Construct
	Sources map[string]internal.Construct
	Sinks   map[string]internal.Construct
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
