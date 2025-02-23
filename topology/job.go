// The topology package provides the serializable definition of a job.
package topology

import (
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/rxnsvr"
	"reduction.dev/reduction-protocol/jobconfigpb"
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

	// Create protobuf config
	config := &jobconfigpb.JobConfig{
		Job: &jobconfigpb.Job{
			WorkerCount:              int32(j.WorkerCount),
			KeyGroupCount:            int32(j.KeyGroupCount),
			WorkingStorageLocation:   j.WorkingStorageLocation,
			SavepointStorageLocation: j.SavepointStorageLocation,
		},
		Sources: make([]*jobconfigpb.Source, len(j.sources)),
		Sinks:   make([]*jobconfigpb.Sink, len(j.sinks)),
	}

	// Convert sources and sinks to protobuf format
	sourceSynth := j.sources[0].Synthesize()
	config.Sources[0] = sourceSynth.Config

	for i, s := range j.sinks {
		synth := s.Synthesize()
		config.Sinks[i] = &jobconfigpb.Sink{
			Id: synth.Construct.ID,
			// Config will be set by the sink implementations
		}
	}

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
		Config: protoConfig{config},
	}, nil
}

// protoConfig wraps jobconfigpb.JobConfig to provide Marshal method
type protoConfig struct {
	*jobconfigpb.JobConfig
}

func (p protoConfig) Marshal() []byte {
	marshaler := protojson.MarshalOptions{
		Indent: "  ",
	}
	data, err := marshaler.Marshal(p.JobConfig)
	if err != nil {
		panic(fmt.Sprintf("BUG: could not marshal synthesized job: %v", err))
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
