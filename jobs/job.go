// The jobs package provides the serializable definition of a job.
package jobs

import (
	"encoding/json"
)

type Job struct {
	doc document
}

type JobParams struct {
	WorkerCount            int
	KeyGroupCount          int
	WorkingStorageLocation string
	Sources                []Source
	Sinks                  []Sink
}

func NewJob(id string, params *JobParams) *Job {
	sourceConstructs := make(map[string]construct, len(params.Sources))
	sourceIDs := make([]string, len(params.Sources))
	for i, s := range params.Sources {
		id, c := s.construct()
		sourceConstructs[id] = c
		sourceIDs[i] = id
	}

	sinkConstructs := make(map[string]construct, len(params.Sinks))
	sinkIDs := make([]string, len(params.Sinks))
	for i, s := range params.Sinks {
		id, c := s.construct()
		sinkConstructs[id] = c
		sinkIDs[i] = id
	}

	return &Job{doc: document{
		Job: construct{
			Type: "Job",
			Params: map[string]any{
				"ID":                     id,
				"WorkerCount":            params.WorkerCount,
				"KeyGroupCount":          params.KeyGroupCount,
				"WorkingStorageLocation": params.WorkingStorageLocation,
				"SourceIDs":              sourceIDs,
				"SinkIDs":                sinkIDs,
			},
		},
		Sources: sourceConstructs,
		Sinks:   sinkConstructs,
	}}
}

func (j *Job) Marshal() []byte {
	return j.doc.Marshal()
}

// A representation of the json document to produce when marshalling.
type document struct {
	Job     construct
	Sources map[string]construct
	Sinks   map[string]construct
}

func (d *document) Marshal() []byte {
	bytes, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		panic(err)
	}

	return bytes
}

// construct defines the format for each abstract type
type construct struct {
	Type   string
	Params map[string]any
}
