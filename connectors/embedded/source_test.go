package embedded_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/connectors/embedded"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

func TestSourceSynthesize(t *testing.T) {
	job := &topology.Job{}
	source := embedded.NewSource(job, "test-source", &embedded.SourceParams{
		SplitCount: 4,
		BatchSize:  100,
		Generator:  embedded.GeneratorSequence,
		KeyEvent:   func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error) { return nil, nil },
	})

	synth := source.Synthesize()

	assert.Equal(t, &jobconfigpb.Source{
		Id: "test-source",
		Config: &jobconfigpb.Source_Embedded{
			Embedded: &jobconfigpb.EmbeddedSource{
				SplitCount: 4,
				BatchSize:  100,
				Generator:  jobconfigpb.EmbeddedSource_GENERATOR_TYPE_SEQUENCE,
			},
		},
	}, synth.Config)
}
