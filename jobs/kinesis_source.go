package jobs

import "reduction.dev/reduction-go/rxn"

type KinesisSource struct {
	id        string
	streamARN string
	endpoint  string
}

type KinesisSourceParams struct {
	StreamARN string
	Endpoint  string
	Targets   []rxn.Handler
}

func NewKinesisSource(id string, params *KinesisSourceParams) *KinesisSource {
	return &KinesisSource{
		id:        id,
		streamARN: params.StreamARN,
		endpoint:  params.Endpoint,
	}
}

func (s *KinesisSource) isSource() {}

func (s *KinesisSource) construct() (string, construct) {
	return s.id, construct{
		Type: "Source:Kinesis",
		Params: map[string]any{
			"StreamARN": s.streamARN,
			"Endpoint":  s.endpoint,
		},
	}
}

var _ Source = (*KinesisSource)(nil)
