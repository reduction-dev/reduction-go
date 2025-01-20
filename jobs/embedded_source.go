package jobs

type EmbeddedSource struct {
	id         string
	splitCount int
	batchSize  int
	generator  string
}

type EmbeddedSourceParams struct {
	SplitCount int
	BatchSize  int
	Generator  string
}

func NewEmbeddedSource(id string, params *EmbeddedSourceParams) *EmbeddedSource {
	return &EmbeddedSource{
		id:         id,
		splitCount: params.SplitCount,
		batchSize:  params.BatchSize,
		generator:  params.Generator,
	}
}

func (s *EmbeddedSource) isSource() {}

func (s *EmbeddedSource) construct() (string, construct) {
	return s.id, construct{
		Type: "Source:Embedded",
		Params: map[string]any{
			"SplitCount": s.splitCount,
			"BatchSize":  s.batchSize,
			"Generator":  s.generator,
		},
	}
}

var _ Source = (*EmbeddedSource)(nil)
