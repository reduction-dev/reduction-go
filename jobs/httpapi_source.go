package jobs

type HTTPAPISource struct {
	id     string
	addr   string
	topics []string
}

type HTTPAPISourceParams struct {
	Addr   string
	Topics []string
}

func NewHTTPAPISource(id string, params *HTTPAPISourceParams) *HTTPAPISource {
	return &HTTPAPISource{
		id:     id,
		addr:   params.Addr,
		topics: params.Topics,
	}
}

func (s *HTTPAPISource) isSource() {}

func (s *HTTPAPISource) construct() (string, construct) {
	return s.id, construct{
		Type: "Source:HTTPAPI",
		Params: map[string]any{
			"Addr":   s.addr,
			"Topics": s.topics,
		},
	}
}

var _ Source = (*HTTPAPISource)(nil)
