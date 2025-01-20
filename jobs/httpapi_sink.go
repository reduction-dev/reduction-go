package jobs

type HTTPAPISink struct {
	id   string
	addr string
}

type HTTPAPISinkParams struct {
	Addr string
}

func NewHTTPAPISink(id string, params *HTTPAPISinkParams) *HTTPAPISink {
	return &HTTPAPISink{addr: params.Addr}
}

func (s *HTTPAPISink) construct() (string, construct) {
	return s.id, construct{
		Type: "Sink:HTTPAPI",
		Params: map[string]any{
			"Addr": s.addr,
		},
	}
}

func (s *HTTPAPISink) isSink() {}

var _ Sink = (*HTTPAPISink)(nil)
