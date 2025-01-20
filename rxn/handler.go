package rxn

import (
	"context"
	"net"
	"time"
)

var server *HTTPServer

type Option func(*HTTPServer)

func Start(h Handler, opts ...Option) error {
	server := newServer(h, newServerParams{})

	for _, o := range opts {
		o(server)
	}

	return server.Start()
}

func Close() {
	if server != nil {
		server.Stop()
	}
}

func WithAddress(addr string) func(server *HTTPServer) {
	return func(s *HTTPServer) {
		s.addr = addr
	}
}

func WithListener(l net.Listener) func(server *HTTPServer) {
	return func(s *HTTPServer) {
		s.listener = l
	}
}

// Uses a singleton, packaged scoped server
func DefaultServer(h Handler, opts ...Option) *HTTPServer {
	server = newServer(h, newServerParams{})

	for _, o := range opts {
		o(server)
	}
	return server
}

// Create a new server instance
func NewServer(h Handler, opts ...Option) *HTTPServer {
	server := newServer(h, newServerParams{})
	for _, o := range opts {
		o(server)
	}
	return server
}

type UnimplementedHandler struct{}

func (u UnimplementedHandler) KeyEvent(ctx context.Context, rawEvent []byte) (mappedEvent []KeyedEvent, err error) {
	panic("unimplemented")
}

func (u UnimplementedHandler) OnEvent(ctx context.Context, subject *Subject, rawEvent []byte) error {
	panic("unimplemented")
}

func (u UnimplementedHandler) OnTimerExpired(ctx context.Context, subject *Subject, timer time.Time) error {
	panic("unimplemented")
}

var _ Handler = UnimplementedHandler{}
