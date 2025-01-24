package rxn

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"reduction.dev/reduction-handler/handlerpb/handlerpbconnect"

	"connectrpc.com/connect"
)

type newServerParams struct {
	addr string
}

type HTTPServer struct {
	httpServer *http.Server
	addr       string
	listener   net.Listener
}

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

// A singleton server instance
var server *HTTPServer

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

func (s *HTTPServer) Start() error {
	if s.listener == nil {
		var err error
		s.listener, err = net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}
	}

	slog.Info("starting server", "addr", s.listener.Addr().String())
	if err := s.httpServer.Serve(s.listener); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *HTTPServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *HTTPServer) Stop() error {
	return s.httpServer.Shutdown(context.Background())
}

// Create an http server to receive requests from the worker.
func newServer(handler Handler, params newServerParams) *HTTPServer {
	if params.addr == "" {
		params.addr = ":8080"
	}

	mux := http.NewServeMux()

	// Add connect service to mux
	path, connectHandler := handlerpbconnect.NewHandlerHandler(&rpcHandler{
		rxnHandler: handler,
	}, connect.WithInterceptors(NewLoggingInterceptor("handler")))
	mux.Handle(path, connectHandler)

	// Add health check to mux
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	s := &http.Server{Handler: mux}

	return &HTTPServer{addr: params.addr, httpServer: s}
}

func NewLoggingInterceptor(prefix string) connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			slog.Debug("["+prefix+"] request", "url", req.Spec().Procedure, "msg", req.Any())
			return next(ctx, req)
		})
	}

	return connect.UnaryInterceptorFunc(interceptor)
}
