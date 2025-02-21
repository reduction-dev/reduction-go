// This package has APIs for running the Reduction handler server directly.  In
// general, you should use [reduction.dev/reduction-go/rxn.Run] which supports
// the `serve` sub command for running the server.
//
// This package is currently used for integration testing in the main reduction
// repo and may be removed.
package rxnsvr

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"connectrpc.com/connect"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/rpc"
	"reduction.dev/reduction-protocol/handlerpb/handlerpbconnect"
)

type Server struct {
	httpServer *http.Server
	addr       string
	listener   net.Listener
}

type Option func(*Server)

func WithListener(l net.Listener) func(server *Server) {
	return func(s *Server) {
		s.listener = l
	}
}

// Create a new server instance
func New(handler internal.ServerHandler, opts ...Option) *Server {
	mux := http.NewServeMux()

	// Add connect service to mux
	path, connectHandler := handlerpbconnect.NewHandlerHandler(
		rpc.NewConnectHandler(handler),
		connect.WithInterceptors(newLoggingInterceptor("handler")),
	)
	mux.Handle(path, connectHandler)

	// Add health check to mux
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	s := &http.Server{Handler: mux}
	server := &Server{httpServer: s}
	for _, o := range opts {
		o(server)
	}
	return server
}

func (s *Server) Start() error {
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

func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func newLoggingInterceptor(prefix string) connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			slog.Debug("["+prefix+"] request", "url", req.Spec().Procedure, "msg", req.Any())
			return next(ctx, req)
		})
	}

	return connect.UnaryInterceptorFunc(interceptor)
}
