package rxn

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction-handler/handlerpb/handlerpbconnect"

	"connectrpc.com/connect"
)

// The handler called as events arrive and timers fire.
type Handler interface {
	// When an event enters the job through the source, this method extracts or
	// generates a key and timestamp for the event. Workers use the key to
	// distribute events between themselves in the cluster. They use the timestamp
	// to understand the passing of event time.
	//
	// This method returns a list of KeyedEvents. This allows KeyEvent to both
	// filter out messages to avoid further processing or to expand a single event
	// into many.
	KeyEvent(ctx context.Context, rawEvent []byte) (keyedEvent []KeyedEvent, err error)

	// Called when a new event arrives. The subject is a set of APIs scoped to
	// the specific partition key being used. Because of this scoping, think of this
	// as the subject (e.g. a User, a Product) in your domain.
	OnEvent(ctx context.Context, subject *Subject, rawEvent []byte) error

	// A previously set timer expires. This is an asynchronous action where the
	// timer fires at the specified time AT THE EARLIEST. That means that events
	// after the timer's timestamp have likely already arrived.
	OnTimerExpired(ctx context.Context, subject *Subject, timer time.Time) error
}

type newServerParams struct {
	addr string
}

type HTTPServer struct {
	httpServer *http.Server
	addr       string
	listener   net.Listener
}

type KeyedEvent struct {
	Key       []byte
	Timestamp time.Time
	Value     []byte
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

type contextKey string

var SubjectContextKey = contextKey("subject")

// Receive connect requests and invoke the user's handler methods.
type rpcHandler struct {
	rxnHandler Handler
}

func (r *rpcHandler) KeyEvent(ctx context.Context, req *connect.Request[handlerpb.KeyEventRequest]) (*connect.Response[handlerpb.KeyEventResponse], error) {
	resp, err := r.rxnHandler.KeyEvent(ctx, req.Msg.Value)
	if err != nil {
		return nil, err
	}
	events := make([]*handlerpb.KeyedEvent, len(resp))
	for i, event := range resp {
		events[i] = &handlerpb.KeyedEvent{
			Key:       event.Key,
			Timestamp: timestamppb.New(event.Timestamp),
			Value:     event.Value,
		}
	}
	return connect.NewResponse(&handlerpb.KeyEventResponse{Events: events}), nil
}

func (r *rpcHandler) OnEvent(ctx context.Context, req *connect.Request[handlerpb.OnEventRequest]) (*connect.Response[handlerpb.HandlerResponse], error) {
	subject := newSubjectFromOnEventRequest(req.Msg)
	ctx = context.WithValue(ctx, SubjectContextKey, subject)
	if err := r.rxnHandler.OnEvent(ctx, subject, req.Msg.Event.Value); err != nil {
		return nil, err
	}
	return connect.NewResponse(subject.encode()), nil
}

func (r *rpcHandler) OnTimerExpired(ctx context.Context, req *connect.Request[handlerpb.OnTimerExpiredRequest]) (*connect.Response[handlerpb.HandlerResponse], error) {
	subject := newSubjectFromOnTimerExpiredRequest(req.Msg)
	ctx = context.WithValue(ctx, SubjectContextKey, subject)
	if err := r.rxnHandler.OnTimerExpired(ctx, subject, req.Msg.Timestamp.AsTime()); err != nil {
		return nil, err
	}
	return connect.NewResponse(subject.encode()), nil
}

var _ handlerpbconnect.HandlerHandler = (*rpcHandler)(nil)

func NewLoggingInterceptor(prefix string) connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			slog.Debug("["+prefix+"] request", "url", req.Spec().Procedure, "msg", req.Any())
			return next(ctx, req)
		})
	}

	return connect.UnaryInterceptorFunc(interceptor)
}
