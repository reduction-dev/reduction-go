package types

import (
	"context"
	"time"

	"reduction.dev/reduction-go/internal"
)

// The ServerHandler represented the synthesized handler for a job that includes
// the source's KeyEvent funtions and methods on the OperatorHandler implemented
// by the user.
type ServerHandler interface {
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
	OnEvent(ctx context.Context, subject *internal.Subject, event KeyedEvent) error

	// A previously set timer expires. This is an asynchronous action where the
	// timer fires at the specified time AT THE EARLIEST. That means that events
	// after the timer's timestamp have likely already arrived.
	OnTimerExpired(ctx context.Context, subject *internal.Subject, timer time.Time) error
}

type UnimplementedHandler struct{}

func (u UnimplementedHandler) KeyEvent(ctx context.Context, rawEvent []byte) (mappedEvent []KeyedEvent, err error) {
	panic("unimplemented")
}

func (u UnimplementedHandler) OnEvent(ctx context.Context, subject *internal.Subject, event KeyedEvent) error {
	panic("unimplemented")
}

func (u UnimplementedHandler) OnTimerExpired(ctx context.Context, subject *internal.Subject, timer time.Time) error {
	panic("unimplemented")
}

var _ ServerHandler = UnimplementedHandler{}
