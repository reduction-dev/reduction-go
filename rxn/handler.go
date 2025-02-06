package rxn

import (
	"context"
	"time"

	"reduction.dev/reduction-go/internal/types"
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

type KeyedEvent = types.KeyedEvent

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
