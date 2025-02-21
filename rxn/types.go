package rxn

import (
	"context"
	"time"

	"reduction.dev/reduction-go/internal"
)

// OperatorHandler defines the two methods operators implement to handle events
// and timers.

type OperatorHandler interface {
	// Called when a new event arrives. The subject is a set of APIs scoped to
	// the specific partition key being used. Because of this scoping, think of this
	// as the subject (e.g. a User, a Product) in your domain.
	OnEvent(ctx context.Context, subject Subject, event KeyedEvent) error

	// A previously set timer expires. This is an asynchronous action where the
	// timer fires at the specified time AT THE EARLIEST. That means that events
	// after the timer's timestamp have likely already arrived.
	OnTimerExpired(ctx context.Context, subject Subject, timer time.Time) error
}

// KeyedEvent has a Key used for partitioning data and a timestamp used for
// tracking time. It's value is arbitrary byte data.
type KeyedEvent = internal.KeyedEvent

// Sink is a generic interface for collecting values.
type Sink[T any] interface {
	Collect(ctx context.Context, value T)
}
