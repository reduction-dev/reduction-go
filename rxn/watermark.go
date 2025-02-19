package rxn

import (
	"context"
	"time"

	"reduction.dev/reduction-go/internal"
)

// Watermark retrieves the current watermark for the operator.
func Watermark(ctx context.Context) time.Time {
	watermark, ok := ctx.Value(internal.WatermarkContextKey).(time.Time)
	if !ok {
		panic("missing watermark in context")
	}
	return watermark
}
