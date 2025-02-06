package internal

import (
	"context"
	"time"
)

var WatermarkContextKey = contextKey("watermark")

func CurrentWatermark(ctx context.Context) time.Time {
	watermark, ok := ctx.Value(WatermarkContextKey).(time.Time)
	if !ok {
		panic("missing watermark in context")
	}
	return watermark
}
