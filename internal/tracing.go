package internal

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Deprecated: use github.com/ipfs/boxo/path/internal.StartSpan
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("go-path").Start(ctx, fmt.Sprintf("Path.%s", name), opts...)
}
