package gcs

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"go.opencensus.io/trace"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
)

func startOpenCensusSpan(ctx context.Context, spanName string) (context.Context, *trace.Span) {
	var spanCtx trace.SpanContext

	// try to link the new span with an existing OpenTracing span
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		jaegerSpanCtx, ok := span.Context().(jaeger.SpanContext)
		if ok {
			opencensusTraceID := tracetranslator.UInt64ToTraceID(jaegerSpanCtx.TraceID().High, jaegerSpanCtx.TraceID().Low)
			opencensusSpanID := tracetranslator.UInt64ToSpanID(uint64(jaegerSpanCtx.SpanID()))

			spanCtx = trace.SpanContext{
				TraceID: opencensusTraceID.Bytes(),
				SpanID:  opencensusSpanID.Bytes(),
			}
		}
	}

	return trace.StartSpanWithRemoteParent(ctx, spanName, spanCtx)
}
