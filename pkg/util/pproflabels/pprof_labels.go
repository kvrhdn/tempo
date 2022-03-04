package pproflabels

import (
	"context"
	"net/http"
	"runtime/pprof"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/grafana/tempo/pkg/util"
)

// HTTPMiddleware adds request-specific pprof labels to the goroutine for the duration of the request.
func HTTPMiddleware() middleware.Interface {
	return middleware.Func(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			pprof.Do(ctx, extractPprofLabels(ctx), func(ctx context.Context) {
				r = r.WithContext(ctx)
				handler.ServeHTTP(w, r)
			})
		})
	})
}

// GRPCMiddleware adds request-specific pprof labels to the goroutine for the duration of the request.
func GRPCMiddleware() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		pprof.Do(ctx, extractPprofLabels(ctx), func(ctx context.Context) {
			resp, err = handler(ctx, req)
		})
		return
	}
}

// GRPCStreamMiddleware adds request-specific pprof labels to the goroutine for the duration of the request.
func GRPCStreamMiddleware() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		ctx := ss.Context()
		pprof.Do(ctx, extractPprofLabels(ctx), func(ctx context.Context) {
			err = handler(srv, ss)
		})
		return
	}
}

func extractPprofLabels(ctx context.Context) (labels pprof.LabelSet) {
	traceID, _ := util.ExtractTraceID(ctx)
	orgID, _ := user.ExtractOrgID(ctx)
	return pprof.Labels("traceID", traceID, "tenant", orgID)
}
