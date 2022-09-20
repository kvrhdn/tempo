package e2e

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/grafana/e2e"
	thrift "github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	util "github.com/grafana/tempo/integration"
	tempoUtil "github.com/grafana/tempo/pkg/util"
)

const (
	configTraceQL = "config-parquet.yaml"
)

func TestTraceQL(t *testing.T) {
	s, err := e2e.NewScenario("tempo_e2e")
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, util.CopyFileToSharedDir(s, configTraceQL, "config.yaml"))
	tempo := util.NewTempoAllInOne()
	require.NoError(t, s.StartAndWaitReady(tempo))

	// Get port for the Jaeger gRPC receiver endpoint
	c, err := util.NewJaegerGRPCClient(tempo.Endpoint(14250))
	require.NoError(t, err)
	require.NotNil(t, c)

	// Send some test data
	r := rand.New(rand.NewSource(time.Now().UnixMilli()))
	traceID1Low := r.Int63()
	traceID1High := r.Int63()
	parentSpanID := r.Int63()

	err = c.EmitBatch(context.Background(), &thrift.Batch{
		Process: &thrift.Process{ServiceName: "app"},
		Spans: []*thrift.Span{
			{
				TraceIdLow:    traceID1Low,
				TraceIdHigh:   traceID1High,
				SpanId:        parentSpanID,
				ParentSpanId:  0,
				OperationName: "HTTP GET /status",
				StartTime:     time.Now().UnixMicro(),
				Duration:      int64(5 * time.Second / time.Microsecond),
				// Tags:          []*thrift.Tag{{Key: "span.kind", VStr: stringPtr("client")}},
			},
			{
				TraceIdLow:    traceID1Low,
				TraceIdHigh:   traceID1High,
				SpanId:        r.Int63(),
				ParentSpanId:  parentSpanID,
				OperationName: "api.StatusHandler",
				StartTime:     time.Now().Add(time.Second).UnixMicro(),
				Duration:      int64(2 * time.Second / time.Microsecond),
				// Tags:          []*thrift.Tag{{Key: "span.kind", VStr: stringPtr("client")}},
			},
		},
	})
	require.NoError(t, err)

	traceID2Low := r.Int63()
	traceID2High := r.Int63()

	err = c.EmitBatch(context.Background(), &thrift.Batch{
		Process: &thrift.Process{ServiceName: "app"},
		Spans: []*thrift.Span{
			{
				TraceIdLow:    traceID2Low,
				TraceIdHigh:   traceID2High,
				SpanId:        r.Int63(),
				ParentSpanId:  0,
				OperationName: "batch.Do",
				StartTime:     time.Now().UnixMicro(),
				Duration:      int64(1 * time.Second / time.Microsecond),
				// Tags:          []*thrift.Tag{{Key: "span.kind", VStr: stringPtr("server")}},
			},
		},
	})
	require.NoError(t, err)

	apiClient := tempoUtil.NewClient("http://"+tempo.Endpoint(3200), "")

	// Send an invalid query
	req, err := http.NewRequest("GET", apiClient.BaseURL+"/api/search?q="+url.QueryEscape(`{ .foo = "bar" }`+"&tags="+url.QueryEscape(`bar=foo`)), nil)
	require.NoError(t, err, "creating test request")

	resp, err := apiClient.Do(req)
	require.NoError(t, err, "sending test request")

	assert.Equal(t, 400, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "invalid TraceQL query: parse error at line 1, col 17: syntax error: unexpected IDENTIFIER\n", string(body))

	// Send an invalid TraceQL query
	_, err = apiClient.SearchTraceQL(`{ .foo = "bar }`)
	assert.ErrorContains(t, err, "invalid TraceQL query: parse error at line 1, col 10: literal not terminated")

	// Send a valid TraceQL query
	searchResp, err := apiClient.SearchTraceQL(`{}`)
	assert.NoError(t, err)
	// TODO we need to check response here - it's not correct yet
	assert.NotNil(t, searchResp)

	searchResp, err = apiClient.SearchTraceQL(`{ .name = "api.StatusHandler" }`)
	assert.NoError(t, err)
	// TODO we need to check response here - it's not correct yet
	assert.NotNil(t, searchResp)
}
