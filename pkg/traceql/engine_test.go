package traceql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/pkg/tempopb"
)

func TestEngine_Execute(t *testing.T) {
	e := Engine{}

	req := &tempopb.SearchRequest{}
	iterator := &MockSpanSetIterator{}

	response, err := e.Execute(context.Background(), req, &MockSpanSetFetcher{
		iterator: iterator,
	})
	require.NoError(t, err)
	assert.NotNil(t, response)
}

type MockSpanSetFetcher struct {
	iterator        SpansetIterator
	capturedRequest FetchSpansRequest
}

func (m *MockSpanSetFetcher) Fetch(ctx context.Context, request FetchSpansRequest) (FetchSpansResponse, error) {
	m.capturedRequest = request
	return FetchSpansResponse{
		Results: m.iterator,
	}, nil
}

type MockSpanSetIterator struct {
	results []*Spanset
}

func (m *MockSpanSetIterator) Next(ctx context.Context) (*Spanset, error) {
	r := m.results[0]
	m.results = m.results[1:]
	return r, nil
}

func TestEngine_createFetchSpansRequest(t *testing.T) {
	tests := []struct {
		name          string
		searchRequest *tempopb.SearchRequest
		err           string
		expected      FetchSpansRequest
	}{
		{
			name: "Duration set in SearchRequest",
			searchRequest: &tempopb.SearchRequest{
				Query:         `{}`,
				MinDurationMs: 100,
				MaxDurationMs: 1000,
			},
			expected: FetchSpansRequest{
				StartTimeUnixNanos: 0,
				EndTimeUnixNanos:   0,
				Conditions: []Condition{
					newCondition(NewIntrinsic(IntrinsicDuration), OpGreaterEqual, NewStaticDuration(100*time.Millisecond)),
					newCondition(NewIntrinsic(IntrinsicDuration), OpLess, NewStaticDuration(1000*time.Millisecond)),
				},
				AllConditions: true,
			},
		},
		{
			name: "Duration set in TraceQL",
			searchRequest: &tempopb.SearchRequest{
				Query: `{ duration >= 100ms && duration < 1000ms }`,
			},
			expected: FetchSpansRequest{
				Conditions: []Condition{
					{
						Op:        OpGreaterEqual,
						Attribute: NewIntrinsic(IntrinsicDuration),
						Operands:  []Static{NewStaticDuration(100 * time.Millisecond)},
					},
					{
						Op:        OpLess,
						Attribute: NewIntrinsic(IntrinsicDuration),
						Operands:  []Static{NewStaticDuration(1000 * time.Millisecond)},
					},
				},
				AllConditions: true,
			},
		},
		{
			name: "Duration set in SearchRequest and TraceQL",
			searchRequest: &tempopb.SearchRequest{
				Query:         `{ duration >= 250ms }`,
				MinDurationMs: 100,
			},
			expected: FetchSpansRequest{
				Conditions: []Condition{
					{
						Op:        OpGreaterEqual,
						Attribute: NewIntrinsic(IntrinsicDuration),
						Operands:  []Static{NewStaticDuration(100 * time.Millisecond)},
					},
					{
						Op:        OpGreaterEqual,
						Attribute: NewIntrinsic(IntrinsicDuration),
						Operands:  []Static{NewStaticDuration(250 * time.Millisecond)},
					},
				},
				AllConditions: true,
			},
		},
		{
			name: "Binary operation",
			searchRequest: &tempopb.SearchRequest{
				Query: `{ .cluster = "prod" }`,
			},
			expected: FetchSpansRequest{
				Conditions: []Condition{
					{
						Op:        OpEqual,
						Attribute: NewAttribute("cluster"),
						Operands:  []Static{NewStaticString("prod")},
					},
				},
				AllConditions: true,
			},
		},
		{
			name: "Invalid binary operation - missing spaces around operator",
			// If a space is missing around the operator, it will be treated as an attribute name
			searchRequest: &tempopb.SearchRequest{
				Query: `{ .cluster="prod" }`,
			},
			err: `span set filter has unexpected expression: traceql.Attribute (.cluster="prod")`,
		},
		{
			name: "Multiple && binary operations",
			searchRequest: &tempopb.SearchRequest{
				Query: `{ .host.name = "http://foo.com" && span.message != "bar" && resource.cluster = "prod" }`,
			},
			expected: FetchSpansRequest{
				Conditions: []Condition{
					{
						Op:        OpEqual,
						Attribute: NewAttribute("host.name"),
						Operands:  []Static{NewStaticString("http://foo.com")},
					},
					{
						Op:        OpNotEqual,
						Attribute: NewScopedAttribute(AttributeScopeSpan, false, "message"),
						Operands:  []Static{NewStaticString("bar")},
					},
					{
						Op:        OpEqual,
						Attribute: NewScopedAttribute(AttributeScopeResource, false, "cluster"),
						Operands:  []Static{NewStaticString("prod")},
					},
				},
				AllConditions: true,
			},
		},
		{
			name: "Multiple binary operations",
			searchRequest: &tempopb.SearchRequest{
				Query: `{ .host.name = "http://foo.com" || .host.name = "http://bar.com" }`,
			},
			expected: FetchSpansRequest{
				Conditions: []Condition{
					newCondition(NewAttribute("host.name"), OpEqual, NewStaticString("http://foo.com")),
					newCondition(NewAttribute("host.name"), OpEqual, NewStaticString("http://bar.com")),
				},
				AllConditions: false,
			},
		},
		{
			name: "Pipeline expression - not yet supported",
			searchRequest: &tempopb.SearchRequest{
				Query: `{ .host.name = "http://foo.com" } | { .cluster = "prod" }`,
			},
			err: "a TraceQL query with a pipeline is not supported yet",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEngine()

			fetchSpansRequest, err := e.createFetchSpansRequest(context.Background(), tt.searchRequest)

			if tt.err != "" {
				assert.EqualError(t, err, tt.err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, fetchSpansRequest)
			}
		})
	}
}

func newCondition(attr Attribute, op Operator, operands ...Static) Condition {
	return Condition{
		Attribute: attr,
		Op:        op,
		Operands:  operands,
	}
}
