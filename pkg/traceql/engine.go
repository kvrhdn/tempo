package traceql

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/util"
)

type Engine struct {
}

func NewEngine() *Engine {
	return &Engine{}
}

func (e *Engine) Execute(ctx context.Context, searchReq *tempopb.SearchRequest, spanSetFetcher SpansetFetcher) (*tempopb.SearchResponse, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "traceql.Engine.Execute")
	defer span.Finish()

	fetchSpansRequest, err := e.createFetchSpansRequest(ctx, searchReq)
	if err != nil {
		return nil, err
	}

	span.SetTag("fetchSpansRequest", fetchSpansRequest)

	fetchSpansResponse, err := spanSetFetcher.Fetch(ctx, fetchSpansRequest)
	if err != nil {
		return nil, err
	}
	iterator := fetchSpansResponse.Results

	res := &tempopb.SearchResponse{
		Traces: nil,
		// TODO capture and update metrics
		Metrics: &tempopb.SearchMetrics{},
	}

	for len(res.Traces) <= int(searchReq.Limit) {
		spanSet, err := iterator.Next(ctx)
		span.LogKV("msg", "iterator.Next", "spanSet", spanSet, "err", err)
		if err != nil {
			return nil, err
		}

		if spanSet == nil {
			break
		}

		if !e.testSpanSet(searchReq, spanSet) {
			continue
		}

		res.Traces = append(res.Traces, e.asTraceSearchMetadata(spanSet))
	}

	span.SetTag("traces found", len(res.Traces))

	return res, nil
}

// createFetchSpansRequest will flatten the search request in simple conditions
// the storage layer can work with.
func (e *Engine) createFetchSpansRequest(ctx context.Context, searchReq *tempopb.SearchRequest) (req FetchSpansRequest, err error) {
	req.StartTimeUnixNanos = unixMilliToNano(searchReq.Start)
	req.EndTimeUnixNanos = unixMilliToNano(searchReq.End)
	req.AllConditions = true

	// TODO duration conditions can also be set from traceQL, we should filter duplicate conditions out
	if searchReq.MinDurationMs > 0 {
		req.appendCondition(Condition{
			Attribute: NewIntrinsic(IntrinsicDuration),
			Op:        OpGreaterEqual,
			Operands:  []Static{NewStaticDuration(time.Duration(searchReq.MinDurationMs) * time.Millisecond)},
		})
	}

	if searchReq.MaxDurationMs > 0 {
		req.appendCondition(Condition{
			Attribute: NewIntrinsic(IntrinsicDuration),
			Op:        OpLess,
			Operands:  []Static{NewStaticDuration(time.Duration(searchReq.MaxDurationMs) * time.Millisecond)},
		})
	}

	// TODO parsing "{}" returns an error, this is a hacky solution but will fail on other valid queries like "{ }"
	if searchReq.Query == "{}" {
		return req, nil
	}

	// Parse TraceQL query
	ast, err := Parse(searchReq.Query)
	if err != nil {
		return FetchSpansRequest{}, err
	}

	if len(ast.Pipeline.Elements) != 1 {
		return FetchSpansRequest{}, fmt.Errorf("a TraceQL query with a pipeline is not supported yet")
	}

	element := ast.Pipeline.Elements[0]

	c, allMustMatch, err := extractConditionsFromElement(element)
	if err != nil {
		return FetchSpansRequest{}, err
	}
	req.appendCondition(c...)
	req.AllConditions = allMustMatch

	return req, nil
}

func extractConditionsFromElement(element Element) ([]Condition, bool, error) {
	switch element.(type) {
	case SpansetFilter:
		return extractConditionsFromSpanSetFilter(element.(SpansetFilter))
	default:
		return nil, false, fmt.Errorf("element is of unsupported type: %T (%v)", element, element)
	}
}

func extractConditionsFromSpanSetFilter(filter SpansetFilter) ([]Condition, bool, error) {
	switch filter.Expression.(type) {
	case BinaryOperation:
		return extractConditionsFromBinaryOperation(filter.Expression.(BinaryOperation))
	default:
		return nil, false, fmt.Errorf("span set filter has unexpected expression: %T (%v)", filter.Expression, filter.Expression)
	}
}

func extractConditionsFromBinaryOperation(op BinaryOperation) (conditions []Condition, allMustMatch bool, err error) {
	allMustMatch = true

	switch op.Op {
	case OpOr:
		allMustMatch = false
		fallthrough
	case OpAnd:
		binaryOperation, ok := op.LHS.(BinaryOperation)
		if !ok {
			return nil, false, fmt.Errorf("expected LHS to have type BinaryOperation, got %T (%v)", op.LHS, op.LHS)
		}

		c, b, err := extractConditionsFromBinaryOperation(binaryOperation)
		if err != nil {
			return nil, false, err
		}
		allMustMatch = allMustMatch && b
		conditions = append(conditions, c...)

		binaryOperation, ok = op.RHS.(BinaryOperation)
		if !ok {
			return nil, false, fmt.Errorf("expected RHS to have type BinaryOperation, got %T (%v)", op.RHS, op.RHS)
		}

		c, b, err = extractConditionsFromBinaryOperation(binaryOperation)
		if err != nil {
			return nil, false, err
		}
		allMustMatch = allMustMatch && b
		conditions = append(conditions, c...)

	case OpEqual:
		fallthrough
	case OpNotEqual:
		fallthrough
	case OpGreater:
		fallthrough
	case OpGreaterEqual:
		fallthrough
	case OpLess:
		fallthrough
	case OpLessEqual:
		fallthrough
	case OpRegex:
		fallthrough
	case OpNotRegex:
		// TODO we need to be more flexible here and can't assume LHS is always an Attribute and RHS a Static

		attribute, ok := op.LHS.(Attribute)
		if !ok {
			return nil, false, fmt.Errorf("LHS: expected an attribute but got %T", op.LHS)
		}

		static, ok := op.RHS.(Static)
		if !ok {
			return nil, false, fmt.Errorf("RHS: expected a static but got %T", op.RHS)
		}

		conditions = append(conditions, Condition{
			Op:        op.Op,
			Attribute: attribute,
			Operands:  []Static{static},
		})

	default:
		return nil, false, fmt.Errorf("binary operation has an unsupported operation: %v", op.Op)
	}

	return
}

// testSpanSet will validate the received span set fulfills the search query.
func (e *Engine) testSpanSet(searchReq *tempopb.SearchRequest, spanSet *Spanset) bool {
	// TODO push the spanset though the original query to ensure it also mathes the original query
	return true
}

func (e *Engine) asTraceSearchMetadata(spanset *Spanset) *tempopb.TraceSearchMetadata {
	metadata := &tempopb.TraceSearchMetadata{
		TraceID:           util.TraceIDToHexString(spanset.TraceID),
		RootServiceName:   "", // TODO can we capture the root service name?
		RootTraceName:     "", // TODO can we capture the root trace name?
		StartTimeUnixNano: math.MaxUint64,
		DurationMs:        0,
	}

	for _, span := range spanset.Spans {
		if span.StartTimeUnixNanos < metadata.StartTimeUnixNano {
			metadata.StartTimeUnixNano = span.StartTimeUnixNanos
		}

		newDurationMs := uint32((span.EndtimeUnixNanos - metadata.StartTimeUnixNano) / 1000)
		if newDurationMs > metadata.DurationMs {
			metadata.DurationMs = newDurationMs
		}

		tempopbSpan := &tempopb.Span{
			SpanId:            span.ID,
			StartTimeUnixNano: span.StartTimeUnixNanos,
			EndTimeUnixNano:   span.EndtimeUnixNanos,
			Attributes:        nil,
		}
		for attribute, static := range span.Attributes {
			tempopbSpan.Attributes = append(tempopbSpan.Attributes, &v1.KeyValue{
				Key: attribute.Name,
				// TODO proper mapping of type
				Value: &v1.AnyValue{
					Value: &v1.AnyValue_StringValue{StringValue: static.String()},
				},
			})
		}

		metadata.Spans = append(metadata.Spans, tempopbSpan)
	}

	return metadata
}

func unixMilliToNano(ts uint32) uint64 {
	return uint64(ts) * 1000
}
