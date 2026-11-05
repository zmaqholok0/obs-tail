// Package otlpjson decodes and re-encodes the JSON flavour of OTLP/HTTP
// trace payloads. Only the fields the sampler needs are kept: identity,
// timing, status and service.name.
package otlpjson

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

// StatusCodeError is the OTLP status code for a failed span.
const StatusCodeError = 2

// Attribute is one key/value pair.
type Attribute struct {
	Key   string `json:"key"`
	Value Value  `json:"value"`
}

// Value carries the string form of an attribute value.
type Value struct {
	StringValue string `json:"stringValue,omitempty"`
	IntValue    string `json:"intValue,omitempty"`
	BoolValue   bool   `json:"boolValue,omitempty"`
}

// Span is a flattened span with its resource's service.name resolved.
type Span struct {
	TraceID           string      `json:"traceId"`
	SpanID            string      `json:"spanId"`
	ParentSpanID      string      `json:"parentSpanId,omitempty"`
	Name              string      `json:"name"`
	StartTimeUnixNano string      `json:"startTimeUnixNano"`
	EndTimeUnixNano   string      `json:"endTimeUnixNano"`
	Status            Status      `json:"status"`
	Attributes        []Attribute `json:"attributes,omitempty"`
	Service           string      `json:"-"`
}

// Status is the OTLP span status.
type Status struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Start parses the span start timestamp.
func (s Span) Start() time.Time { return parseNano(s.StartTimeUnixNano) }

// End parses the span end timestamp.
func (s Span) End() time.Time { return parseNano(s.EndTimeUnixNano) }

// Failed reports whether the span was reported as an error.
func (s Span) Failed() bool { return s.Status.Code == StatusCodeError }

func parseNano(v string) time.Time {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

type exportRequest struct {
	ResourceSpans []resourceSpans `json:"resourceSpans"`
}

type resourceSpans struct {
	Resource   resource     `json:"resource"`
	ScopeSpans []scopeSpans `json:"scopeSpans"`
}

type resource struct {
	Attributes []Attribute `json:"attributes"`
}

type scopeSpans struct {
	Spans []Span `json:"spans"`
}

// Decode reads one ExportTraceServiceRequest and returns its spans with
// Service filled in. Spans without a traceId are skipped.
func Decode(r io.Reader) ([]Span, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty body")
	}
	var req exportRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode OTLP JSON: %w", err)
	}
	var out []Span
	for _, rs := range req.ResourceSpans {
		service := serviceName(rs.Resource.Attributes)
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				if sp.TraceID == "" {
					continue
				}
				sp.Service = service
				out = append(out, sp)
			}
		}
	}
	return out, nil
}

func serviceName(attrs []Attribute) string {
	for _, a := range attrs {
		if a.Key == "service.name" {
			return a.Value.StringValue
		}
	}
	return "unknown_service"
}

// Encode writes spans back as an OTLP/HTTP JSON request, grouped by service.
func Encode(w io.Writer, spans []Span) error {
	groups := map[string][]Span{}
	var order []string
	for _, sp := range spans {
		if _, seen := groups[sp.Service]; !seen {
			order = append(order, sp.Service)
		}
		groups[sp.Service] = append(groups[sp.Service], sp)
	}
	req := exportRequest{ResourceSpans: make([]resourceSpans, 0, len(order))}
	for _, service := range order {
		req.ResourceSpans = append(req.ResourceSpans, resourceSpans{
			Resource: resource{Attributes: []Attribute{{
				Key:   "service.name",
				Value: Value{StringValue: service},
			}}},
			ScopeSpans: []scopeSpans{{Spans: groups[service]}},
		})
	}
	return json.NewEncoder(w).Encode(req)
}
