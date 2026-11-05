package tailsampler

import (
	"strconv"
	"testing"
	"time"

	"github.com/zmaqholok0/obs-tail/internal/otlpjson"
	"github.com/zmaqholok0/obs-tail/internal/policy"
)

var base = time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)

func span(traceID, service string, offset, dur time.Duration, code int) otlpjson.Span {
	return otlpjson.Span{
		TraceID:           traceID,
		SpanID:            "0123456789abcdef",
		Name:              "GET /pay",
		Service:           service,
		StartTimeUnixNano: strconv.FormatInt(base.Add(offset).UnixNano(), 10),
		EndTimeUnixNano:   strconv.FormatInt(base.Add(offset+dur).UnixNano(), 10),
		Status:            otlpjson.Status{Code: code},
	}
}

func newSampler(t *testing.T, rules ...policy.Rule) *Sampler {
	t.Helper()
	s, err := New(policy.Policy{WaitMS: 500, MaxTraces: 100, Rules: rules}, 7)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s.WithClock(func() time.Time { return base })
}

func TestErrorAndLatencyAbstainSoFallbackDecides(t *testing.T) {
	s := newSampler(t,
		policy.Rule{Name: "errors", Kind: policy.KindError, Rate: 1},
		policy.Rule{Name: "slow", Kind: policy.KindLatency, ThresholdMS: 500},
		policy.Rule{Name: "rest", Kind: policy.KindProbabilistic, Rate: 1},
	)
	s.Add([]otlpjson.Span{
		span("fast-ok", "pricing", 0, 10*time.Millisecond, 0),
		span("fast-err", "pricing", 0, 10*time.Millisecond, otlpjson.StatusCodeError),
		span("slow-ok", "pricing", 0, 900*time.Millisecond, 0),
	})

	s.WithClock(func() time.Time { return base.Add(time.Second) })
	out := s.Flush()
	if len(out) != 3 {
		t.Fatalf("decided %d traces, want 3", len(out))
	}
	reasons := map[string]string{}
	for _, o := range out {
		reasons[o.TraceID] = o.Reason
		if !o.Kept {
			t.Errorf("trace %s dropped, want kept", o.TraceID)
		}
	}
	if reasons["fast-ok"] != "rest" {
		t.Errorf("fast-ok reason = %q, want rest", reasons["fast-ok"])
	}
	if reasons["fast-err"] != "errors" || reasons["slow-ok"] != "slow" {
		t.Errorf("reasons = %v, want errors/slow", reasons)
	}
}

func TestNoMatchingRuleDrops(t *testing.T) {
	s := newSampler(t, policy.Rule{
		Name: "checkout-only", Kind: policy.KindProbabilistic, Rate: 1,
		ServicePattern: "checkout-*",
	})
	s.Add([]otlpjson.Span{span("t1", "pricing", 0, time.Millisecond, 0)})
	s.WithClock(func() time.Time { return base.Add(time.Second) })

	out := s.Flush()
	if len(out) != 1 || out[0].Kept || out[0].Reason != "no-match" {
		t.Fatalf("got %+v, want one dropped no-match trace", out)
	}
}

func TestFlushWaitsForTraceAge(t *testing.T) {
	s := newSampler(t, policy.Rule{Name: "all", Kind: policy.KindProbabilistic, Rate: 1})
	s.Add([]otlpjson.Span{span("t1", "pricing", 0, time.Millisecond, 0)})

	s.WithClock(func() time.Time { return base.Add(100 * time.Millisecond) })
	if out := s.Flush(); len(out) != 0 {
		t.Fatalf("flushed %d traces before wait elapsed", len(out))
	}
	s.WithClock(func() time.Time { return base.Add(time.Second) })
	if out := s.Flush(); len(out) != 1 {
		t.Fatalf("flushed %d traces after wait, want 1", len(out))
	}
}

func TestOverflowEvictsOldestAndCounts(t *testing.T) {
	s, err := New(policy.Policy{
		WaitMS: 60000, MaxTraces: 100,
		Rules: []policy.Rule{{Name: "all", Kind: policy.KindProbabilistic, Rate: 1}},
	}, 7)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.WithClock(func() time.Time { return base })

	for i := range 150 {
		s.Add([]otlpjson.Span{span("trace-"+strconv.Itoa(i), "pricing", 0, time.Millisecond, 0)})
	}
	stats := s.Stats()
	if stats.TracesOverflow != 50 {
		t.Errorf("overflow = %d, want 50", stats.TracesOverflow)
	}
	if stats.OpenTraces != 100 {
		t.Errorf("open = %d, want buffer capped at 100", stats.OpenTraces)
	}
}
