// Package tailsampler buffers whole traces and decides once the trace has
// aged out, so a decision sees every span of the trace.
//
// Rules are keep conditions in priority order. Error and latency rules abstain
// when their condition does not hold, so the final probabilistic rule is what
// catches everything else; without one, unmatched traces are dropped.
//
// Memory is bounded by policy.MaxTraces. When the buffer is full the oldest
// open trace is evicted undecided and counted as overflow, which is the
// cheapest honest failure: newer traces are usually the ones being debugged.
package tailsampler

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmaqholok0/obs-tail/internal/otlpjson"
	"github.com/zmaqholok0/obs-tail/internal/policy"
)

// Outcome is the decision for one trace, with the spans needed to forward it.
type Outcome struct {
	TraceID  string
	Service  string
	Spans    []otlpjson.Span
	Duration time.Duration
	SpanErrs int
	Kept     bool
	Reason   string
}

// Counters is a snapshot of sampler activity.
type Counters struct {
	SpansReceived  uint64
	TracesFinished uint64
	TracesKept     uint64
	TracesDropped  uint64
	TracesOverflow uint64
	OpenTraces     int
}

type trace struct {
	id       string
	service  string
	start    time.Time
	end      time.Time
	spans    []otlpjson.Span
	spanErrs int
}

func (t *trace) duration() time.Duration { return t.end.Sub(t.start) }

// Sampler owns the trace buffer. It is safe for concurrent use.
type Sampler struct {
	pol policy.Policy
	rnd *rand.Rand
	now func() time.Time

	mu     sync.Mutex
	traces map[string]*trace
	order  []string // insertion order, stale ids allowed
	dirty  int

	spansReceived  atomic.Uint64
	tracesFinished atomic.Uint64
	tracesKept     atomic.Uint64
	tracesDropped  atomic.Uint64
	tracesOverflow atomic.Uint64
}

// New validates pol and returns a ready sampler. seed makes decisions
// reproducible in tests.
func New(pol policy.Policy, seed int64) (*Sampler, error) {
	if err := pol.Validate(); err != nil {
		return nil, err
	}
	return &Sampler{
		pol:    pol,
		rnd:    rand.New(rand.NewPCG(uint64(seed), 0x9e3779b97f4a7c15)),
		now:    time.Now,
		traces: make(map[string]*trace),
	}, nil
}

// WithClock replaces the clock; used by tests.
func (s *Sampler) WithClock(now func() time.Time) *Sampler {
	s.now = now
	return s
}

// Add buffers spans grouped by trace id.
func (s *Sampler) Add(spans []otlpjson.Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sp := range spans {
		s.spansReceived.Add(1)
		t, ok := s.traces[sp.TraceID]
		if !ok {
			t = &trace{id: sp.TraceID, service: sp.Service, start: sp.Start(), end: sp.End()}
			s.traces[sp.TraceID] = t
			s.order = append(s.order, sp.TraceID)
			for len(s.traces) > s.pol.MaxTraces {
				s.evictOldestLocked()
			}
		}
		if end := sp.End(); end.After(t.end) {
			t.end = end
		}
		if sp.Start().Before(t.start) {
			t.start = sp.Start()
		}
		t.spans = append(t.spans, sp)
		if sp.Failed() {
			t.spanErrs++
		}
	}
}

func (s *Sampler) evictOldestLocked() {
	for len(s.order) > 0 {
		id := s.order[0]
		s.order = s.order[1:]
		if t, ok := s.traces[id]; ok {
			delete(s.traces, id)
			s.tracesOverflow.Add(1)
			_ = t
			return
		}
	}
}

// Flush decides every trace older than the configured wait and removes it.
func (s *Sampler) Flush() []Outcome {
	now := s.now()
	wait := time.Duration(s.pol.WaitMS) * time.Millisecond

	s.mu.Lock()
	due := make([]*trace, 0, len(s.traces))
	for id, t := range s.traces {
		if now.Sub(t.start) >= wait {
			due = append(due, t)
			delete(s.traces, id)
			s.dirty++
		}
	}
	s.compactLocked()
	s.mu.Unlock()

	out := make([]Outcome, 0, len(due))
	for _, t := range due {
		out = append(out, s.decide(t))
	}
	return out
}

// compactLocked drops stale ids once they dominate the order slice.
func (s *Sampler) compactLocked() {
	if s.dirty < 1024 || s.dirty < len(s.order)/2 {
		return
	}
	kept := s.order[:0]
	for _, id := range s.order {
		if _, ok := s.traces[id]; ok {
			kept = append(kept, id)
		}
	}
	s.order = kept
	s.dirty = 0
}

func (s *Sampler) decide(t *trace) Outcome {
	o := Outcome{
		TraceID:  t.id,
		Service:  t.service,
		Spans:    t.spans,
		Duration: t.duration(),
		SpanErrs: t.spanErrs,
	}
	s.tracesFinished.Add(1)
	reason, keep := s.apply(t, o.Duration)
	o.Kept = keep
	o.Reason = reason
	if o.Kept {
		s.tracesKept.Add(1)
	} else {
		s.tracesDropped.Add(1)
	}
	return o
}

// apply walks the rules in priority order. Error and latency rules that do not
// fire abstain; a probabilistic rule always decides, so it belongs last.
func (s *Sampler) apply(t *trace, d time.Duration) (reason string, keep bool) {
	for i := range s.pol.Rules {
		r := &s.pol.Rules[i]
		if !r.Matches(t.service) {
			continue
		}
		switch r.Kind {
		case policy.KindError:
			if t.spanErrs > 0 && s.roll(r.Rate) {
				return r.Name, true
			}
		case policy.KindLatency:
			if d >= r.Threshold() {
				return r.Name, true
			}
		case policy.KindProbabilistic:
			return r.Name, s.roll(r.Rate)
		}
	}
	return "no-match", false
}

func (s *Sampler) roll(rate float64) bool { return s.rnd.Float64() < rate }

// Stats returns a consistent-enough snapshot for the metrics endpoint.
func (s *Sampler) Stats() Counters {
	s.mu.Lock()
	open := len(s.traces)
	s.mu.Unlock()
	return Counters{
		SpansReceived:  s.spansReceived.Load(),
		TracesFinished: s.tracesFinished.Load(),
		TracesKept:     s.tracesKept.Load(),
		TracesDropped:  s.tracesDropped.Load(),
		TracesOverflow: s.tracesOverflow.Load(),
		OpenTraces:     open,
	}
}

// String renders counters in Prometheus text format.
func (c Counters) String() string {
	lines := []string{
		"# HELP obs_tail_spans_received Spans accepted since start.",
		"# TYPE obs_tail_spans_received counter",
		fmt.Sprintf("obs_tail_spans_received %d", c.SpansReceived),
		"# HELP obs_tail_traces_finished Traces that reached a decision.",
		"# TYPE obs_tail_traces_finished counter",
		fmt.Sprintf("obs_tail_traces_finished %d", c.TracesFinished),
		"# HELP obs_tail_traces_kept Traces forwarded upstream.",
		"# TYPE obs_tail_traces_kept counter",
		fmt.Sprintf("obs_tail_traces_kept %d", c.TracesKept),
		"# HELP obs_tail_traces_dropped Traces dropped by policy.",
		"# TYPE obs_tail_traces_dropped counter",
		fmt.Sprintf("obs_tail_traces_dropped %d", c.TracesDropped),
		"# HELP obs_tail_traces_overflow Open traces evicted before decision.",
		"# TYPE obs_tail_traces_overflow counter",
		fmt.Sprintf("obs_tail_traces_overflow %d", c.TracesOverflow),
		"# HELP obs_tail_open_traces Traces currently buffered.",
		"# TYPE obs_tail_open_traces gauge",
		fmt.Sprintf("obs_tail_open_traces %d", c.OpenTraces),
	}
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}
