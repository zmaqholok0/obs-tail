// Package policy holds the tail-sampling configuration and its validation.
//
// A policy is an ordered rule list. The first rule whose service pattern
// matches a trace decides that trace: either it is kept or dropped, and
// nothing further is evaluated. Rules are intentionally short so that a
// reader can explain why any given trace survived.
package policy

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"
)

// Kind enumerates the supported decision rules.
type Kind string

const (
	// KindError keeps a fraction of traces that carry at least one failed span.
	KindError Kind = "error"
	// KindLatency keeps traces slower than ThresholdMS.
	KindLatency Kind = "latency"
	// KindProbabilistic keeps a random fraction of traces.
	KindProbabilistic Kind = "probabilistic"
)

// Rule is one ordered decision.
type Rule struct {
	Name string `json:"name"`
	Kind Kind   `json:"kind"`
	// ThresholdMS is the wall-clock threshold for KindLatency.
	ThresholdMS float64 `json:"threshold_ms,omitempty"`
	// Rate is the keep probability for KindError and KindProbabilistic.
	Rate float64 `json:"rate,omitempty"`
	// ServicePattern is a path.Match pattern over service.name; empty means all.
	ServicePattern string `json:"service_pattern,omitempty"`
}

// Matches reports whether the rule applies to service.
func (r Rule) Matches(service string) bool {
	if r.ServicePattern == "" {
		return true
	}
	ok, err := path.Match(r.ServicePattern, service)
	return err == nil && ok
}

// Threshold returns ThresholdMS as a duration.
func (r Rule) Threshold() time.Duration {
	return time.Duration(r.ThresholdMS * float64(time.Millisecond))
}

// Policy is the whole tail-sampling configuration.
type Policy struct {
	// WaitMS is how long a trace is buffered before the rules run.
	WaitMS int `json:"wait_ms"`
	// MaxTraces bounds memory; the oldest open trace is evicted first.
	MaxTraces int    `json:"max_traces"`
	Rules     []Rule `json:"rules"`
}

// Load reads a JSON policy file.
func Load(file string) (Policy, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return Policy{}, err
	}
	var p Policy
	dec := json.NewDecoder(bytesReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return Policy{}, fmt.Errorf("parse %s: %w", file, err)
	}
	return p, p.Validate()
}

// Validate rejects configurations that would silently keep or drop everything.
func (p Policy) Validate() error {
	if p.WaitMS < 100 || p.WaitMS > 60000 {
		return fmt.Errorf("wait_ms %d outside 100..60000", p.WaitMS)
	}
	if p.MaxTraces < 100 {
		return fmt.Errorf("max_traces %d below 100", p.MaxTraces)
	}
	if len(p.Rules) == 0 {
		return fmt.Errorf("policy has no rules")
	}
	for i, r := range p.Rules {
		if r.Name == "" {
			return fmt.Errorf("rule %d has no name", i)
		}
		if _, err := path.Match(r.ServicePattern, "svc"); err != nil && r.ServicePattern != "" {
			return fmt.Errorf("rule %q: bad service_pattern: %w", r.Name, err)
		}
		switch r.Kind {
		case KindError, KindProbabilistic:
			if r.Rate <= 0 || r.Rate > 1 {
				return fmt.Errorf("rule %q: rate %v outside (0,1]", r.Name, r.Rate)
			}
		case KindLatency:
			if r.ThresholdMS <= 0 {
				return fmt.Errorf("rule %q: threshold_ms must be positive", r.Name)
			}
		default:
			return fmt.Errorf("rule %q: unknown kind %q", r.Name, r.Kind)
		}
	}
	return nil
}
