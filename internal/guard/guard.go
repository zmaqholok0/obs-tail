// Package guard makes the buffer's limit boring: before a trace can fill the
// buffer it is shed at ingest, by trace-id hash, once the buffer is above a
// high-water mark.
//
// Shedding a whole trace instead of individual spans keeps a trace coherent:
// either every span of it is buffered or none is. The hash makes the decision
// sticky per trace, so retries of the same trace get the same answer.
package guard

import (
	"fmt"
	"hash/fnv"
	"sync/atomic"
)

// Guard is a pressure-based admission gate.
type Guard struct {
	highWater  float64
	keepPermil uint32

	shed  atomic.Uint64
	admit atomic.Uint64
}

// New returns a guard that lets everything through below highWater (a
// fraction of the buffer limit) and keeps keepPercent of trace ids above it.
func New(highWater float64, keepPercent int) (*Guard, error) {
	if highWater <= 0 || highWater > 1 {
		return nil, fmt.Errorf("high water %v outside (0,1]", highWater)
	}
	if keepPercent < 1 || keepPercent > 100 {
		return nil, fmt.Errorf("shed keep percent %d outside 1..100", keepPercent)
	}
	return &Guard{highWater: highWater, keepPermil: uint32(keepPercent)}, nil
}

// Allow reports whether the trace may be buffered, given open and max traces.
func (g *Guard) Allow(traceID string, open, max int) bool {
	if max <= 0 || float64(open) < g.highWater*float64(max) {
		g.admit.Add(1)
		return true
	}
	if g.hash(traceID) < g.keepPermil {
		g.admit.Add(1)
		return true
	}
	g.shed.Add(1)
	return false
}

func (g *Guard) hash(traceID string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(traceID))
	return h.Sum32() % 100
}

// Stats returns admitted and shed trace counts.
func (g *Guard) Stats() (admitted, shed uint64) {
	return g.admit.Load(), g.shed.Load()
}

// HighWater returns the configured threshold as a fraction.
func (g *Guard) HighWater() float64 { return g.highWater }

// KeepPercent returns the configured keep percentage above the threshold.
func (g *Guard) KeepPercent() int { return int(g.keepPermil) }
