# obs-tail

Tail-sampling proxy for OTLP traces. It buffers a trace until all of its spans
have arrived, then runs an ordered policy on the finished trace: keep errors,
keep slow endpoints, sample the rest deterministically.

Head sampling at the SDK cannot see errors or latency - the decision happens
before the request finishes. This proxy can, because it sees the whole trace.
It is deliberately the smallest thing that does that job: no span rewriting, no
storage, no UI.

## Why

Two failure modes eat tracing budgets:

1. Errors are only visible at the end of a trace, so a head sampler keeps them
   at the same rate as happy-path traffic.
2. Producers happily emit the same rare trace at 1:1 forever because the SDK
   has no idea what it costs.

`obs-tail` moves the decision to the collector edge, where the trace is
complete and the policy is reviewable in a code review.

## Install

```
go build ./cmd/obs-tail
cp config.example.json config.json
```

No third-party dependencies. Go 1.22+ because `math/rand/v2` and the uppercase
route patterns (`POST /v1/traces`) are used.

## Usage

Point a collector's `otlphttp` exporter at it and point it at your backend:

```
otelcol --config collector.yaml   # exporter endpoint: http://obs-tail:4318
./obs-tail --config config.json --upstream http://tempo:4318/v1/traces
```

Send a batch by hand:

```
curl -sS -X POST http://localhost:4318/v1/traces \
  -H 'content-type: application/json' --data @sample-trace.json
```

Without `--upstream` the kept traces are written to stdout as OTLP JSON, which
makes `--upstream` optional for a quick look at what the policy keeps.

`GET /metrics` exposes accepted spans, decisions, overflow evictions and the
current buffer size. Overflow is the number to alert on: it means the buffer
hit `max_traces` and the oldest undecided trace was discarded.

### Policy

Rules are keep conditions evaluated in priority order, and the **first rule
that fires decides** - a trace an earlier rule kept never reaches a later one.
`error` and `latency` rules abstain when the trace does not qualify, so they
cost nothing for happy-path traffic. `probabilistic` always decides and belongs
last; without it every trace no rule keeps is dropped.

| kind            | keeps when                                  |
|-----------------|---------------------------------------------|
| `error`         | the trace has a failed span, at `rate`      |
| `latency`       | the trace is slower than `threshold_ms`     |
| `probabilistic` | always, at `rate`                           |

`rate` on an `error` rule is the fraction of *error traces* kept, so `1.0`
keeps all of them.

`service_pattern` is a `path.Match` glob over `service.name`; omit it to match
every service. Put the specific rules first and one probabilistic rule last:

```json
{
  "wait_ms": 3000,
  "max_traces": 20000,
  "rules": [
    { "name": "errors-are-kept", "kind": "error", "rate": 1.0 },
    { "name": "slow-checkout", "kind": "latency", "threshold_ms": 800,
      "service_pattern": "checkout-*" },
    { "name": "tail-of-everything", "kind": "probabilistic", "rate": 0.05 }
  ]
}
```

### Overflow guard

Hard limits are worse than slow degradation: a full buffer drops the oldest
undecided trace, which is usually the one being debugged. The guard keeps the
buffer off that wall. Once open traces pass `--high-water` of `max_traces`, new
traces are shed at ingest by trace-id hash, keeping `--shed-percent` of them.
The hash is sticky, so a retried trace is never half-buffered.

```
./obs-tail --config config.json --high-water 0.8 --shed-percent 25
```

`obs_tail_guard_traces_shed` moving off zero means the buffer is undersized,
the rules keep too much, or `wait_ms` lets traces pile up.

### Cost notes

- `wait_ms` trades completeness for memory: a trace whose spans straddle the
  wait is decided on the spans that made it, and late spans are dropped. Keep
  it above the p99 span gap of the services you sample, not below.
- `max_traces` bounds memory at roughly `max_traces * spans * ~300 bytes` of
  decoded heap. 20k traces of 20 spans is a few hundred MB; size the pod for it.
- Rate `1.0` is fine for errors on low-traffic services and ruinous on a noisy
  one. Start at 0.3 and lower it when the error budget outgrows the backend.
- A probabilistic rule at 5% of 100k spans/s still writes 5k spans/s. Tail
  sampling lowers volume, it does not make an oversized pipeline free.

## Layout

```
cmd/obs-tail/        flags, HTTP ingest, metrics, forwarding
internal/otlpjson/   OTLP/HTTP JSON decode, re-encode, timestamp helpers
internal/policy/     JSON rules, pattern matching, validation
internal/tailsampler/ trace buffer, eviction, decision engine, counters
```

`internal/tailsampler` takes a clock so the wait logic is testable without
sleeping.

## Limits

- JSON over OTLP/HTTP only; protobuf and gRPC are not implemented.
- `traceId` is the only correlation key; unsampled trace state is not stored.
- No persistence: a restart loses buffered traces.

## License

MIT, see [LICENSE](LICENSE).
