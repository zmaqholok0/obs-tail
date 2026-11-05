package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zmaqholok0/obs-tail/internal/guard"
	"github.com/zmaqholok0/obs-tail/internal/otlpjson"
	"github.com/zmaqholok0/obs-tail/internal/policy"
	"github.com/zmaqholok0/obs-tail/internal/tailsampler"
)

const (
	maxBodyBytes = 8 << 20
	batchSpans   = 512
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfgPath := flag.String("config", "config.json", "path to the JSON policy file")
	listen := flag.String("listen", ":4318", "OTLP/HTTP listen address")
	upstream := flag.String("upstream", "", "OTLP/HTTP JSON endpoint for kept traces; stdout when empty")
	highWater := flag.Float64("high-water", 0.8, "buffer fill fraction above which ingest sheds traces")
	shedPercent := flag.Int("shed-percent", 25, "percent of trace ids kept while above --high-water")
	flag.Parse()

	pol, err := policy.Load(*cfgPath)
	if err != nil {
		return err
	}
	sampler, err := tailsampler.New(pol, time.Now().UnixNano())
	if err != nil {
		return err
	}
	gate, err := guard.New(*highWater, *shedPercent)
	if err != nil {
		return err
	}
	fw := newForwarder(*upstream, batchSpans, 2*time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go fw.run(ctx)
	go func() {
		tick := time.NewTicker(time.Duration(pol.WaitMS) * time.Millisecond / 4)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				dispatch(sampler, fw)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", ingest(sampler, gate, pol.MaxTraces))
	mux.HandleFunc("GET /metrics", metrics(sampler, gate))
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("obs-tail listening on %s, upstream %q", *listen, *upstream)

	select {
	case <-ctx.Done():
		log.Print("signal received, flushing")
	case err = <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	dispatch(sampler, fw)
	fw.flush()
	return nil
}

// ingest accepts one OTLP/HTTP JSON request and buffers its spans. While the
// buffer is above the guard's high-water mark whole traces are shed here,
// before they can cost memory.
func ingest(sampler *tailsampler.Sampler, gate *guard.Guard, maxTraces int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		spans, err := otlpjson.Decode(bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		open := sampler.Stats().OpenTraces
		admitted := spans[:0]
		for _, sp := range spans {
			if gate.Allow(sp.TraceID, open, maxTraces) {
				admitted = append(admitted, sp)
			}
		}
		sampler.Add(admitted)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{}")
	}
}

func metrics(sampler *tailsampler.Sampler, gate *guard.Guard) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, sampler.Stats().String())
		shed, admitted := guardStats(gate)
		_, _ = fmt.Fprintf(w, "# HELP obs_tail_guard_traces_shed Traces shed at ingest under buffer pressure.\n")
		_, _ = fmt.Fprintf(w, "# TYPE obs_tail_guard_traces_shed counter\nobs_tail_guard_traces_shed %d\n", shed)
		_, _ = fmt.Fprintf(w, "# HELP obs_tail_guard_traces_admitted Traces admitted by the guard.\n")
		_, _ = fmt.Fprintf(w, "# TYPE obs_tail_guard_traces_admitted counter\nobs_tail_guard_traces_admitted %d\n", admitted)
	}
}

func guardStats(gate *guard.Guard) (shed, admitted uint64) {
	admitted, shed = gate.Stats()
	return shed, admitted
}

func dispatch(sampler *tailsampler.Sampler, fw *forwarder) {
	for _, outcome := range sampler.Flush() {
		if outcome.Kept {
			fw.add(outcome.Spans)
		}
	}
}

// forwarder batches kept spans so an upstream sees one request per flush
// instead of one per trace.
type forwarder struct {
	url      string
	max      int
	interval time.Duration
	client   *http.Client

	mu    sync.Mutex
	buf   []otlpjson.Span
	outMu sync.Mutex
}

func newForwarder(url string, max int, interval time.Duration) *forwarder {
	return &forwarder{
		url:      url,
		max:      max,
		interval: interval,
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (f *forwarder) add(spans []otlpjson.Span) {
	f.mu.Lock()
	f.buf = append(f.buf, spans...)
	full := len(f.buf) >= f.max
	f.mu.Unlock()
	if full {
		f.flush()
	}
}

func (f *forwarder) run(ctx context.Context) {
	tick := time.NewTicker(f.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			f.flush()
		}
	}
}

func (f *forwarder) flush() {
	f.mu.Lock()
	if len(f.buf) == 0 {
		f.mu.Unlock()
		return
	}
	batch := f.buf
	f.buf = nil
	f.mu.Unlock()

	var payload bytes.Buffer
	if err := otlpjson.Encode(&payload, batch); err != nil {
		log.Printf("encode batch: %v", err)
		return
	}
	if f.url == "" {
		f.outMu.Lock()
		_, _ = os.Stdout.Write(payload.Bytes())
		f.outMu.Unlock()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, &payload)
	if err != nil {
		log.Printf("build forward request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req)
	if err != nil {
		log.Printf("forward %d spans: %v", len(batch), err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		log.Printf("upstream returned %s for %d spans", resp.Status, len(batch))
	}
}
