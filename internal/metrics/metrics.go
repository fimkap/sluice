// Package metrics is a minimal Prometheus text-format exporter: counters, gauge functions and fixed-bucket
// histograms. It exists so the broker needs no dependency for /metrics.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metric families and writes them in the Prometheus text exposition format.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
}

type family struct {
	name, help, kind string
	series           []series
}

type series interface {
	labels() string
	write(w io.Writer, name string)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{families: map[string]*family{}} }

func (r *Registry) add(name, help, kind string, s series) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.families[name]
	if !ok {
		f = &family{name: name, help: help, kind: kind}
		r.families[name] = f
	}
	for i, old := range f.series {
		if old.labels() == s.labels() { // re-registration replaces (e.g. a recreated partition)
			f.series[i] = s
			return
		}
	}
	f.series = append(f.series, s)
}

// Remove drops every series of family name whose labels contain substr.
func (r *Registry) Remove(name, substr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.families[name]
	if !ok {
		return
	}
	kept := f.series[:0]
	for _, s := range f.series {
		if !strings.Contains(s.labels(), substr) {
			kept = append(kept, s)
		}
	}
	f.series = kept
}

// Labels formats label pairs ("k1", "v1", "k2", "v2") as {k1="v1",k2="v2"}.
func Labels(kv ...string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", kv[i], kv[i+1])
	}
	b.WriteByte('}')
	return b.String()
}

// Counter is a monotonically increasing count.
type Counter struct {
	lbl string
	v   atomic.Uint64
}

// Counter registers and returns a counter. kv are label pairs.
func (r *Registry) Counter(name, help string, kv ...string) *Counter {
	c := &Counter{lbl: Labels(kv...)}
	r.add(name, help, "counter", c)
	return c
}

func (c *Counter) Add(n uint64)   { c.v.Add(n) }
func (c *Counter) Inc()           { c.v.Add(1) }
func (c *Counter) Value() uint64  { return c.v.Load() }
func (c *Counter) labels() string { return c.lbl }
func (c *Counter) write(w io.Writer, name string) {
	fmt.Fprintf(w, "%s%s %d\n", name, c.lbl, c.v.Load())
}

type gaugeFunc struct {
	lbl string
	fn  func() float64
}

// GaugeFunc registers a gauge whose value is computed at scrape time.
func (r *Registry) GaugeFunc(name, help string, fn func() float64, kv ...string) {
	r.add(name, help, "gauge", &gaugeFunc{lbl: Labels(kv...), fn: fn})
}

func (g *gaugeFunc) labels() string { return g.lbl }
func (g *gaugeFunc) write(w io.Writer, name string) {
	fmt.Fprintf(w, "%s%s %s\n", name, g.lbl, formatFloat(g.fn()))
}

// Histogram counts observations into fixed buckets.
type Histogram struct {
	lbl     string
	bounds  []float64
	counts  []atomic.Uint64 // one per bound, plus +Inf
	sumBits atomic.Uint64
	count   atomic.Uint64
}

// Histogram registers a histogram with the given upper bounds (ascending).
func (r *Registry) Histogram(name, help string, bounds []float64, kv ...string) *Histogram {
	h := &Histogram{lbl: Labels(kv...), bounds: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
	r.add(name, help, "histogram", h)
	return h
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v)
	h.counts[i].Add(1)
	h.count.Add(1)
	for {
		old := h.sumBits.Load()
		if h.sumBits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

// Count is the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Sum is the sum of all observations.
func (h *Histogram) Sum() float64 { return math.Float64frombits(h.sumBits.Load()) }

func (h *Histogram) labels() string { return h.lbl }

func (h *Histogram) write(w io.Writer, name string) {
	inner := strings.TrimSuffix(strings.TrimPrefix(h.lbl, "{"), "}")
	sep := ""
	if inner != "" {
		sep = ","
	}
	var cum uint64
	for i, b := range h.bounds {
		cum += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{%s%sle=%q} %d\n", name, inner, sep, formatFloat(b), cum)
	}
	cum += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket{%s%sle=\"+Inf\"} %d\n", name, inner, sep, cum)
	fmt.Fprintf(w, "%s_sum%s %s\n", name, h.lbl, formatFloat(h.Sum()))
	fmt.Fprintf(w, "%s_count%s %d\n", name, h.lbl, h.count.Load())
}

// Write writes every family, sorted by name.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	names := make([]string, 0, len(r.families))
	for n := range r.families {
		names = append(names, n)
	}
	sort.Strings(names)
	fams := make([]family, 0, len(names))
	for _, n := range names {
		f := r.families[n]
		fams = append(fams, family{name: f.name, help: f.help, kind: f.kind, series: append([]series(nil), f.series...)})
	}
	r.mu.Unlock()
	for _, f := range fams {
		if len(f.series) == 0 {
			continue
		}
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind)
		for _, s := range f.series {
			s.write(w, f.name)
		}
	}
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

// ExpBuckets returns n bounds starting at start, each factor times the previous one.
func ExpBuckets(start, factor float64, n int) []float64 {
	b := make([]float64, n)
	for i := range b {
		b[i] = start
		start *= factor
	}
	return b
}
