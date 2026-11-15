package metrics

import (
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("sluice_requests_total", "Requests.", "api", "Produce")
	c.Add(3)
	r.GaugeFunc("sluice_isr_size", "ISR size.", func() float64 { return 2 }, "partition", "t-0")
	h := r.Histogram("sluice_fsync_seconds", "Fsync latency.", []float64{0.001, 0.01})
	h.Observe(0.0005)
	h.Observe(0.005)
	h.Observe(1)

	var b strings.Builder
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		`# TYPE sluice_requests_total counter`,
		`sluice_requests_total{api="Produce"} 3`,
		`sluice_isr_size{partition="t-0"} 2`,
		`sluice_fsync_seconds_bucket{le="0.001"} 1`,
		`sluice_fsync_seconds_bucket{le="0.01"} 2`,
		`sluice_fsync_seconds_bucket{le="+Inf"} 3`,
		`sluice_fsync_seconds_count 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if h.Count() != 3 || h.Sum() < 1.005 || h.Sum() > 1.0056 {
		t.Fatalf("count %d sum %v", h.Count(), h.Sum())
	}

	r.Remove("sluice_isr_size", `"t-0"`)
	b.Reset()
	r.Write(&b)
	if strings.Contains(b.String(), "sluice_isr_size") {
		t.Fatal("removed series still exported")
	}
}
