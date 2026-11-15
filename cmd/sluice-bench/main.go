// Command sluice-bench runs the experiments from docs/design.md against brokers started in this process
// (real TCP on loopback, real files) and prints Markdown tables.
//
//	sluice-bench experiment all            # E1..E4, a few minutes
//	sluice-bench experiment durability -quick
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fimkap/sluice/internal/broker"
)

type env struct {
	dir   string
	quick bool
	out   io.Writer
}

func main() {
	if len(os.Args) < 3 || os.Args[1] != "experiment" {
		fmt.Fprintln(os.Stderr, "usage: sluice-bench experiment <durability|queues|replication|fetch|all> [-quick] [-dir DIR] [-o FILE]")
		os.Exit(2)
	}
	name := os.Args[2]
	fs := flag.NewFlagSet("experiment", flag.ExitOnError)
	quick := fs.Bool("quick", false, "shorter runs (for CI and smoke tests)")
	dir := fs.String("dir", "", "where brokers keep their data (default: a temp dir)")
	outFile := fs.String("o", "", "also write the Markdown report to this file")
	fs.Parse(os.Args[3:])

	base := *dir
	if base == "" {
		var err error
		if base, err = os.MkdirTemp("", "sluice-bench-"); err != nil {
			fail(err)
		}
		defer os.RemoveAll(base)
	}
	var out io.Writer = os.Stdout
	if *outFile != "" {
		f, err := os.Create(*outFile)
		if err != nil {
			fail(err)
		}
		defer f.Close()
		out = io.MultiWriter(os.Stdout, f)
	}
	e := &env{dir: base, quick: *quick, out: out}
	fmt.Fprintf(out, "# sluice experiments\n\n%s\n", environment())

	experiments := map[string]func(*env) error{
		"durability": expDurability, "queues": expQueues, "replication": expReplication, "fetch": expFetch,
	}
	order := []string{"durability", "queues", "replication", "fetch"}
	if name != "all" {
		if _, ok := experiments[name]; !ok {
			fail(fmt.Errorf("unknown experiment %q", name))
		}
		order = []string{name}
	}
	for _, n := range order {
		start := time.Now()
		if err := experiments[n](e); err != nil {
			fail(fmt.Errorf("%s: %w", n, err))
		}
		fmt.Fprintf(os.Stderr, "%s finished in %v\n", n, time.Since(start).Round(time.Second))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "sluice-bench:", err)
	os.Exit(1)
}

func (e *env) sub(name string) string {
	d := filepath.Join(e.dir, fmt.Sprintf("%s-%d", name, time.Now().UnixNano()))
	os.MkdirAll(d, 0o755)
	return d
}

func (e *env) printf(format string, args ...any) { fmt.Fprintf(e.out, format, args...) }

// environment describes the machine, so numbers are never quoted without their context.
func environment() string {
	var b strings.Builder
	fmt.Fprintf(&b, "- %s/%s, %d CPUs, %s\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	if rel, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		fmt.Fprintf(&b, "- Linux kernel %s\n", strings.TrimSpace(string(rel)))
	}
	if _, err := os.Stat("/.dockerenv"); err == nil {
		b.WriteString("- inside a Docker container\n")
	}
	fmt.Fprintf(&b, "- run at %s\n", time.Now().UTC().Format("2006-01-02 15:04 UTC"))
	return b.String()
}

// startBroker starts a single broker in its own directory.
func startBroker(dir string, mutate func(*broker.Config)) (*broker.Broker, error) {
	cfg := broker.Config{NodeID: 1, DataDir: dir, HeartbeatInterval: 100 * time.Millisecond, SessionTimeout: 3 * time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	return broker.New(cfg)
}

// startCluster starts n brokers that know each other's addresses.
func startCluster(dir string, n int, mutate func(i int, cfg *broker.Config)) ([]*broker.Broker, error) {
	peers := map[int32]string{}
	lns := make([]net.Listener, n)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		lns[i] = ln
		peers[int32(i+1)] = ln.Addr().String()
	}
	var out []*broker.Broker
	for i := 0; i < n; i++ {
		cfg := broker.Config{NodeID: int32(i + 1), Listener: lns[i], Peers: peers, DataDir: filepath.Join(dir, strconv.Itoa(i+1)),
			ReplicaFetchWait: 500 * time.Millisecond}
		if mutate != nil {
			mutate(i, &cfg)
		}
		b, err := broker.New(cfg)
		if err != nil {
			for _, o := range out {
				o.Close()
			}
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func closeAll(bs []*broker.Broker) {
	for _, b := range bs {
		b.Close()
	}
}

// metric reads one series from a broker's registry.
func metric(b *broker.Broker, name, partition string) float64 {
	var sb strings.Builder
	b.Registry().Write(&sb)
	prefix := name
	if partition != "" {
		prefix = fmt.Sprintf("%s{partition=%q}", name, partition)
	}
	sc := bufio.NewScanner(strings.NewReader(sb.String()))
	for sc.Scan() {
		if line := sc.Text(); strings.HasPrefix(line, prefix+" ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
			return v
		}
	}
	return 0
}

type latencies []time.Duration

func (l latencies) pct(p float64) time.Duration {
	if len(l) == 0 {
		return 0
	}
	s := append(latencies(nil), l...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(p / 100 * float64(len(s)-1))
	return s[i]
}

func (l latencies) max() time.Duration {
	var m time.Duration
	for _, d := range l {
		m = max(m, d)
	}
	return m
}

func ms(d time.Duration) string {
	switch {
	case d >= 100*time.Millisecond:
		return fmt.Sprintf("%.0f ms", float64(d)/1e6)
	case d >= 10*time.Millisecond:
		return fmt.Sprintf("%.1f ms", float64(d)/1e6)
	default:
		return fmt.Sprintf("%.2f ms", float64(d)/1e6)
	}
}

func rate(n int, d time.Duration) string {
	r := float64(n) / d.Seconds()
	if r >= 10000 {
		return fmt.Sprintf("%.0fk", r/1000)
	}
	return fmt.Sprintf("%.0f", r)
}

func cpuSeconds() float64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}
