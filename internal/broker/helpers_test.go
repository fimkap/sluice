package broker_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fimkap/sluice/client"
	"github.com/fimkap/sluice/internal/broker"
)

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// startBroker runs one broker on a temp dir.
func startBroker(t *testing.T, mutate func(*broker.Config)) *broker.Broker {
	t.Helper()
	cfg := broker.Config{NodeID: 1, DataDir: t.TempDir(), HeartbeatInterval: 50 * time.Millisecond, SessionTimeout: time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

type cluster struct {
	t       *testing.T
	brokers []*broker.Broker
	cfgs    []broker.Config
	stopped []bool
}

// startCluster runs n brokers that know each other. Listeners are opened first so every broker knows
// every address up front.
func startCluster(t *testing.T, n int, mutate func(i int, cfg *broker.Config)) *cluster {
	t.Helper()
	peers := map[int32]string{}
	lns := make([]net.Listener, n)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		peers[int32(i+1)] = ln.Addr().String()
	}
	c := &cluster{t: t, stopped: make([]bool, n)}
	for i := 0; i < n; i++ {
		cfg := broker.Config{NodeID: int32(i + 1), Listener: lns[i], Peers: peers, DataDir: t.TempDir(),
			ReplicaFetchWait: 200 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond, SessionTimeout: time.Second}
		if mutate != nil {
			mutate(i, &cfg)
		}
		b, err := broker.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.brokers = append(c.brokers, b)
		c.cfgs = append(c.cfgs, cfg)
	}
	t.Cleanup(func() {
		for i, b := range c.brokers {
			if !c.stopped[i] {
				b.Close()
			}
		}
	})
	return c
}

func (c *cluster) addrs() []string {
	var out []string
	for _, b := range c.brokers {
		out = append(out, b.Addr())
	}
	return out
}

func (c *cluster) stop(id int32) {
	c.brokers[id-1].Close()
	c.stopped[id-1] = true
}

func newClient(t *testing.T, addrs ...string) *client.Client {
	t.Helper()
	cl, err := client.New(ctxT(t), client.Config{Bootstrap: addrs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// metric reads one series from a broker's registry, e.g. metric(b, "sluice_partition_isr_size", "t-0").
func metric(b *broker.Broker, name, partition string) float64 {
	var sb strings.Builder
	b.Registry().Write(&sb)
	prefix := name
	if partition != "" {
		prefix = fmt.Sprintf("%s{partition=%q}", name, partition)
	}
	sc := bufio.NewScanner(strings.NewReader(sb.String()))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, prefix+" ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
			return v
		}
	}
	return -1
}

func produceN(t *testing.T, p *client.Producer, topic string, n int, key func(i int) []byte) {
	t.Helper()
	ctx := ctxT(t)
	futs := make([]*client.Future, n)
	for i := 0; i < n; i++ {
		var k []byte
		if key != nil {
			k = key(i)
		}
		futs[i] = p.Produce(ctx, client.Message{Topic: topic, Key: k, Value: []byte(fmt.Sprintf("m-%d", i))})
	}
	for i, f := range futs {
		if _, err := f.Wait(ctx); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
}
