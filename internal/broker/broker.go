package broker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fimkap/sluice/internal/metrics"
	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/wire"
)

// Broker is one sluice server.
type Broker struct {
	cfg      Config
	ln       net.Listener
	httpSrv  *http.Server
	httpLn   net.Listener
	topics   *topicManager
	groups   *groupCoordinator
	shares   *shareManager
	metrics  *brokerMetrics
	registry *metrics.Registry

	pidCounter atomic.Int64

	peerMu sync.Mutex
	peers  map[int32]*wire.Conn

	connMu sync.Mutex
	conns  map[*conn]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool
}

// New opens the data directory, loads existing topics and starts serving.
func New(cfg Config) (*Broker, error) {
	cfg.applyDefaults()
	if cfg.DataDir == "" {
		return nil, errors.New("broker: DataDir is required")
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "meta"), 0o755); err != nil {
		return nil, err
	}
	ln := cfg.Listener
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", cfg.ListenAddr); err != nil {
			return nil, err
		}
	}
	if len(cfg.Peers) == 0 {
		cfg.Peers = map[int32]string{cfg.NodeID: ln.Addr().String()}
	}
	if _, ok := cfg.Peers[cfg.NodeID]; !ok {
		ln.Close()
		return nil, fmt.Errorf("broker: node %d is not in the peer list", cfg.NodeID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		cfg:      cfg,
		ln:       ln,
		registry: metrics.NewRegistry(),
		peers:    map[int32]*wire.Conn{},
		conns:    map[*conn]struct{}{},
		ctx:      ctx,
		cancel:   cancel,
	}
	// Producer ids: node id in the top bits, a time-seeded counter below, unique across restarts.
	b.pidCounter.Store(time.Now().UnixMicro() & (1<<52 - 1))
	b.metrics = newBrokerMetrics(b.registry)
	b.groups = newGroupCoordinator(b)
	b.shares = newShareManager(b)
	b.topics = newTopicManager(b)
	if err := b.topics.load(); err != nil {
		ln.Close()
		cancel()
		return nil, err
	}

	b.wg.Add(2)
	go b.acceptLoop()
	go b.retentionLoop()

	if cfg.HTTPAddr != "" {
		if err := b.startHTTP(); err != nil {
			b.Close()
			return nil, err
		}
	}
	cfg.Logger.Info("broker started", "node", cfg.NodeID, "addr", ln.Addr().String(), "fsync", cfg.Log.Fsync.String(),
		"commit_rule", cfg.CommitRule.String(), "zero_copy", cfg.ZeroCopy)
	return b, nil
}

// Addr is the address the broker listens on.
func (b *Broker) Addr() string { return b.ln.Addr().String() }

// HTTPAddr is the address of the HTTP endpoint, or "" if disabled.
func (b *Broker) HTTPAddr() string {
	if b.httpLn == nil {
		return ""
	}
	return b.httpLn.Addr().String()
}

// NodeID is the broker's id.
func (b *Broker) NodeID() int32 { return b.cfg.NodeID }

// Registry exposes the broker's metrics (for in-process experiments).
func (b *Broker) Registry() *metrics.Registry { return b.registry }

func (b *Broker) acceptLoop() {
	defer b.wg.Done()
	for {
		nc, err := b.ln.Accept()
		if err != nil {
			if b.closed.Load() {
				return
			}
			b.cfg.Logger.Warn("accept failed", "err", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if tc, ok := nc.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		c := newConn(b, nc)
		b.connMu.Lock()
		b.conns[c] = struct{}{}
		b.connMu.Unlock()
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			c.serve()
			b.connMu.Lock()
			delete(b.conns, c)
			b.connMu.Unlock()
		}()
	}
}

func (b *Broker) retentionLoop() {
	defer b.wg.Done()
	t := time.NewTicker(b.cfg.RetentionCheck)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			for _, p := range b.topics.allPartitions() {
				if n := p.log.EnforceRetention(time.Now()); n > 0 {
					b.cfg.Logger.Info("retention deleted segments", "partition", p.key, "segments", n)
				}
			}
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Broker) startHTTP() error {
	ln, err := net.Listen("tcp", b.cfg.HTTPAddr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		b.registry.Write(w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if b.closed.Load() {
			http.Error(w, "closing", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	b.httpLn = ln
	b.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go b.httpSrv.Serve(ln)
	return nil
}

// Close stops the broker: no new connections, fetchers stopped, logs synced and closed.
func (b *Broker) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	b.cancel()
	b.ln.Close()
	if b.httpSrv != nil {
		b.httpSrv.Close()
	}
	b.connMu.Lock()
	for c := range b.conns {
		c.close()
	}
	b.connMu.Unlock()
	b.peerMu.Lock()
	for _, pc := range b.peers {
		pc.Close()
	}
	b.peerMu.Unlock()
	b.wg.Wait()
	b.shares.checkpointAll()
	return b.topics.close()
}

// peer returns a shared connection to broker id, dialing if needed.
func (b *Broker) peer(ctx context.Context, id int32) (*wire.Conn, error) {
	b.peerMu.Lock()
	defer b.peerMu.Unlock()
	if pc, ok := b.peers[id]; ok && pc.Err() == nil {
		return pc, nil
	}
	addr, ok := b.cfg.Peers[id]
	if !ok {
		return nil, fmt.Errorf("unknown broker %d", id)
	}
	pc, err := wire.Dial(ctx, addr, fmt.Sprintf("broker-%d", b.cfg.NodeID))
	if err != nil {
		return nil, err
	}
	b.peers[id] = pc
	return pc, nil
}

// coordinatorFor returns the broker that coordinates a consumer group.
func (b *Broker) coordinatorFor(key string) int32 {
	ids := b.cfg.nodeIDs()
	return ids[fnv32(key)%uint32(len(ids))]
}

func (b *Broker) nextProducerID() int64 {
	return int64(b.cfg.NodeID)<<52 | b.pidCounter.Add(1)
}

func fnv32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func splitHostPort(addr string) (string, int32) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	var p int32
	fmt.Sscanf(port, "%d", &p)
	return host, p
}

// brokerMetrics are the broker-wide series; per-partition gauges are registered by partitions.
type brokerMetrics struct {
	requests      map[protocol.APIKey]*metrics.Counter
	produceBytes  *metrics.Counter
	fetchBytes    *metrics.Counter
	zeroCopyBytes *metrics.Counter
	produceTime   *metrics.Histogram
}

func newBrokerMetrics(r *metrics.Registry) *brokerMetrics {
	m := &brokerMetrics{requests: map[protocol.APIKey]*metrics.Counter{}}
	for key := protocol.APIProduce; key <= protocol.APIShareAcknowledge; key++ {
		m.requests[key] = r.Counter("sluice_requests_total", "Requests received, by API.", "api", key.String())
	}
	m.produceBytes = r.Counter("sluice_produce_bytes_total", "Batch bytes appended by produce requests.")
	m.fetchBytes = r.Counter("sluice_fetch_bytes_total", "Record bytes sent in fetch responses.")
	m.zeroCopyBytes = r.Counter("sluice_fetch_sendfile_bytes_total", "Record bytes sent with sendfile(2).")
	m.produceTime = r.Histogram("sluice_produce_seconds", "Time from receiving a produce request to replying.",
		metrics.ExpBuckets(0.0001, 2, 16))
	r.GaugeFunc("sluice_goroutines", "Goroutines.", func() float64 { return float64(runtime.NumGoroutine()) })
	r.GaugeFunc("process_cpu_seconds_total", "User and system CPU time.", cpuSeconds)
	return m
}

// cpuSeconds returns the process's user+system CPU time.
func cpuSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}
