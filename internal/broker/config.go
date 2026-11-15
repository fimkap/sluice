// Package broker is the sluice server: it accepts client connections, owns topics and partitions,
// replicates them, and coordinates consumer groups and share groups. See docs/design.md.
package broker

import (
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fimkap/sluice/internal/logstore"
)

// CommitRule decides when a record is committed (below the high watermark).
type CommitRule uint8

const (
	// CommitISR is Kafka's rule: every in-sync replica has the record.
	CommitISR CommitRule = iota
	// CommitQuorum is Raft's rule: a majority of all replicas has the record.
	CommitQuorum
)

func (r CommitRule) String() string {
	if r == CommitQuorum {
		return "quorum"
	}
	return "isr"
}

// ParseCommitRule parses "isr" or "quorum".
func ParseCommitRule(s string) (CommitRule, error) {
	switch s {
	case "isr":
		return CommitISR, nil
	case "quorum":
		return CommitQuorum, nil
	}
	return 0, fmt.Errorf("unknown commit rule %q (want isr or quorum)", s)
}

// Config configures a broker. Zero values take the defaults applied by New.
type Config struct {
	NodeID     int32
	ListenAddr string       // default 127.0.0.1:0
	Listener   net.Listener // optional, overrides ListenAddr (tests pre-allocate ports)
	HTTPAddr   string       // /metrics, /healthz, /debug/pprof; empty disables
	DataDir    string

	// Peers maps every broker id, including this one, to its advertised address. Empty means a single
	// broker that advertises its listener address.
	Peers map[int32]string

	Log logstore.Config // segment size, fsync policy, retention

	CommitRule        CommitRule
	MinInsync         int           // acks=all needs at least this many ISR members (ISR rule only), default 1
	ReplicaLagMax     time.Duration // a follower that hasn't caught up for this long leaves the ISR, default 10s
	ReplicaFetchWait  time.Duration // follower long-poll, default 500ms
	ReplicaFetchBytes int           // default 1 MiB
	// ChaosReplicaDelay makes this broker's followers sleep before every fetch, to play a slow
	// follower in experiment E3.
	ChaosReplicaDelay time.Duration

	ZeroCopy bool // serve fetches with sendfile(2)

	SessionTimeout    time.Duration // consumer group member timeout, default 10s
	HeartbeatInterval time.Duration // told to group members, default 1s

	ShareLockDuration  time.Duration // default 30s
	ShareMaxDeliveries int           // default 5
	ShareMaxInFlight   int           // per share partition, default 2000

	RetentionCheck time.Duration // default 30s
	Logger         *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.ListenAddr == "" {
		c.ListenAddr = "127.0.0.1:0"
	}
	if c.MinInsync <= 0 {
		c.MinInsync = 1
	}
	if c.ReplicaLagMax <= 0 {
		c.ReplicaLagMax = 10 * time.Second
	}
	if c.ReplicaFetchWait <= 0 {
		c.ReplicaFetchWait = 500 * time.Millisecond
	}
	if c.ReplicaFetchBytes <= 0 {
		c.ReplicaFetchBytes = 1 << 20
	}
	if c.SessionTimeout <= 0 {
		c.SessionTimeout = 10 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = time.Second
	}
	if c.ShareLockDuration <= 0 {
		c.ShareLockDuration = 30 * time.Second
	}
	if c.ShareMaxDeliveries <= 0 {
		c.ShareMaxDeliveries = 5
	}
	if c.ShareMaxInFlight <= 0 {
		c.ShareMaxInFlight = 2000
	}
	if c.RetentionCheck <= 0 {
		c.RetentionCheck = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// ParsePeers parses "1=host:9092,2=host:9093".
func ParsePeers(s string) (map[int32]string, error) {
	peers := map[int32]string{}
	if strings.TrimSpace(s) == "" {
		return peers, nil
	}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("peer %q: want id=host:port", part)
		}
		n, err := strconv.ParseInt(id, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("peer %q: %w", part, err)
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("peer %q: %w", part, err)
		}
		peers[int32(n)] = addr
	}
	return peers, nil
}

// nodeIDs returns the broker ids in ascending order.
func (c *Config) nodeIDs() []int32 {
	ids := make([]int32, 0, len(c.Peers))
	for id := range c.Peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
