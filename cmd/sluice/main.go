// Command sluice runs a broker.
//
//	sluice --node-id 1 --data-dir ./data                    # single broker on :9092
//	sluice --node-id 2 --peers 1=b1:9092,2=b2:9092,3=b3:9092 # one of three
//	sluice healthcheck                                      # exit 0 if the local broker is healthy
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/fimkap/sluice/internal/broker"
	"github.com/fimkap/sluice/internal/logstore"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck(os.Args[2:]))
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sluice:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		nodeID           = flag.Int("node-id", 1, "this broker's id")
		listen           = flag.String("listen", ":9092", "client and replication listener")
		advertise        = flag.String("advertise", "", "address others use to reach this broker (default: hostname and listen port)")
		httpAddr         = flag.String("http", ":9100", "HTTP listener for /metrics, /healthz and /debug/pprof (empty disables)")
		dataDir          = flag.String("data-dir", "./data", "where logs and metadata live")
		peers            = flag.String("peers", "", "all brokers as id=host:port,... (empty: a single broker)")
		fsync            = flag.String("fsync", "group", "fsync policy: none, interval, always or group")
		fsyncInterval    = flag.Duration("fsync-interval", 100*time.Millisecond, "for --fsync interval")
		groupCommitDelay = flag.Duration("group-commit-delay", 0, "extra wait before a group commit, to gather more writes")
		segmentBytes     = flag.Int64("segment-bytes", 64<<20, "roll segments at this size")
		retentionBytes   = flag.Int64("retention-bytes", -1, "keep at least this many bytes per partition (-1: unlimited)")
		retention        = flag.Duration("retention", 7*24*time.Hour, "delete segments older than this (0: keep)")
		commitRule       = flag.String("commit-rule", "isr", "isr (Kafka) or quorum (Raft majority)")
		minInsync        = flag.Int("min-insync", 1, "acks=all needs this many in-sync replicas (isr rule)")
		replicaLagMax    = flag.Duration("replica-lag-max", 10*time.Second, "a follower not caught up for this long leaves the ISR")
		zeroCopy         = flag.Bool("zero-copy", true, "serve fetches with sendfile(2)")
		shareLock        = flag.Duration("share-lock", 30*time.Second, "share-group record lock duration")
		shareDeliveries  = flag.Int("share-max-deliveries", 5, "deliveries before a share-group record is archived")
		chaosDelay       = flag.Duration("chaos-replica-delay", 0, "sleep before every replica fetch (to play a slow follower)")
		logLevel         = flag.String("log-level", "info", "debug, info, warn or error")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	policy, err := logstore.ParseFsyncPolicy(*fsync)
	if err != nil {
		return err
	}
	rule, err := broker.ParseCommitRule(*commitRule)
	if err != nil {
		return err
	}
	peerMap, err := broker.ParsePeers(*peers)
	if err != nil {
		return err
	}
	if len(peerMap) == 0 {
		addr, err := advertised(*advertise, *listen)
		if err != nil {
			return err
		}
		peerMap[int32(*nodeID)] = addr
	}

	b, err := broker.New(broker.Config{
		NodeID:     int32(*nodeID),
		ListenAddr: *listen,
		HTTPAddr:   *httpAddr,
		DataDir:    *dataDir,
		Peers:      peerMap,
		Log: logstore.Config{
			SegmentBytes:     *segmentBytes,
			Fsync:            policy,
			FsyncInterval:    *fsyncInterval,
			GroupCommitDelay: *groupCommitDelay,
			RetentionBytes:   *retentionBytes,
			RetentionAge:     *retention,
		},
		CommitRule:         rule,
		MinInsync:          *minInsync,
		ReplicaLagMax:      *replicaLagMax,
		ChaosReplicaDelay:  *chaosDelay,
		ZeroCopy:           *zeroCopy,
		ShareLockDuration:  *shareLock,
		ShareMaxDeliveries: *shareDeliveries,
		Logger:             logger,
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	logger.Info("shutting down")
	return b.Close()
}

// advertised picks the address other brokers and clients use: the flag, or the hostname with the
// listener's port.
func advertised(flagValue, listen string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		if host, err = os.Hostname(); err != nil {
			return "", err
		}
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", fmt.Errorf("listen port %q: %w", port, err)
	}
	return net.JoinHostPort(host, port), nil
}

// healthcheck is for container HEALTHCHECKs: the distroless image has no shell or curl.
func healthcheck(args []string) int {
	fs := flag.NewFlagSet("healthcheck", flag.ExitOnError)
	url := fs.String("url", "http://127.0.0.1:9100/healthz", "health endpoint")
	fs.Parse(args)
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(*url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
