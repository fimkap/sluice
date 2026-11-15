package broker_test

import (
	"errors"
	"testing"
	"time"

	"github.com/fimkap/sluice/client"
	"github.com/fimkap/sluice/internal/broker"
	"github.com/fimkap/sluice/internal/logstore"
	"github.com/fimkap/sluice/internal/protocol"
)

// leaderAndFollowers returns the replicas of topic/partition 0 from metadata.
func leaderAndFollowers(t *testing.T, cl *client.Client, topic string) (int32, []int32) {
	md, err := cl.Metadata(ctxT(t), topic)
	if err != nil || len(md.Topics) != 1 || md.Topics[0].Error != protocol.ErrNone {
		t.Fatalf("metadata: %v %+v", err, md)
	}
	p := md.Topics[0].Partitions[0]
	return p.Leader, p.Replicas[1:]
}

func TestAcksAllReplicatesToEveryFollower(t *testing.T) {
	c := startCluster(t, 3, func(_ int, cfg *broker.Config) { cfg.Log.Fsync = logstore.FsyncGroup; cfg.MinInsync = 2 })
	cl := newClient(t, c.addrs()...)
	ctx := ctxT(t)
	if err := cl.CreateTopic(ctx, "rep", 1, 3); err != nil {
		t.Fatal(err)
	}
	p, _ := cl.NewProducer(ctx, client.ProducerConfig{})
	defer p.Close()
	produceN(t, p, "rep", 200, nil)

	leader, followers := leaderAndFollowers(t, cl, "rep")
	if hw := metric(c.brokers[leader-1], "sluice_partition_high_watermark", "rep-0"); hw != 200 {
		t.Fatalf("leader HW %v, want 200", hw)
	}
	// acks=all returned, so every in-sync follower must already hold all 200 records.
	for _, f := range followers {
		if leo := metric(c.brokers[f-1], "sluice_partition_log_end_offset", "rep-0"); leo != 200 {
			t.Fatalf("follower %d LEO %v after acks=all", f, leo)
		}
	}
	if isr := metric(c.brokers[leader-1], "sluice_partition_isr_size", "rep-0"); isr != 3 {
		t.Fatalf("ISR size %v", isr)
	}
}

func TestStoppedFollowerLeavesISR(t *testing.T) {
	c := startCluster(t, 3, func(_ int, cfg *broker.Config) {
		cfg.ReplicaLagMax = 400 * time.Millisecond
		cfg.MinInsync = 2
	})
	cl := newClient(t, c.addrs()...)
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "isr", 1, 3)
	p, _ := cl.NewProducer(ctx, client.ProducerConfig{Timeout: 5 * time.Second})
	defer p.Close()
	produceN(t, p, "isr", 10, nil)

	leader, followers := leaderAndFollowers(t, cl, "isr")
	c.stop(followers[0])
	start := time.Now()
	produceN(t, p, "isr", 10, nil) // completes once the stopped follower drops out of the ISR
	t.Logf("acks=all waited %v for the ISR to shrink", time.Since(start).Round(time.Millisecond))
	waitFor(t, 5*time.Second, "ISR to shrink to 2", func() bool {
		return metric(c.brokers[leader-1], "sluice_partition_isr_size", "isr-0") == 2
	})
}

func TestMinInsyncRejectsWhenISRTooSmall(t *testing.T) {
	c := startCluster(t, 3, func(_ int, cfg *broker.Config) {
		cfg.ReplicaLagMax = 300 * time.Millisecond
		cfg.MinInsync = 3
	})
	cl := newClient(t, c.addrs()...)
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "strict", 1, 3)
	p, _ := cl.NewProducer(ctx, client.ProducerConfig{Timeout: 3 * time.Second, Retries: 1})
	defer p.Close()
	produceN(t, p, "strict", 5, nil)

	_, followers := leaderAndFollowers(t, cl, "strict")
	c.stop(followers[0])
	time.Sleep(500 * time.Millisecond) // past the lag window
	_, err := p.ProduceSync(ctx, client.Message{Topic: "strict", Value: []byte("x")})
	if !errors.Is(err, protocol.ErrNotEnoughReplicas) {
		t.Fatalf("want NOT_ENOUGH_REPLICAS, got %v", err)
	}
}

// Under the quorum rule a majority is enough: one follower down costs nothing, no lag window to wait out.
func TestQuorumCommitsWithOneFollowerDown(t *testing.T) {
	c := startCluster(t, 3, func(_ int, cfg *broker.Config) {
		cfg.CommitRule = broker.CommitQuorum
		cfg.ReplicaLagMax = time.Hour // would block the ISR rule for an hour
	})
	cl := newClient(t, c.addrs()...)
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "q", 1, 3)
	p, _ := cl.NewProducer(ctx, client.ProducerConfig{Timeout: 5 * time.Second})
	defer p.Close()
	produceN(t, p, "q", 10, nil)

	_, followers := leaderAndFollowers(t, cl, "q")
	c.stop(followers[0])
	start := time.Now()
	produceN(t, p, "q", 50, nil)
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("quorum produce took %v with one follower down", took)
	}
}

// A follower that is slow but keeps fetching stays in the ISR, so acks=all waits for it; the quorum rule
// doesn't. This is experiment E3 in miniature.
func TestSlowFollowerSetsThePaceOnlyUnderISR(t *testing.T) {
	latency := func(rule broker.CommitRule) time.Duration {
		c := startCluster(t, 3, func(i int, cfg *broker.Config) {
			cfg.CommitRule = rule
			if i == 2 {
				cfg.ChaosReplicaDelay = 60 * time.Millisecond
			}
		})
		cl := newClient(t, c.addrs()...)
		ctx := ctxT(t)
		cl.CreateTopic(ctx, "slow", 1, 3)
		p, _ := cl.NewProducer(ctx, client.ProducerConfig{})
		defer p.Close()
		produceN(t, p, "slow", 5, nil) // warm up
		start := time.Now()
		for i := 0; i < 10; i++ {
			if _, err := p.ProduceSync(ctx, client.Message{Topic: "slow", Value: []byte("x")}); err != nil {
				t.Fatal(err)
			}
		}
		return time.Since(start) / 10
	}
	isr, quorum := latency(broker.CommitISR), latency(broker.CommitQuorum)
	t.Logf("acks=all per record: ISR %v, quorum %v", isr, quorum)
	if isr < 40*time.Millisecond || quorum > isr/2 {
		t.Fatalf("expected the slow follower to dominate only the ISR rule: ISR %v, quorum %v", isr, quorum)
	}
}
