package broker_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/fimkap/sluice/client"
	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/wire"
)

// pollUntil keeps polling (which applies assignment changes) until cond holds.
func pollUntil(t *testing.T, what string, cond func() bool, consumers ...*client.Consumer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		for _, c := range consumers {
			if _, err := c.Poll(ctxT(t)); err != nil {
				t.Fatal(err)
			}
		}
		assertDisjoint(t, consumers...)
	}
}

func assertDisjoint(t *testing.T, consumers ...*client.Consumer) {
	t.Helper()
	owner := map[client.TopicPartition]int{}
	for i, c := range consumers {
		for _, tp := range c.Assignment() {
			if j, ok := owner[tp]; ok {
				t.Fatalf("partition %v owned by consumers %d and %d at once", tp, j, i)
			}
			owner[tp] = i
		}
	}
}

func TestGroupRebalanceNeverDoubleAssigns(t *testing.T) {
	b := startBroker(t, nil)
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "work", 4, 1)
	cfg := client.ConsumerConfig{Group: "g", Topics: []string{"work"}, FromEarliest: true, MaxWait: 20 * time.Millisecond}

	a, err := cl.NewConsumer(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	pollUntil(t, "a to own all 4", func() bool { return len(a.Assignment()) == 4 }, a)

	bcons, _ := cl.NewConsumer(ctx, cfg)
	pollUntil(t, "a 2 / b 2", func() bool { return len(a.Assignment()) == 2 && len(bcons.Assignment()) == 2 }, a, bcons)

	a.Close(ctx)
	pollUntil(t, "b to own all 4 after a leaves", func() bool { return len(bcons.Assignment()) == 4 }, bcons)
	bcons.Close(ctx)
}

// A member that stops heartbeating is removed after the session timeout and its partitions move.
func TestSessionTimeoutReassigns(t *testing.T) {
	b := startBroker(t, nil) // session timeout 1s
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "st", 2, 1)

	// A raw member joins once and then goes silent.
	cn, _ := wire.Dial(ctx, b.Addr(), "zombie")
	defer cn.Close()
	var resp protocol.GroupHeartbeatResponse
	cn.Do(ctx, protocol.APIGroupHeartbeat, &protocol.GroupHeartbeatRequest{GroupID: "g2", Topics: []string{"st"}}, &resp)
	if resp.Error != protocol.ErrNone || len(resp.Assignment) == 0 {
		t.Fatalf("zombie join: %+v", resp)
	}
	owned := resp.Assignment
	cn.Do(ctx, protocol.APIGroupHeartbeat, &protocol.GroupHeartbeatRequest{GroupID: "g2", MemberID: resp.MemberID,
		MemberEpoch: resp.MemberEpoch, Topics: []string{"st"}, Owned: owned}, &resp)

	live, _ := cl.NewConsumer(ctx, client.ConsumerConfig{Group: "g2", Topics: []string{"st"}, MaxWait: 20 * time.Millisecond})
	defer live.Close(ctx)
	pollUntil(t, "live member to get both partitions after the zombie times out",
		func() bool { return len(live.Assignment()) == 2 }, live)
}

func TestCommittedOffsetsResumeTheGroup(t *testing.T) {
	b := startBroker(t, nil)
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "resume", 1, 1)
	p, _ := cl.NewProducer(ctx, client.ProducerConfig{})
	produceN(t, p, "resume", 100, nil)

	cfg := client.ConsumerConfig{Group: "r", Topics: []string{"resume"}, FromEarliest: true, AutoCommit: true, MaxBytes: 2048}
	first, _ := cl.NewConsumer(ctx, cfg)
	got := map[string]bool{}
	for len(got) < 30 {
		recs, err := first.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			got[string(r.Value)] = true
		}
	}
	first.Close(ctx) // commits the position reached
	produceN(t, p, "resume", 50, func(int) []byte { return nil })
	p.Close()

	second, _ := cl.NewConsumer(ctx, cfg)
	defer second.Close(ctx)
	total := len(got)
	for total < 150 {
		recs, err := second.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			key := fmt.Sprintf("%s@%d", r.Value, r.Offset)
			if got[key] || (r.Offset < 100 && got[string(r.Value)]) {
				t.Fatalf("redelivered %s at offset %d after a clean close", r.Value, r.Offset)
			}
			got[key] = true
			total++
		}
	}
}
