package broker_test

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/fimkap/sluice/client"
	"github.com/fimkap/sluice/internal/broker"
	"github.com/fimkap/sluice/internal/protocol"
)

func shareSetup(t *testing.T, n int, mutate func(*broker.Config)) *client.Client {
	b := startBroker(t, mutate)
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	if err := cl.CreateTopic(ctx, "jobs", 1, 1); err != nil {
		t.Fatal(err)
	}
	p, _ := cl.NewProducer(ctx, client.ProducerConfig{})
	produceN(t, p, "jobs", n, nil)
	p.Close()
	return cl
}

func newShare(t *testing.T, cl *client.Client) *client.ShareConsumer {
	s, err := cl.NewShareConsumer(ctxT(t), client.ShareConsumerConfig{Group: "sg", Topic: "jobs", FromEarliest: true, MaxWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func pollOffsets(t *testing.T, s *client.ShareConsumer, want int) map[int64]client.Record {
	t.Helper()
	got := map[int64]client.Record{}
	wait := 5 * time.Second
	if want == 0 {
		wait = 300 * time.Millisecond // proving absence: poll a few times, then stop
	}
	deadline := time.Now().Add(wait)
	for (want == 0 || len(got) < want) && time.Now().Before(deadline) {
		recs, err := s.Poll(ctxT(t))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			got[r.Offset] = r
		}
	}
	return got
}

func keys(m map[int64]client.Record) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestShareAcceptReleaseRejectAndLockExpiry(t *testing.T) {
	cl := shareSetup(t, 10, func(c *broker.Config) { c.ShareLockDuration = 400 * time.Millisecond })
	a, b := newShare(t, cl), newShare(t, cl)

	got := pollOffsets(t, a, 10)
	if len(got) != 10 {
		t.Fatalf("a acquired %v", keys(got))
	}
	for off, r := range got {
		switch {
		case off <= 3:
			a.Ack(r, protocol.AckAccept)
		case off <= 5:
			a.Ack(r, protocol.AckRelease)
		case off == 6:
			a.Ack(r, protocol.AckReject)
		} // 7..9 stay acquired by a
	}
	if err := a.Commit(ctxT(t)); err != nil {
		t.Fatal(err)
	}

	again := pollOffsets(t, b, 2)
	if k := keys(again); len(k) != 2 || k[0] != 4 || k[1] != 5 || again[4].DeliveryCount != 2 {
		t.Fatalf("b should get only the released 4 and 5 (delivery 2), got %v", keys(again))
	}
	for _, r := range again {
		b.Ack(r, protocol.AckAccept)
	}
	// 7..9 come back once a's lock expires.
	late := pollOffsets(t, b, 3)
	if k := keys(late); len(k) != 3 || k[0] != 7 || late[7].DeliveryCount != 2 {
		t.Fatalf("after lock expiry b should get 7..9, got %v", keys(late))
	}
	// a's lock is gone: its late ack is refused, and nothing it does can take 7 from b.
	a.Ack(got[7], protocol.AckAccept)
	if err := a.Commit(ctxT(t)); err == nil {
		t.Fatal("ack after lock expiry should fail with INVALID_RECORD_STATE")
	}
}

func TestShareDeliveryLimitArchives(t *testing.T) {
	cl := shareSetup(t, 1, func(c *broker.Config) { c.ShareMaxDeliveries = 2 })
	s := newShare(t, cl)
	for attempt := 1; attempt <= 2; attempt++ {
		got := pollOffsets(t, s, 1)
		r, ok := got[0]
		if !ok || r.DeliveryCount != int16(attempt) {
			t.Fatalf("attempt %d: %v", attempt, keys(got))
		}
		s.Ack(r, protocol.AckRelease)
		if err := s.Commit(ctxT(t)); err != nil {
			t.Fatal(err)
		}
	}
	if got := pollOffsets(t, s, 1); len(got) != 0 {
		t.Fatalf("record should be archived after 2 deliveries, got %v", keys(got))
	}
}

// Many members on one partition: every record is delivered exactly once while locks hold, and the work
// spreads across members (no partition-count ceiling).
func TestShareMembersSplitOnePartition(t *testing.T) {
	const n, members = 2000, 4
	cl := shareSetup(t, n, nil)
	var mu sync.Mutex
	seen := map[int64]int{}
	perMember := make([]int, members)
	var wg sync.WaitGroup
	for m := 0; m < members; m++ {
		wg.Add(1)
		go func(m int) {
			defer wg.Done()
			s, err := cl.NewShareConsumer(ctxT(t), client.ShareConsumerConfig{Group: "sg", Topic: "jobs", FromEarliest: true,
				MaxRecords: 50, MaxWait: 50 * time.Millisecond})
			if err != nil {
				t.Error(err)
				return
			}
			defer s.Close(ctxT(t))
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				mu.Lock()
				done := len(seen) == n
				mu.Unlock()
				if done {
					return
				}
				recs, err := s.Poll(ctxT(t))
				if err != nil {
					t.Error(err)
					return
				}
				for _, r := range recs {
					mu.Lock()
					seen[r.Offset]++
					perMember[m]++
					mu.Unlock()
					time.Sleep(100 * time.Microsecond) // pretend to work, so members interleave
					s.Ack(r, protocol.AckAccept)
				}
			}
		}(m)
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("delivered %d of %d", len(seen), n)
	}
	for off, c := range seen {
		if c != 1 {
			t.Fatalf("offset %d delivered %d times", off, c)
		}
	}
	busy := 0
	for _, c := range perMember {
		if c > 0 {
			busy++
		}
	}
	if busy < 2 {
		t.Fatalf("work did not spread: %v", perMember)
	}
	t.Logf("records per member: %v", perMember)
}
