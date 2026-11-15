package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fimkap/sluice/client"
	"github.com/fimkap/sluice/internal/broker"
	"github.com/fimkap/sluice/internal/logstore"
	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/record"
)

// runProducers runs n producers, each with its own connection and one record in flight, for d. It
// returns the number of acknowledged records and their latencies.
func runProducers(addrs []string, topic string, n int, d time.Duration, acks int16, size int) (int, latencies, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d+30*time.Second)
	defer cancel()
	value := make([]byte, size)
	var mu sync.Mutex
	var all latencies
	var total int
	errc := make(chan error, n)
	var wg sync.WaitGroup
	stop := time.Now().Add(d)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl, err := client.New(ctx, client.Config{Bootstrap: addrs})
			if err != nil {
				errc <- err
				return
			}
			defer cl.Close()
			p, err := cl.NewProducer(ctx, client.ProducerConfig{Acks: acks, AcksSet: true})
			if err != nil {
				errc <- err
				return
			}
			defer p.Close()
			var lat latencies
			for time.Now().Before(stop) {
				t0 := time.Now()
				if _, err := p.ProduceSync(ctx, client.Message{Topic: topic, Partition: 0, PartitionSet: true, Value: value}); err != nil {
					errc <- err
					return
				}
				lat = append(lat, time.Since(t0))
			}
			mu.Lock()
			all = append(all, lat...)
			total += len(lat)
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		return 0, nil, err
	}
	return total, all, nil
}

// E1: what durability costs.
func expDurability(e *env) error {
	dur, counts := 3*time.Second, []int{1, 16, 64}
	if e.quick {
		dur, counts = time.Second, []int{1, 16}
	}
	e.printf("\n## E1: durability policies\n\n")
	e.printf("1 KiB records, acks=1, one partition, each producer has its own connection and one record in flight; %v per row.\n\n", dur)
	e.printf("| fsync policy | producers | records/s | MiB/s | p50 | p99 | appends per fsync |\n")
	e.printf("|---|---:|---:|---:|---:|---:|---:|\n")
	for _, pol := range []logstore.FsyncPolicy{logstore.FsyncNone, logstore.FsyncInterval, logstore.FsyncAlways, logstore.FsyncGroup} {
		b, err := startBroker(e.sub("e1"), func(c *broker.Config) { c.Log.Fsync = pol })
		if err != nil {
			return err
		}
		if err := createTopic(b.Addr(), "e1", 1, 1); err != nil {
			b.Close()
			return err
		}
		for _, n := range counts {
			f0, a0 := metric(b, "sluice_log_fsyncs_total", "e1-0"), metric(b, "sluice_log_appends_total", "e1-0")
			total, lat, err := runProducers([]string{b.Addr()}, "e1", n, dur, protocol.AcksLeader, 1024)
			if err != nil {
				b.Close()
				return err
			}
			fsyncs, appends := metric(b, "sluice_log_fsyncs_total", "e1-0")-f0, metric(b, "sluice_log_appends_total", "e1-0")-a0
			per := "only at segment roll"
			if pol != logstore.FsyncNone && fsyncs > 0 {
				per = fmt.Sprintf("%.1f", appends/fsyncs)
			}
			e.printf("| %s | %d | %s | %.1f | %s | %s | %s |\n", pol, n, rate(total, dur),
				float64(total)*1024/dur.Seconds()/(1<<20), ms(lat.pct(50)), ms(lat.pct(99)), per)
		}
		b.Close()
	}
	return nil
}

func createTopic(addr, name string, partitions, rf int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cl, err := client.New(ctx, client.Config{Bootstrap: []string{addr}})
	if err != nil {
		return err
	}
	defer cl.Close()
	return cl.CreateTopic(ctx, name, partitions, rf)
}

// E2: consumer groups vs share groups when 1% of messages are slow.
func expQueues(e *env) error {
	const partitions = 4
	ratePerSec, dur := 1000, 5*time.Second
	if e.quick {
		dur = 2 * time.Second
	}
	const fast, slow, slowEvery = time.Millisecond, 200 * time.Millisecond, 100
	e.printf("\n## E2: consumer groups vs share groups\n\n")
	e.printf("A topic with %d partitions gets %d records/s for %v. Each consumer handles one record at a time: "+
		"%v of work, except every %dth record, which takes %v. Latency is from produce to the end of processing, for the "+
		"fast records only: it shows how long they wait behind slow ones.\n\n", partitions, ratePerSec, dur, fast, slowEvery, slow)
	e.printf("| model | consumers | drained in | fast p50 | fast p99 | fast max | records out of partition order |\n")
	e.printf("|---|---:|---:|---:|---:|---:|---:|\n")
	scenarios := []struct {
		share     bool
		consumers int
	}{{false, 4}, {false, 8}, {true, 4}, {true, 8}, {true, 16}}
	for i, sc := range scenarios {
		res, err := runQueueScenario(e, i, sc.share, sc.consumers, partitions, ratePerSec, dur, fast, slow, slowEvery)
		if err != nil {
			return err
		}
		model := "consumer group"
		if sc.share {
			model = "share group"
		}
		e.printf("| %s | %d | %v | %s | %s | %s | %d |\n", model, sc.consumers, res.drained.Round(100*time.Millisecond),
			ms(res.lat.pct(50)), ms(res.lat.pct(99)), ms(res.lat.max()), res.reordered)
	}
	return nil
}

type queueResult struct {
	drained   time.Duration
	lat       latencies
	reordered int
}

func runQueueScenario(e *env, idx int, share bool, consumers, partitions, ratePerSec int, dur, fast, slow time.Duration, slowEvery int) (queueResult, error) {
	b, err := startBroker(e.sub("e2"), func(c *broker.Config) { c.ShareLockDuration = time.Minute })
	if err != nil {
		return queueResult{}, err
	}
	defer b.Close()
	topic := fmt.Sprintf("jobs-%d", idx)
	if err := createTopic(b.Addr(), topic, partitions, 1); err != nil {
		return queueResult{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), dur+2*time.Minute)
	defer cancel()

	total := int(float64(ratePerSec) * dur.Seconds())
	var processed atomic.Int64
	var mu sync.Mutex
	var lat latencies
	lastOffset := map[int32]int64{}
	reordered := 0
	record := func(r client.Record) {
		seq, _ := strconv.Atoi(string(r.Key))
		if seq%slowEvery == 0 {
			time.Sleep(slow)
		} else {
			time.Sleep(fast)
		}
		mu.Lock()
		if seq%slowEvery != 0 {
			lat = append(lat, time.Since(r.Timestamp))
		}
		if last, ok := lastOffset[r.Partition]; ok && r.Offset < last {
			reordered++
		}
		lastOffset[r.Partition] = max(lastOffset[r.Partition], r.Offset)
		mu.Unlock()
		processed.Add(1)
	}

	var ready sync.WaitGroup
	var wg sync.WaitGroup
	errc := make(chan error, consumers+1)
	for c := 0; c < consumers; c++ {
		ready.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl, err := client.New(ctx, client.Config{Bootstrap: []string{b.Addr()}})
			if err != nil {
				errc <- err
				ready.Done()
				return
			}
			defer cl.Close()
			if share {
				s, err := cl.NewShareConsumer(ctx, client.ShareConsumerConfig{Group: "e2", Topic: topic, FromEarliest: true,
					MaxRecords: 1, MaxWait: 100 * time.Millisecond})
				ready.Done()
				if err != nil {
					errc <- err
					return
				}
				defer s.Close(context.Background())
				for processed.Load() < int64(total) && ctx.Err() == nil {
					recs, err := s.Poll(ctx)
					if err != nil {
						errc <- err
						return
					}
					for _, r := range recs {
						record(r)
						s.Ack(r, protocol.AckAccept)
					}
				}
				return
			}
			cons, err := cl.NewConsumer(ctx, client.ConsumerConfig{Group: "e2", Topics: []string{topic}, FromEarliest: true,
				AutoCommit: true, MaxWait: 100 * time.Millisecond})
			ready.Done()
			if err != nil {
				errc <- err
				return
			}
			defer cons.Close(context.Background())
			for processed.Load() < int64(total) && ctx.Err() == nil {
				recs, err := cons.Poll(ctx)
				if err != nil {
					errc <- err
					return
				}
				for _, r := range recs {
					record(r)
				}
			}
		}()
	}
	ready.Wait()
	time.Sleep(time.Second) // let the group settle its assignment before records arrive

	cl, err := client.New(ctx, client.Config{Bootstrap: []string{b.Addr()}})
	if err != nil {
		return queueResult{}, err
	}
	defer cl.Close()
	p, err := cl.NewProducer(ctx, client.ProducerConfig{Acks: protocol.AcksLeader, AcksSet: true})
	if err != nil {
		return queueResult{}, err
	}
	start := time.Now()
	interval := time.Second / time.Duration(ratePerSec)
	for i := 1; i <= total; i++ {
		if d := time.Until(start.Add(time.Duration(i) * interval)); d > 0 {
			time.Sleep(d)
		}
		p.Produce(ctx, client.Message{Topic: topic, Partition: int32(i % partitions), PartitionSet: true, Key: []byte(strconv.Itoa(i))})
	}
	p.Close()
	for processed.Load() < int64(total) {
		select {
		case err := <-errc:
			return queueResult{}, err
		case <-ctx.Done():
			return queueResult{}, fmt.Errorf("timed out with %d of %d processed", processed.Load(), total)
		case <-time.After(10 * time.Millisecond):
		}
	}
	drained := time.Since(start)
	cancel()
	wg.Wait()
	return queueResult{drained: drained, lat: lat, reordered: reordered}, nil
}

// E3: Kafka's ISR rule vs a majority quorum, with and without a slow follower.
func expReplication(e *env) error {
	dur := 3 * time.Second
	if e.quick {
		dur = time.Second
	}
	const producers, slowBy = 8, 20 * time.Millisecond
	e.printf("\n## E3: commit rule with a slow follower\n\n")
	e.printf("Three brokers, replication factor 3, fsync=group, 1 KiB records, %d producers with one record in flight each; "+
		"the slow follower sleeps %v before every fetch; %v per row.\n\n", producers, slowBy, dur)
	e.printf("| commit rule | followers | acks | records/s | p50 | p99 |\n|---|---|---|---:|---:|---:|\n")
	scenarios := []struct {
		rule broker.CommitRule
		slow bool
		acks int16
	}{
		{broker.CommitISR, false, protocol.AcksLeader},
		{broker.CommitISR, false, protocol.AcksAll},
		{broker.CommitQuorum, false, protocol.AcksAll},
		{broker.CommitISR, true, protocol.AcksAll},
		{broker.CommitQuorum, true, protocol.AcksAll},
	}
	for _, sc := range scenarios {
		dir := e.sub("e3")
		bs, err := startCluster(dir, 3, func(i int, c *broker.Config) {
			c.CommitRule = sc.rule
			c.Log.Fsync = logstore.FsyncGroup
			if sc.slow && i == 2 {
				c.ChaosReplicaDelay = slowBy
			}
		})
		if err != nil {
			return err
		}
		addrs := []string{bs[0].Addr(), bs[1].Addr(), bs[2].Addr()}
		topic, err := topicNotLedBy(addrs, 3)
		if err != nil {
			closeAll(bs)
			return err
		}
		total, lat, err := runProducers(addrs, topic, producers, dur, sc.acks, 1024)
		closeAll(bs)
		if err != nil {
			return err
		}
		followers := "healthy"
		if sc.slow {
			followers = "one slow"
		}
		acks := "1"
		if sc.acks == protocol.AcksAll {
			acks = "all"
		}
		e.printf("| %s | %s | %s | %s | %s | %s |\n", sc.rule, followers, acks, rate(total, dur), ms(lat.pct(50)), ms(lat.pct(99)))
	}
	return nil
}

// topicNotLedBy creates topics until partition 0 of one is led by someone other than broker id, so the
// slow broker is a follower.
func topicNotLedBy(addrs []string, id int32) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := client.New(ctx, client.Config{Bootstrap: addrs})
	if err != nil {
		return "", err
	}
	defer cl.Close()
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("e3-%d", i)
		if err := cl.CreateTopic(ctx, name, 1, 3); err != nil {
			return "", err
		}
		md, err := cl.Metadata(ctx, name)
		if err != nil {
			return "", err
		}
		if md.Topics[0].Partitions[0].Leader != id {
			return name, nil
		}
	}
	return "", errors.New("could not place the slow broker as a follower")
}

// E4: serving fetches with sendfile vs read+write.
func expFetch(e *env) error {
	size := int64(512 << 20)
	if e.quick {
		size = 64 << 20
	}
	const passes = 3
	e.printf("\n## E4: zero-copy fetch\n\n")
	e.printf("One partition holding %d MiB of 1 KiB records, read end to end by a consumer fetching 1 MiB at a time "+
		"(records not decoded); best of %d passes over a warm page cache. CPU is the whole process (broker and consumer).\n\n", size>>20, passes)
	e.printf("| fetch path | GiB/s | CPU seconds per GiB |\n|---|---:|---:|\n")
	dir := e.sub("e4")
	if err := preload(dir, size); err != nil {
		return err
	}
	for _, zc := range []bool{true, false} {
		b, err := startBroker(dir, func(c *broker.Config) { c.ZeroCopy = zc })
		if err != nil {
			return err
		}
		best, bestCPU := 0.0, 0.0
		for pass := 0; pass < passes; pass++ {
			gibps, cpu, err := readAll(b.Addr())
			if err != nil {
				b.Close()
				return err
			}
			if gibps > best {
				best, bestCPU = gibps, cpu
			}
		}
		b.Close()
		name := "sendfile(2)"
		if !zc {
			name = "read + write"
		}
		e.printf("| %s | %.2f | %.2f |\n", name, best, bestCPU)
	}
	return nil
}

func preload(dir string, size int64) error {
	b, err := startBroker(dir, nil)
	if err != nil {
		return err
	}
	defer b.Close()
	if err := createTopic(b.Addr(), "e4", 1, 1); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cl, err := client.New(ctx, client.Config{Bootstrap: []string{b.Addr()}})
	if err != nil {
		return err
	}
	defer cl.Close()
	p, err := cl.NewProducer(ctx, client.ProducerConfig{Acks: protocol.AcksLeader, AcksSet: true, NotIdempotent: true, BatchBytes: 1 << 20})
	if err != nil {
		return err
	}
	value := make([]byte, 1024)
	var last *client.Future
	for written := int64(0); written < size; written += 1024 {
		last = p.Produce(ctx, client.Message{Topic: "e4", Partition: 0, PartitionSet: true, Value: value})
	}
	if _, err := last.Wait(ctx); err != nil {
		return err
	}
	return p.Close()
}

// readAll fetches the whole partition and returns GiB/s and CPU seconds per GiB.
func readAll(addr string) (float64, float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cl, err := client.New(ctx, client.Config{Bootstrap: []string{addr}})
	if err != nil {
		return 0, 0, err
	}
	defer cl.Close()
	end, err := cl.ListOffsets(ctx, "e4", 0, protocol.LatestOffset)
	if err != nil {
		return 0, 0, err
	}
	cpu0, t0 := cpuSeconds(), time.Now()
	var bytes int64
	for next := int64(0); next < end; {
		resp, err := cl.Fetch(ctx, "e4", 0, next, 1<<20, 0)
		if err != nil {
			return 0, 0, err
		}
		if resp.Error != protocol.ErrNone {
			return 0, 0, resp.Error
		}
		bytes += int64(len(resp.Records))
		data := resp.Records
		for {
			b, rest, ok := record.Next(data)
			if !ok {
				break
			}
			next, data = b.NextOffset(), rest
		}
	}
	secs := time.Since(t0).Seconds()
	gib := float64(bytes) / (1 << 30)
	return gib / secs, (cpuSeconds() - cpu0) / gib, nil
}
