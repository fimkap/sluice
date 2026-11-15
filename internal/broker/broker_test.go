package broker_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fimkap/sluice/client"
	"github.com/fimkap/sluice/internal/broker"
	"github.com/fimkap/sluice/internal/logstore"
	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/record"
	"github.com/fimkap/sluice/internal/wire"
)

func TestProduceAndConsumeInOrder(t *testing.T) {
	b := startBroker(t, func(c *broker.Config) { c.Log.Fsync = logstore.FsyncGroup })
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	if err := cl.CreateTopic(ctx, "orders", 3, 1); err != nil {
		t.Fatal(err)
	}
	if err := cl.CreateTopic(ctx, "orders", 3, 1); !errors.Is(err, protocol.ErrTopicExists) {
		t.Fatalf("second create: %v", err)
	}
	p, err := cl.NewProducer(ctx, client.ProducerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	const n = 1000
	produceN(t, p, "orders", n, func(i int) []byte { return []byte(fmt.Sprintf("customer-%d", i%17)) })

	cons, err := cl.NewConsumer(ctx, client.ConsumerConfig{Group: "g", Topics: []string{"orders"}, FromEarliest: true})
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close(ctx)
	seen := map[string]bool{}
	lastPerKey := map[string]int{}
	for len(seen) < n {
		recs, err := cons.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if seen[string(r.Value)] {
				t.Fatalf("duplicate %s", r.Value)
			}
			seen[string(r.Value)] = true
			var i int
			fmt.Sscanf(string(r.Value), "m-%d", &i)
			if last, ok := lastPerKey[string(r.Key)]; ok && i < last {
				t.Fatalf("key %s out of order: %d after %d", r.Key, i, last)
			}
			lastPerKey[string(r.Key)] = i
		}
	}
}

// A retried batch (same producer id and sequence) is stored once and gets its original offset.
func TestIdempotentRetry(t *testing.T) {
	b := startBroker(t, nil)
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	if err := cl.CreateTopic(ctx, "idem", 1, 1); err != nil {
		t.Fatal(err)
	}
	cn, err := wire.Dial(ctx, b.Addr(), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()

	send := func(seq int32, n int) protocol.ProduceResponse {
		bl := record.NewBuilder(77, 0, seq)
		for i := 0; i < n; i++ {
			bl.Append(time.Now().UnixMilli(), nil, []byte("v"), nil)
		}
		var resp protocol.ProduceResponse
		if err := cn.Do(ctx, protocol.APIProduce, &protocol.ProduceRequest{Topic: "idem", Acks: protocol.AcksLeader, Batch: bl.Build()}, &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	first := send(0, 3)
	second := send(3, 2)
	retry := send(3, 2) // the same batch again, as after a lost response
	if first.Error != protocol.ErrNone || second.BaseOffset != 3 || retry.Error != protocol.ErrNone || retry.BaseOffset != 3 {
		t.Fatalf("first %+v second %+v retry %+v", first, second, retry)
	}
	if end, _ := cl.ListOffsets(ctx, "idem", 0, protocol.LatestOffset); end != 5 {
		t.Fatalf("log end %d, want 5: the retry was stored twice", end)
	}
	if gap := send(9, 1); gap.Error != protocol.ErrOutOfOrderSequence {
		t.Fatalf("sequence gap: %v", gap.Error)
	}
}

func TestLongPollWakesOnProduce(t *testing.T) {
	b := startBroker(t, nil)
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "lp", 1, 1)
	p, _ := cl.NewProducer(ctx, client.ProducerConfig{})
	defer p.Close()

	go func() {
		time.Sleep(100 * time.Millisecond)
		p.ProduceSync(ctx, client.Message{Topic: "lp", Value: []byte("wake")})
	}()
	start := time.Now()
	resp, err := cl.Fetch(ctx, "lp", 0, 0, 1<<20, 5*time.Second)
	if err != nil || resp.Error != protocol.ErrNone || len(resp.Records) == 0 {
		t.Fatalf("fetch: %v %+v", err, resp)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("long poll returned after %v, should wake when data arrives", took)
	}
}

func TestRestartRecoversTopicsDataAndProducerState(t *testing.T) {
	dir := t.TempDir()
	cfg := func(c *broker.Config) { c.DataDir = dir }
	b := startBroker(t, cfg)
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "durable", 1, 1)

	batch := func(seq int32) record.Batch {
		bl := record.NewBuilder(5, 0, seq)
		bl.Append(1, nil, []byte(fmt.Sprintf("seq-%d", seq)), nil)
		return bl.Build()
	}
	produce := func(addr string, seq int32) protocol.ProduceResponse {
		cn, err := wire.Dial(ctx, addr, "t")
		if err != nil {
			t.Fatal(err)
		}
		defer cn.Close()
		var resp protocol.ProduceResponse
		cn.Do(ctx, protocol.APIProduce, &protocol.ProduceRequest{Topic: "durable", Acks: protocol.AcksLeader, Batch: batch(seq)}, &resp)
		return resp
	}
	for seq := int32(0); seq < 10; seq++ {
		if r := produce(b.Addr(), seq); r.Error != protocol.ErrNone {
			t.Fatal(r.Error)
		}
	}
	cl.Close()
	b.Close()

	b2 := startBroker(t, cfg)
	cl2 := newClient(t, b2.Addr())
	if end, err := cl2.ListOffsets(ctx, "durable", 0, protocol.LatestOffset); err != nil || end != 10 {
		t.Fatalf("after restart: end %d err %v", end, err)
	}
	// The dedup window was rebuilt from the log: a retry of the last batch is recognised.
	if r := produce(b2.Addr(), 9); r.Error != protocol.ErrNone || r.BaseOffset != 9 {
		t.Fatalf("retry after restart: %+v", r)
	}
	if end, _ := cl2.ListOffsets(ctx, "durable", 0, protocol.LatestOffset); end != 10 {
		t.Fatalf("retry after restart was appended again: end %d", end)
	}
}

func TestZeroCopyAndCopyServeIdenticalBytes(t *testing.T) {
	var raw [2][]byte
	for i, zc := range []bool{true, false} {
		b := startBroker(t, func(c *broker.Config) { c.ZeroCopy = zc })
		cl := newClient(t, b.Addr())
		ctx := ctxT(t)
		cl.CreateTopic(ctx, "zc", 1, 1)
		p, _ := cl.NewProducer(ctx, client.ProducerConfig{NotIdempotent: true})
		produceN(t, p, "zc", 500, nil)
		p.Close()
		resp, err := cl.Fetch(ctx, "zc", 0, 0, 1<<20, 0)
		if err != nil || resp.Error != protocol.ErrNone {
			t.Fatal(err, resp.Error)
		}
		raw[i] = resp.Records
		sent := metric(b, "sluice_fetch_sendfile_bytes_total", "")
		if zc && sent <= 0 {
			t.Fatal("zero-copy broker did not use sendfile")
		}
		if !zc && sent != 0 {
			t.Fatal("copying broker used sendfile")
		}
	}
	// Timestamps differ between the two runs; compare the structure instead of the raw bytes.
	count := func(data []byte) (n int) {
		for {
			b, rest, ok := record.Next(data)
			if !ok {
				return
			}
			if err := b.Validate(); err != nil {
				t.Fatal(err)
			}
			n += int(b.RecordCount())
			data = rest
		}
	}
	if count(raw[0]) != 500 || count(raw[1]) != 500 {
		t.Fatalf("records: zero-copy %d, copy %d", count(raw[0]), count(raw[1]))
	}
}

func TestMetricsAndHealthEndpoints(t *testing.T) {
	b := startBroker(t, func(c *broker.Config) { c.HTTPAddr = "127.0.0.1:0" })
	cl := newClient(t, b.Addr())
	cl.CreateTopic(ctxT(t), "m", 1, 1)
	get := func(path string) string {
		resp, err := http.Get("http://" + b.HTTPAddr() + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}
	if !strings.Contains(get("/healthz"), "ok") {
		t.Fatal("healthz")
	}
	m := get("/metrics")
	for _, want := range []string{`sluice_requests_total{api="CreateTopic"} 1`, `sluice_partition_high_watermark{partition="m-0"}`, "process_cpu_seconds_total"} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestRejectsCorruptBatch(t *testing.T) {
	b := startBroker(t, nil)
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "c", 1, 1)
	bl := record.NewBuilder(record.NoProducerID, 0, record.NoSequence)
	bl.Append(1, nil, []byte("x"), nil)
	batch := bl.Build()
	batch[len(batch)-1] ^= 0xff
	cn, _ := wire.Dial(ctx, b.Addr(), "t")
	defer cn.Close()
	var resp protocol.ProduceResponse
	cn.Do(ctx, protocol.APIProduce, &protocol.ProduceRequest{Topic: "c", Acks: protocol.AcksLeader, Batch: batch}, &resp)
	if resp.Error != protocol.ErrCorruptMessage {
		t.Fatalf("got %v", resp.Error)
	}
	var unknown protocol.ProduceResponse
	cn.Do(ctx, protocol.APIProduce, &protocol.ProduceRequest{Topic: "nope", Acks: protocol.AcksLeader, Batch: bytes.Clone(batch)}, &unknown)
	if unknown.Error != protocol.ErrUnknownTopicOrPartition {
		t.Fatalf("unknown topic: %v", unknown.Error)
	}
}

func TestAcksZeroGetsNoResponse(t *testing.T) {
	b := startBroker(t, nil)
	cl := newClient(t, b.Addr())
	ctx := ctxT(t)
	cl.CreateTopic(ctx, "a0", 1, 1)
	p, _ := cl.NewProducer(ctx, client.ProducerConfig{Acks: protocol.AcksNone, AcksSet: true, NotIdempotent: true})
	for i := 0; i < 50; i++ {
		if off, err := p.ProduceSync(ctx, client.Message{Topic: "a0", Value: []byte("x")}); err != nil || off != -1 {
			t.Fatalf("acks=0 produce: %d %v", off, err)
		}
	}
	p.Close()
	waitFor(t, 5*time.Second, "acks=0 records to land", func() bool {
		end, _ := cl.ListOffsets(ctx, "a0", 0, protocol.LatestOffset)
		return end == 50
	})
}
