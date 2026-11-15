package client

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/record"
	"github.com/fimkap/sluice/internal/wire"
)

// ProducerConfig configures a Producer. Zero values take the defaults noted.
type ProducerConfig struct {
	// Acks: protocol.AcksAll (default), AcksLeader or AcksNone. Set AcksSet to use AcksNone (0).
	Acks    int16
	AcksSet bool
	// NotIdempotent turns off sequence numbers. By default the producer is idempotent: a retried batch is
	// stored once.
	NotIdempotent bool
	// Linger is how long to wait for more records before sending a batch (default 0: send as soon as the
	// previous batch for the partition is acknowledged, batching whatever queued up meanwhile).
	Linger time.Duration
	// BatchBytes caps a batch (default 256 KiB).
	BatchBytes int
	// Timeout per produce request, also sent to the broker (default 30s).
	Timeout time.Duration
	// Retries for retriable errors (default 10).
	Retries int
}

// Message is a record to produce. Partition -1 (or leaving PartitionSet false) lets the producer pick:
// by key hash if Key is set, round-robin otherwise.
type Message struct {
	Topic        string
	Partition    int32
	PartitionSet bool
	Key          []byte
	Value        []byte
	Headers      []record.Header
	Timestamp    time.Time
}

// Future completes when a produced record is acknowledged.
type Future struct {
	done      chan struct{}
	Partition int32
	Offset    int64 // -1 with acks=0
	Err       error
}

// Done is closed when the result is available.
func (f *Future) Done() <-chan struct{} { return f.done }

// Wait blocks for the result.
func (f *Future) Wait(ctx context.Context) (int64, error) {
	select {
	case <-f.done:
		return f.Offset, f.Err
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

// Producer batches records per partition and sends one batch per partition at a time, which keeps
// records in order and lets the broker deduplicate retries by sequence number.
type Producer struct {
	c          *Client
	cfg        ProducerConfig
	producerID int64
	epoch      int16

	closeMu sync.RWMutex // Produce holds it shared while queueing; Close takes it to stop new records
	closed  bool
	mu      sync.Mutex
	senders map[partKey]*partSender
	rr      atomic.Uint32
	wg      sync.WaitGroup
}

type partKey struct {
	topic     string
	partition int32
}

type pendingRec struct {
	msg Message
	fut *Future
}

type partSender struct {
	p       *Producer
	key     partKey
	ch      chan pendingRec
	nextSeq int32
}

// ErrProducerClosed is returned for records produced after Close.
var ErrProducerClosed = errors.New("sluice: producer closed")

// NewProducer creates a producer. An idempotent producer asks the cluster for a producer id first.
func (c *Client) NewProducer(ctx context.Context, cfg ProducerConfig) (*Producer, error) {
	if !cfg.AcksSet && cfg.Acks == 0 {
		cfg.Acks = protocol.AcksAll
	}
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = 256 << 10
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.Retries <= 0 {
		cfg.Retries = 10
	}
	p := &Producer{c: c, cfg: cfg, producerID: record.NoProducerID, senders: map[partKey]*partSender{}}
	if !cfg.NotIdempotent {
		var resp protocol.InitProducerIDResponse
		if err := c.anyBroker(ctx, func(cn *wire.Conn) error {
			return cn.Do(ctx, protocol.APIInitProducerID, &protocol.InitProducerIDRequest{}, &resp)
		}); err != nil {
			return nil, err
		}
		p.producerID, p.epoch = resp.ProducerID, resp.Epoch
	}
	return p, nil
}

// Produce queues a record and returns a Future for its offset.
func (p *Producer) Produce(ctx context.Context, m Message) *Future {
	fut := &Future{done: make(chan struct{}), Offset: -1}
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}
	if !m.PartitionSet || m.Partition < 0 {
		n, err := p.c.Partitions(ctx, m.Topic)
		if err != nil {
			fut.Err = err
			close(fut.done)
			return fut
		}
		if m.Key != nil {
			h := fnv.New32a()
			h.Write(m.Key)
			m.Partition = int32(h.Sum32() % uint32(n))
		} else {
			m.Partition = int32(p.rr.Add(1) % uint32(n))
		}
	}
	fut.Partition = m.Partition
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()
	if p.closed {
		fut.Err = ErrProducerClosed
		close(fut.done)
		return fut
	}
	s := p.sender(partKey{m.Topic, m.Partition})
	select {
	case s.ch <- pendingRec{msg: m, fut: fut}:
	case <-ctx.Done():
		fut.Err = ctx.Err()
		close(fut.done)
	}
	return fut
}

// ProduceSync produces one record and waits for its offset.
func (p *Producer) ProduceSync(ctx context.Context, m Message) (int64, error) {
	return p.Produce(ctx, m).Wait(ctx)
}

// Close sends everything queued and stops the producer.
func (p *Producer) Close() error {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return nil
	}
	p.closed = true
	p.closeMu.Unlock()
	p.mu.Lock()
	for _, s := range p.senders {
		close(s.ch)
	}
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

func (p *Producer) sender(k partKey) *partSender {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.senders[k]
	if !ok {
		s = &partSender{p: p, key: k, ch: make(chan pendingRec, 4096)}
		p.senders[k] = s
		p.wg.Add(1)
		go s.run()
	}
	return s
}

func (s *partSender) run() {
	defer s.p.wg.Done()
	cfg := s.p.cfg
	for {
		first, ok := <-s.ch
		if !ok {
			return
		}
		batch := []pendingRec{first}
		size := estimate(first.msg)
		var linger <-chan time.Time
		var timer *time.Timer
		if cfg.Linger > 0 {
			timer = time.NewTimer(cfg.Linger)
			linger = timer.C
		}
	collect:
		for size < cfg.BatchBytes {
			if linger == nil {
				select {
				case r, ok := <-s.ch:
					if !ok {
						break collect
					}
					batch, size = append(batch, r), size+estimate(r.msg)
				default:
					break collect
				}
			} else {
				select {
				case r, ok := <-s.ch:
					if !ok {
						break collect
					}
					batch, size = append(batch, r), size+estimate(r.msg)
				case <-linger:
					break collect
				}
			}
		}
		if timer != nil {
			timer.Stop()
		}
		s.send(batch)
	}
}

func estimate(m Message) int { return len(m.Key) + len(m.Value) + 24 }

// send builds one batch, sends it with retries (same bytes, same sequence numbers) and completes the
// futures.
func (s *partSender) send(recs []pendingRec) {
	p, cfg := s.p, s.p.cfg
	seq := int32(record.NoSequence)
	if p.producerID != record.NoProducerID {
		seq = s.nextSeq
	}
	bl := record.NewBuilder(p.producerID, p.epoch, seq)
	for _, r := range recs {
		bl.Append(r.msg.Timestamp.UnixMilli(), r.msg.Key, r.msg.Value, r.msg.Headers)
	}
	batch := bl.Build()

	base, err := s.sendWithRetry(batch)
	if err == nil && p.producerID != record.NoProducerID {
		s.nextSeq += int32(len(recs))
	}
	for i, r := range recs {
		if err != nil {
			r.fut.Err = err
		} else if cfg.Acks != protocol.AcksNone {
			r.fut.Offset = base + int64(i)
		}
		close(r.fut.done)
	}
}

func (s *partSender) sendWithRetry(batch record.Batch) (int64, error) {
	p, cfg := s.p, s.p.cfg
	backoff := 20 * time.Millisecond
	var last error
	for attempt := 0; attempt <= cfg.Retries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff = min(backoff*2, time.Second)
		}
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout+5*time.Second)
		base, err := s.sendOnce(ctx, batch)
		cancel()
		if err == nil {
			return base, nil
		}
		last = err
		var code protocol.ErrorCode
		if errors.As(err, &code) && !code.Retriable() {
			return -1, err
		}
		p.c.invalidate(s.key.topic)
	}
	return -1, fmt.Errorf("sluice: produce to %s-%d failed after %d attempts: %w", s.key.topic, s.key.partition, cfg.Retries+1, last)
}

func (s *partSender) sendOnce(ctx context.Context, batch record.Batch) (int64, error) {
	p, cfg := s.p, s.p.cfg
	id, cn, err := p.c.leaderConn(ctx, s.key.topic, s.key.partition)
	if err != nil {
		return -1, err
	}
	req := &protocol.ProduceRequest{Topic: s.key.topic, Partition: s.key.partition, Acks: cfg.Acks,
		TimeoutMs: int32(cfg.Timeout / time.Millisecond), Batch: batch}
	if cfg.Acks == protocol.AcksNone {
		return -1, cn.Send(ctx, protocol.APIProduce, req)
	}
	var resp protocol.ProduceResponse
	if err := cn.Do(ctx, protocol.APIProduce, req, &resp); err != nil {
		p.c.dropConn(id, cn)
		return -1, err
	}
	if resp.Error != protocol.ErrNone {
		return -1, resp.Error
	}
	return resp.BaseOffset, nil
}
