package broker

import (
	"context"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/record"
)

const defaultProduceTimeout = 30 * time.Second

// handleProduce validates and appends on the connection's read loop, so appends from one connection
// keep their order, then waits for the acknowledgement on a goroutine.
func (b *Broker) handleProduce(c *conn, h protocol.RequestHeader, r *protocol.ProduceRequest) {
	start := time.Now()
	respond := func(resp *protocol.ProduceResponse) {
		if r.Acks == protocol.AcksNone {
			return // acks=0: the producer does not wait for, or get, a response
		}
		b.metrics.produceTime.Observe(time.Since(start).Seconds())
		c.reply(h, resp)
	}

	p, code := b.topics.leaderPartition(r.Topic, r.Partition)
	if code != protocol.ErrNone {
		respond(&protocol.ProduceResponse{Error: code})
		return
	}
	batch := record.Batch(r.Batch)
	if len(batch) > int(b.cfg.Log.SegmentBytes) && b.cfg.Log.SegmentBytes > 0 {
		respond(&protocol.ProduceResponse{Error: protocol.ErrMessageTooLarge})
		return
	}
	if err := batch.Validate(); err != nil {
		respond(&protocol.ProduceResponse{Error: protocol.ErrCorruptMessage})
		return
	}
	checkISR := r.Acks == protocol.AcksAll && b.cfg.CommitRule == CommitISR && len(p.replicas) > 1
	if checkISR && len(p.currentISR()) < b.cfg.MinInsync {
		respond(&protocol.ProduceResponse{Error: protocol.ErrNotEnoughReplicas})
		return
	}

	base, next, _, code, err := p.appendBatch(batch)
	if err != nil {
		b.cfg.Logger.Error("append failed", "partition", p.key, "err", err)
	}
	if code != protocol.ErrNone {
		respond(&protocol.ProduceResponse{Error: code})
		return
	}
	b.metrics.produceBytes.Add(uint64(len(batch)))
	if r.Acks == protocol.AcksNone {
		return
	}

	timeout := time.Duration(r.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultProduceTimeout
	}
	c.async(func() {
		ctx, cancel := context.WithTimeout(b.ctx, timeout)
		defer cancel()
		go func() { // a closed connection abandons the wait
			select {
			case <-c.closed:
				cancel()
			case <-ctx.Done():
			}
		}()
		var err error
		if r.Acks == protocol.AcksAll {
			err = p.waitHW(ctx, next)
		} else {
			err = p.log.WaitAcked(ctx.Done(), next)
		}
		if err != nil {
			respond(&protocol.ProduceResponse{Error: protocol.ErrRequestTimedOut})
			return
		}
		// Kafka's NOT_ENOUGH_REPLICAS_AFTER_APPEND: the HW passed the batch, but only because the ISR
		// shrank below the minimum while we waited.
		if checkISR && len(p.currentISR()) < b.cfg.MinInsync {
			respond(&protocol.ProduceResponse{Error: protocol.ErrNotEnoughReplicas})
			return
		}
		respond(&protocol.ProduceResponse{BaseOffset: base, LogStart: p.log.LogStart()})
	})
}
