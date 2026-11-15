package broker

import (
	"context"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/record"
)

// runFetcher is the follower side of replication: fetch from the leader at our log end offset, append the
// batches with the leader's offsets, wait until they are acknowledged locally, and fetch again. The next
// fetch's offset is how the leader learns what we have.
func (p *partition) runFetcher(ctx context.Context) {
	defer p.fetchWG.Done()
	b := p.b
	backoff := 50 * time.Millisecond
	sleep := func(d time.Duration) bool {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	fail := func(msg string, args ...any) bool {
		b.cfg.Logger.Debug(msg, append(args, "partition", p.key)...)
		ok := sleep(backoff)
		backoff = min(backoff*2, time.Second)
		return ok
	}

	for ctx.Err() == nil {
		if d := b.cfg.ChaosReplicaDelay; d > 0 && !sleep(d) {
			return
		}
		pc, err := b.peer(ctx, p.leader)
		if err != nil {
			if !fail("replica fetch: dial leader", "err", err) {
				return
			}
			continue
		}
		leo := p.log.LEO().Offset
		req := &protocol.FetchRequest{
			ReplicaID:   b.cfg.NodeID,
			MaxWaitMs:   int32(b.cfg.ReplicaFetchWait / time.Millisecond),
			Topic:       p.topic,
			Partition:   p.id,
			FetchOffset: leo,
			MaxBytes:    int32(b.cfg.ReplicaFetchBytes),
		}
		var resp protocol.FetchResponse
		rctx, cancel := context.WithTimeout(ctx, b.cfg.ReplicaFetchWait+5*time.Second)
		err = pc.Do(rctx, protocol.APIFetch, req, &resp)
		cancel()
		if err != nil {
			if !fail("replica fetch failed", "err", err) {
				return
			}
			continue
		}
		switch resp.Error {
		case protocol.ErrNone:
		case protocol.ErrOffsetOutOfRange:
			// Our log is ahead of the leader's (it lost an unsynced tail) or behind its log start.
			if leo > resp.LogEnd {
				b.cfg.Logger.Warn("follower ahead of leader, truncating", "partition", p.key, "from", leo, "to", resp.LogEnd)
				if err := p.log.TruncateTo(resp.LogEnd); err != nil {
					b.cfg.Logger.Error("truncate failed", "partition", p.key, "err", err)
				}
			} else if leo < resp.LogStart {
				if err := p.log.ResetTo(resp.LogStart); err != nil {
					b.cfg.Logger.Error("reset failed", "partition", p.key, "err", err)
				}
			}
			continue
		default:
			if !fail("replica fetch error", "code", resp.Error.String()) {
				return
			}
			continue
		}
		backoff = 50 * time.Millisecond

		data := resp.Records
		appended := false
		for {
			batch, rest, ok := record.Next(data)
			if !ok {
				break // a partial batch at the end comes again with the next fetch
			}
			data = rest
			if batch.NextOffset() <= p.log.LEO().Offset {
				continue
			}
			if err := batch.ValidateHeader(); err != nil {
				b.cfg.Logger.Error("leader sent a corrupt batch", "partition", p.key, "err", err)
				break
			}
			if _, err := p.log.AppendReplica(batch); err != nil {
				b.cfg.Logger.Error("replica append failed", "partition", p.key, "err", err)
				break
			}
			appended = true
		}
		if appended {
			// Report only acknowledged data: under always/group that means durable.
			if err := p.log.WaitAcked(ctx.Done(), p.log.LEO().Offset); err != nil {
				return
			}
		}
		p.mu.Lock()
		if hw := min(resp.HighWatermark, p.log.LEO().Offset); hw > p.hw.Offset {
			p.hw.Offset = hw // followers only report it; nobody reads through a follower
		}
		p.mu.Unlock()
	}
}
