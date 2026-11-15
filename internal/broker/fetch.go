package broker

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/fimkap/sluice/internal/logstore"
	"github.com/fimkap/sluice/internal/protocol"
)

const maxFetchWait = 30 * time.Second

// handleFetch serves consumers (up to the high watermark) and followers (up to the log end offset, and
// their fetch offset updates the ISR and HW). With nothing to return it long-polls until data arrives or
// MaxWaitMs passes.
func (b *Broker) handleFetch(c *conn, h protocol.RequestHeader, r *protocol.FetchRequest) {
	p, code := b.topics.leaderPartition(r.Topic, r.Partition)
	if code != protocol.ErrNone {
		c.reply(h, &protocol.FetchResponse{Error: code})
		return
	}
	replica := r.ReplicaID >= 0
	if replica {
		if code := p.updateFollower(r.ReplicaID, r.FetchOffset); code != protocol.ErrNone {
			c.reply(h, &protocol.FetchResponse{Error: code, HighWatermark: p.highWatermark().Offset,
				LogStart: p.log.LogStart(), LogEnd: p.log.LEO().Offset})
			return
		}
	}
	maxBytes := int(r.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	wait := min(time.Duration(r.MaxWaitMs)*time.Millisecond, maxFetchWait)
	deadline := time.Now().Add(wait)

	for {
		ch := p.changed() // before reading state: a change after this wakes us
		hw := p.highWatermark()
		leo := p.log.LEO()
		limit := hw
		if replica {
			limit = leo
		}
		rr, err := p.log.Read(r.FetchOffset, maxBytes, limit)
		if err != nil {
			code := protocol.ErrInternal
			if errors.Is(err, logstore.ErrOffsetOutOfRange) {
				code = protocol.ErrOffsetOutOfRange
			}
			c.reply(h, &protocol.FetchResponse{Error: code, HighWatermark: hw.Offset, LogStart: p.log.LogStart(), LogEnd: leo.Offset})
			return
		}
		if rr != nil {
			b.sendFetch(c, h, &protocol.FetchResponse{HighWatermark: hw.Offset, LogStart: p.log.LogStart(), LogEnd: leo.Offset}, rr)
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.reply(h, &protocol.FetchResponse{HighWatermark: hw.Offset, LogStart: p.log.LogStart(), LogEnd: leo.Offset})
			return
		}
		t := time.NewTimer(remaining)
		select {
		case <-ch:
		case <-t.C:
		case <-c.closed:
			t.Stop()
			return
		case <-b.ctx.Done():
			t.Stop()
			return
		}
		t.Stop()
	}
}

// sendFetch queues a fetch response whose record bytes the writer streams from the segment file.
func (b *Broker) sendFetch(c *conn, h protocol.RequestHeader, resp *protocol.FetchResponse, rr *logstore.ReadResult) {
	w := &protocol.Writer{B: make([]byte, 8, 8+protocol.FetchResponseHeaderSize)}
	binary.BigEndian.PutUint32(w.B[4:], uint32(h.CorrelationID))
	resp.EncodeHeader(w, int(rr.Len))
	binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4+int(rr.Len)))
	c.send(outMsg{frame: w.B, records: rr})
}
