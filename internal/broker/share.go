package broker

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/record"
)

// Share groups (KIP-932): queue semantics on a log. Each (group, partition) keeps a window of in-flight
// records between the share-partition start offset (SPSO, everything below is finished) and its end
// offset (SPEO). Records move Available -> Acquired (with a lock) -> Acknowledged or Archived, and the
// SPSO advances over the finished prefix.

type recState uint8

const (
	stAvailable recState = iota
	stAcquired
	stAcked
	stArchived
)

type inflight struct {
	state      recState
	deliveries int16
	deadline   time.Time
	member     string
}

type shareKey struct {
	group     string
	topic     string
	partition int32
}

type shareManager struct {
	b     *Broker
	mu    sync.Mutex
	parts map[shareKey]*sharePartition
}

type sharePartition struct {
	mgr       *shareManager
	p         *partition
	mu        sync.Mutex
	spso      int64
	window    []inflight // window[i] is offset spso+i; spso+len(window) is the SPEO
	ckptPath  string
	savedSPSO int64
	lastSave  time.Time
}

func newShareManager(b *Broker) *shareManager {
	sm := &shareManager{b: b, parts: map[shareKey]*sharePartition{}}
	b.registry.GaugeFunc("sluice_share_acquired_records", "Share-group records currently acquired (locked).", func() float64 {
		return float64(sm.acquiredCount())
	})
	return sm
}

func (sm *shareManager) get(group string, p *partition, earliest bool) *sharePartition {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	k := shareKey{group, p.topic, p.id}
	if sp, ok := sm.parts[k]; ok {
		return sp
	}
	sp := &sharePartition{mgr: sm, p: p,
		ckptPath: filepath.Join(sm.b.cfg.DataDir, "meta", "share", url.PathEscape(group), p.key+".json")}
	var ck struct {
		SPSO int64 `json:"spso"`
	}
	if data, err := os.ReadFile(sp.ckptPath); err == nil && json.Unmarshal(data, &ck) == nil {
		sp.spso = ck.SPSO
	} else if earliest {
		sp.spso = p.log.LogStart()
	} else {
		sp.spso = p.highWatermark().Offset
	}
	sp.spso = max(sp.spso, p.log.LogStart())
	sp.savedSPSO = sp.spso
	sm.parts[k] = sp
	return sp
}

func (sm *shareManager) lookup(group string, p *partition) *sharePartition {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.parts[shareKey{group, p.topic, p.id}]
}

func (sm *shareManager) acquiredCount() int {
	sm.mu.Lock()
	parts := make([]*sharePartition, 0, len(sm.parts))
	for _, sp := range sm.parts {
		parts = append(parts, sp)
	}
	sm.mu.Unlock()
	n := 0
	for _, sp := range parts {
		sp.mu.Lock()
		for _, r := range sp.window {
			if r.state == stAcquired {
				n++
			}
		}
		sp.mu.Unlock()
	}
	return n
}

func (sm *shareManager) checkpointAll() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, sp := range sm.parts {
		sp.mu.Lock()
		sp.saveLocked(time.Time{}, true)
		sp.mu.Unlock()
	}
}

// acquire hands out up to max records to member: first records that are available again (released or
// with an expired lock), then new records up to the high watermark, within the in-flight limit.
func (sp *sharePartition) acquire(member string, max int, now time.Time) []protocol.AcquiredRange {
	cfg := &sp.mgr.b.cfg
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.expireLocked(now)
	if start := sp.p.log.LogStart(); sp.spso < start { // retention overtook the window
		drop := min(int(start-sp.spso), len(sp.window))
		sp.window = sp.window[drop:]
		sp.spso = start
	}

	var ranges []protocol.AcquiredRange
	take := func(i int) {
		r := &sp.window[i]
		r.state, r.deliveries, r.deadline, r.member = stAcquired, r.deliveries+1, now.Add(cfg.ShareLockDuration), member
		off := sp.spso + int64(i)
		if n := len(ranges); n > 0 && ranges[n-1].Last == off-1 && ranges[n-1].DeliveryCount == r.deliveries {
			ranges[n-1].Last = off
		} else {
			ranges = append(ranges, protocol.AcquiredRange{First: off, Last: off, DeliveryCount: r.deliveries})
		}
	}
	count := 0
	for i := range sp.window {
		if count == max {
			break
		}
		if sp.window[i].state == stAvailable {
			take(i)
			count++
		}
	}
	hw := sp.p.highWatermark().Offset
	for count < max && sp.spso+int64(len(sp.window)) < hw && len(sp.window) < cfg.ShareMaxInFlight {
		sp.window = append(sp.window, inflight{})
		take(len(sp.window) - 1)
		count++
	}
	return ranges
}

// expireLocked returns records with an expired lock to Available, or archives them at the delivery limit.
func (sp *sharePartition) expireLocked(now time.Time) {
	maxDel := int16(sp.mgr.b.cfg.ShareMaxDeliveries)
	for i := range sp.window {
		r := &sp.window[i]
		if r.state == stAcquired && now.After(r.deadline) {
			if r.deliveries >= maxDel {
				r.state = stArchived
			} else {
				r.state = stAvailable
			}
			r.member = ""
		}
	}
	sp.advanceLocked(now)
}

// ack applies acknowledgements from member. Acknowledging a record the member doesn't hold (never
// acquired, lock expired, or now held by someone else) is INVALID_RECORD_STATE; the rest still apply.
func (sp *sharePartition) ack(member string, acks []protocol.ShareAck, now time.Time) protocol.ErrorCode {
	if len(acks) == 0 {
		return protocol.ErrNone
	}
	cfg := &sp.mgr.b.cfg
	sp.mu.Lock()
	defer sp.mu.Unlock()
	code := protocol.ErrNone
	for _, a := range acks {
		if a.Last < a.First || a.Last-a.First > int64(cfg.ShareMaxInFlight) {
			code = protocol.ErrInvalidRequest
			continue
		}
		for off := a.First; off <= a.Last; off++ {
			i := off - sp.spso
			if i < 0 || i >= int64(len(sp.window)) {
				code = protocol.ErrInvalidRecordState
				continue
			}
			r := &sp.window[i]
			if r.state != stAcquired || r.member != member || now.After(r.deadline) {
				code = protocol.ErrInvalidRecordState
				continue
			}
			switch a.Type {
			case protocol.AckAccept:
				r.state = stAcked
			case protocol.AckReject:
				r.state = stArchived
			case protocol.AckRelease:
				if r.deliveries >= int16(cfg.ShareMaxDeliveries) {
					r.state = stArchived
				} else {
					r.state = stAvailable
				}
				r.member = ""
			case protocol.AckRenew:
				r.deadline = now.Add(cfg.ShareLockDuration)
			default:
				code = protocol.ErrInvalidRequest
			}
		}
	}
	sp.advanceLocked(now)
	return code
}

// advanceLocked moves the SPSO over the finished prefix of the window.
func (sp *sharePartition) advanceLocked(now time.Time) {
	k := 0
	for k < len(sp.window) && (sp.window[k].state == stAcked || sp.window[k].state == stArchived) {
		k++
	}
	if k == 0 {
		return
	}
	sp.spso += int64(k)
	sp.window = sp.window[k:]
	if cap(sp.window) > 4*len(sp.window)+1024 { // don't let the re-sliced array grow without bound
		sp.window = append([]inflight(nil), sp.window...)
	}
	sp.saveLocked(now, false)
}

// saveLocked checkpoints the SPSO, at most every 100 ms unless forced. A restart redelivers whatever was
// in flight above the checkpoint: at-least-once.
func (sp *sharePartition) saveLocked(now time.Time, force bool) {
	if sp.spso == sp.savedSPSO || (!force && now.Sub(sp.lastSave) < 100*time.Millisecond) {
		return
	}
	data, _ := json.Marshal(map[string]int64{"spso": sp.spso})
	if err := writeFileAtomic(sp.ckptPath, data, false); err != nil {
		sp.mgr.b.cfg.Logger.Warn("share checkpoint failed", "path", sp.ckptPath, "err", err)
		return
	}
	sp.savedSPSO, sp.lastSave = sp.spso, now
}

// nextDeadline is the earliest lock expiry, or the zero time if nothing is acquired.
func (sp *sharePartition) nextDeadline() time.Time {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	var next time.Time
	for _, r := range sp.window {
		if r.state == stAcquired && (next.IsZero() || r.deadline.Before(next)) {
			next = r.deadline
		}
	}
	return next
}

func (b *Broker) handleShareFetch(c *conn, h protocol.RequestHeader, r *protocol.ShareFetchRequest) {
	p, code := b.topics.leaderPartition(r.Topic, r.Partition)
	if code != protocol.ErrNone {
		c.reply(h, &protocol.ShareFetchResponse{Error: code})
		return
	}
	if r.MemberID == "" || r.GroupID == "" {
		c.reply(h, &protocol.ShareFetchResponse{Error: protocol.ErrInvalidRequest})
		return
	}
	sp := b.shares.get(r.GroupID, p, r.ResetToEarliest)
	ackErr := sp.ack(r.MemberID, r.Acks, time.Now())
	maxRecords := int(r.MaxRecords)
	if maxRecords <= 0 {
		maxRecords = 500
	}
	deadline := time.Now().Add(min(time.Duration(r.MaxWaitMs)*time.Millisecond, maxFetchWait))
	for {
		ch := p.changed()
		ranges := sp.acquire(r.MemberID, maxRecords, time.Now())
		if len(ranges) > 0 {
			records, err := b.readRanges(p, ranges)
			if err != nil {
				b.cfg.Logger.Error("share fetch read", "partition", p.key, "err", err)
				c.reply(h, &protocol.ShareFetchResponse{Error: protocol.ErrInternal, AckError: ackErr})
				return
			}
			c.reply(h, &protocol.ShareFetchResponse{AckError: ackErr, Acquired: ranges, Records: records})
			return
		}
		now := time.Now()
		if !now.Before(deadline) {
			c.reply(h, &protocol.ShareFetchResponse{AckError: ackErr})
			return
		}
		wake := deadline
		if nd := sp.nextDeadline(); !nd.IsZero() && nd.Before(wake) {
			wake = nd.Add(time.Millisecond)
		}
		t := time.NewTimer(time.Until(wake))
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

// readRanges returns the batches that overlap the acquired ranges (ascending).
func (b *Broker) readRanges(p *partition, ranges []protocol.AcquiredRange) ([]byte, error) {
	var out []byte
	next, last := ranges[0].First, ranges[len(ranges)-1].Last
	limit := p.highWatermark()
	ri := 0
	for next <= last {
		rr, err := p.log.Read(next, 1<<20, limit)
		if err != nil {
			return nil, err
		}
		if rr == nil {
			break
		}
		data, err := rr.ReadAll(nil)
		rr.Release()
		if err != nil {
			return nil, err
		}
		progressed := false
		for {
			batch, rest, ok := record.Next(data)
			if !ok {
				break
			}
			data = rest
			progressed = true
			for ri < len(ranges) && ranges[ri].Last < batch.BaseOffset() {
				ri++
			}
			if ri < len(ranges) && ranges[ri].First <= batch.LastOffset() {
				out = append(out, batch...)
			}
			next = batch.NextOffset()
			if next > last {
				break
			}
		}
		if !progressed {
			break
		}
	}
	return out, nil
}

func (b *Broker) handleShareAcknowledge(r *protocol.ShareAcknowledgeRequest) *protocol.ShareAcknowledgeResponse {
	p, code := b.topics.leaderPartition(r.Topic, r.Partition)
	if code != protocol.ErrNone {
		return &protocol.ShareAcknowledgeResponse{Error: code}
	}
	sp := b.shares.lookup(r.GroupID, p)
	if sp == nil {
		return &protocol.ShareAcknowledgeResponse{Error: protocol.ErrInvalidRecordState}
	}
	return &protocol.ShareAcknowledgeResponse{Error: sp.ack(r.MemberID, r.Acks, time.Now())}
}
