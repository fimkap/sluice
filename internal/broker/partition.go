package broker

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/fimkap/sluice/internal/logstore"
	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/record"
)

// partition is this broker's replica of one topic partition.
type partition struct {
	b        *Broker
	topic    string
	id       int32
	key      string
	replicas []int32
	leader   int32
	log      *logstore.Log

	appendMu  sync.Mutex // serializes the idempotence check with the append
	producers map[int64]*producerState

	mu        sync.Mutex
	hw        logstore.Position // high watermark: never moves backwards
	followers map[int32]*followerState
	isr       []int32
	notify    chan struct{} // closed and replaced whenever LEO, acknowledged LEO or HW move

	cancel  context.CancelFunc // stops the replica fetcher (followers only)
	fetchWG sync.WaitGroup
}

// followerState is what a leader knows about one follower.
type followerState struct {
	acked         logstore.Position // the follower's acknowledged LEO, as its last fetch reported
	lastCaughtUp  time.Time
	lastFetchLEO  int64 // the leader's LEO when the previous fetch arrived
	lastFetchTime time.Time
}

func openPartition(b *Broker, topic string, id int32, replicas []int32) (*partition, error) {
	p := &partition{
		b:         b,
		topic:     topic,
		id:        id,
		key:       fmt.Sprintf("%s-%d", topic, id),
		replicas:  replicas,
		leader:    replicas[0],
		producers: map[int64]*producerState{},
		notify:    make(chan struct{}),
	}
	cfg := b.cfg.Log
	cfg.Logger = b.cfg.Logger
	cfg.OnChange = p.onLogChange
	log, err := logstore.Open(filepath.Join(b.cfg.DataDir, p.key), cfg)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", p.key, err)
	}
	p.log = log

	if p.isLeader() {
		now := time.Now()
		p.followers = map[int32]*followerState{}
		for _, r := range replicas[1:] {
			// Followers get one lag window to report in before they leave the ISR.
			p.followers[r] = &followerState{lastCaughtUp: now}
		}
		p.mu.Lock()
		p.recomputeHWLocked(now)
		p.mu.Unlock()
		if err := p.rebuildProducers(); err != nil {
			log.Close()
			return nil, err
		}
	} else {
		ctx, cancel := context.WithCancel(b.ctx)
		p.cancel = cancel
		p.fetchWG.Add(1)
		go p.runFetcher(ctx)
	}
	p.registerMetrics()
	return p, nil
}

func (p *partition) isLeader() bool { return p.leader == p.b.cfg.NodeID }

func (p *partition) close() error {
	if p.cancel != nil {
		p.cancel()
		p.fetchWG.Wait()
	}
	for _, name := range []string{"sluice_partition_high_watermark", "sluice_partition_log_end_offset",
		"sluice_partition_isr_size", "sluice_log_fsyncs_total", "sluice_log_appends_total"} {
		p.b.registry.Remove(name, fmt.Sprintf("%q", p.key))
	}
	return p.log.Close()
}

func (p *partition) registerMetrics() {
	r, lbl := p.b.registry, []string{"partition", p.key}
	r.GaugeFunc("sluice_partition_high_watermark", "High watermark.", func() float64 { return float64(p.highWatermark().Offset) }, lbl...)
	r.GaugeFunc("sluice_partition_log_end_offset", "Log end offset.", func() float64 { return float64(p.log.LEO().Offset) }, lbl...)
	r.GaugeFunc("sluice_partition_isr_size", "In-sync replicas (leader's view).", func() float64 { return float64(len(p.isrSnapshot())) }, lbl...)
	r.GaugeFunc("sluice_log_fsyncs_total", "fsync calls on the partition log.", func() float64 { return float64(p.log.Stats().Fsyncs) }, lbl...)
	r.GaugeFunc("sluice_log_appends_total", "Batches appended to the partition log.", func() float64 { return float64(p.log.Stats().Appends) }, lbl...)
}

// changed returns a channel that is closed at the next state change. Take it before reading the state
// you wait on, so a change in between is not missed.
func (p *partition) changed() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.notify
}

func (p *partition) signalLocked() {
	close(p.notify)
	p.notify = make(chan struct{})
}

// onLogChange runs after the log end offset or the acknowledged offset moved.
func (p *partition) onLogChange() {
	p.mu.Lock()
	if p.isLeader() {
		p.recomputeHWLocked(time.Now())
	}
	p.signalLocked()
	p.mu.Unlock()
}

func (p *partition) highWatermark() logstore.Position {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hw
}

func (p *partition) isrSnapshot() []int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int32(nil), p.isr...)
}

// recomputeHWLocked recomputes the ISR and the high watermark from the acknowledged LEOs.
func (p *partition) recomputeHWLocked(now time.Time) {
	leaderAck := p.log.Acked()
	var hw logstore.Position
	switch p.b.cfg.CommitRule {
	case CommitQuorum:
		positions := []logstore.Position{leaderAck}
		for _, f := range p.followers {
			positions = append(positions, f.acked)
		}
		sort.Slice(positions, func(i, j int) bool { return positions[i].Offset > positions[j].Offset })
		hw = positions[len(positions)/2] // replicated on a majority
		isr := []int32{p.b.cfg.NodeID}
		for id, f := range p.followers {
			if f.acked.Offset >= hw.Offset {
				isr = append(isr, id)
			}
		}
		p.isr = sortIDs(isr)
	default:
		isr := []int32{p.b.cfg.NodeID}
		hw = leaderAck
		for id, f := range p.followers {
			if now.Sub(f.lastCaughtUp) > p.b.cfg.ReplicaLagMax {
				continue
			}
			isr = append(isr, id)
			if f.acked.Offset < hw.Offset {
				hw = f.acked
			}
		}
		p.isr = sortIDs(isr)
	}
	if hw.Offset > p.hw.Offset {
		p.hw = hw
	}
}

// updateFollower records a follower's fetch: its fetch offset is its acknowledged LEO. It returns
// ErrOffsetOutOfRange if the follower is ahead of the leader.
func (p *partition) updateFollower(replica int32, fetchOffset int64) protocol.ErrorCode {
	if fetchOffset > p.log.LEO().Offset {
		return protocol.ErrOffsetOutOfRange
	}
	pos, err := p.log.PositionOf(fetchOffset)
	if err != nil {
		return protocol.ErrOffsetOutOfRange
	}
	now := time.Now()
	leo := p.log.LEO().Offset
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.followers[replica]
	if !ok {
		return protocol.ErrNotLeader // not a replica of this partition
	}
	f.acked = pos // the follower's report is authoritative, even if it went back after a truncation
	// Kafka's catch-up rule: a follower is caught up now if it reached our LEO, or caught up as of its
	// previous fetch if it reached the LEO we had back then.
	if fetchOffset >= leo {
		f.lastCaughtUp = now
	} else if fetchOffset >= f.lastFetchLEO && !f.lastFetchTime.IsZero() {
		f.lastCaughtUp = f.lastFetchTime
	}
	f.lastFetchLEO, f.lastFetchTime = leo, now
	old := p.hw
	p.recomputeHWLocked(now)
	if p.hw != old {
		p.signalLocked()
	}
	return protocol.ErrNone
}

// refreshLocked recomputes the ISR and HW now, so a follower that stopped fetching drops out of the ISR
// on time even when nothing else happens, and wakes waiters if the HW moved.
func (p *partition) refreshLocked() {
	if !p.isLeader() || len(p.followers) == 0 {
		return
	}
	old := p.hw
	p.recomputeHWLocked(time.Now())
	if p.hw != old {
		p.signalLocked()
	}
}

// currentISR refreshes and returns the ISR.
func (p *partition) currentISR() []int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refreshLocked()
	return append([]int32(nil), p.isr...)
}

// waitHW blocks until the high watermark reaches end.
func (p *partition) waitHW(ctx context.Context, end int64) error {
	for {
		ch := p.changed()
		p.mu.Lock()
		p.refreshLocked()
		hw := p.hw.Offset
		p.mu.Unlock()
		if hw >= end {
			return nil
		}
		t := time.NewTimer(p.b.cfg.ReplicaLagMax / 4)
		select {
		case <-ch:
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		t.Stop()
	}
}

// producerState is the dedup window for one idempotent producer on one partition.
type producerState struct {
	epoch   int16
	batches []batchMeta // the last maxCachedBatches batches, oldest first
}

type batchMeta struct {
	firstSeq, lastSeq int32
	base, next        int64
}

const maxCachedBatches = 5

// appendBatch runs the idempotence check and appends. For a retried batch it returns the original
// offsets and dup=true without appending.
func (p *partition) appendBatch(batch record.Batch) (base, next int64, dup bool, code protocol.ErrorCode, err error) {
	p.appendMu.Lock()
	defer p.appendMu.Unlock()

	pid := batch.ProducerID()
	var ps *producerState
	if pid >= 0 && batch.BaseSequence() >= 0 {
		ps = p.producers[pid]
		if ps != nil {
			switch {
			case batch.ProducerEpoch() < ps.epoch:
				return 0, 0, false, protocol.ErrInvalidRequest, nil // a fenced, older producer instance
			case batch.ProducerEpoch() > ps.epoch:
				ps = &producerState{epoch: batch.ProducerEpoch()}
				p.producers[pid] = ps
			default:
				for _, m := range ps.batches {
					if m.firstSeq == batch.BaseSequence() && m.lastSeq == batch.LastSequence() {
						return m.base, m.next, true, protocol.ErrNone, nil
					}
				}
				if n := len(ps.batches); n > 0 && batch.BaseSequence() != ps.batches[n-1].lastSeq+1 {
					return 0, 0, false, protocol.ErrOutOfOrderSequence, nil
				}
			}
		}
	}

	res, err := p.log.Append(batch)
	if err != nil {
		return 0, 0, false, protocol.ErrInternal, err
	}
	if pid >= 0 && batch.BaseSequence() >= 0 {
		if ps == nil {
			ps = &producerState{epoch: batch.ProducerEpoch()}
			p.producers[pid] = ps
		}
		ps.remember(batch, res.Base, res.End.Offset)
	}
	return res.Base, res.End.Offset, false, protocol.ErrNone, nil
}

func (ps *producerState) remember(b record.Batch, base, next int64) {
	ps.batches = append(ps.batches, batchMeta{firstSeq: b.BaseSequence(), lastSeq: b.LastSequence(), base: base, next: next})
	if len(ps.batches) > maxCachedBatches {
		ps.batches = ps.batches[1:]
	}
}

// rebuildProducers restores the dedup windows from the active segment after a restart.
func (p *partition) rebuildProducers() error {
	from := p.log.ActiveBase()
	end := p.log.LEO()
	var buf []byte
	for from < end.Offset {
		rr, err := p.log.Read(from, 1<<20, end)
		if err != nil {
			return fmt.Errorf("rebuild producer state for %s: %w", p.key, err)
		}
		if rr == nil {
			break
		}
		buf, err = rr.ReadAll(buf)
		rr.Release()
		if err != nil {
			return err
		}
		data := buf
		advanced := false
		for {
			b, rest, ok := record.Next(data)
			if !ok {
				break
			}
			data = rest
			if b.ValidateHeader() == nil && b.ProducerID() >= 0 && b.BaseSequence() >= 0 {
				ps := p.producers[b.ProducerID()]
				if ps == nil || b.ProducerEpoch() > ps.epoch {
					ps = &producerState{epoch: b.ProducerEpoch()}
					p.producers[b.ProducerID()] = ps
				}
				ps.remember(b, b.BaseOffset(), b.NextOffset())
			}
			from = b.NextOffset()
			advanced = true
		}
		if !advanced {
			break
		}
	}
	return nil
}

func sortIDs(ids []int32) []int32 {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
