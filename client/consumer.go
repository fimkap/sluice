package client

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/wire"
)

// TopicPartition names one partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// ConsumerConfig configures a group Consumer.
type ConsumerConfig struct {
	Group  string
	Topics []string
	// FromEarliest starts partitions without a committed offset at the log start (default: the end).
	FromEarliest bool
	// MaxBytes per partition fetch (default 1 MiB).
	MaxBytes int32
	// MaxWait is how long a fetch waits for data (default 500 ms).
	MaxWait time.Duration
	// AutoCommit commits the positions reached by the previous Poll at the start of each Poll, and when
	// partitions are revoked or the consumer closes: at-least-once delivery.
	AutoCommit bool
}

// Consumer is a member of a consumer group. The coordinator assigns it partitions; it reads them in
// order and commits offsets. Poll is not safe for concurrent use; one goroutine should drive a Consumer.
type Consumer struct {
	c   *Client
	cfg ConsumerConfig

	mu       sync.Mutex
	coord    *wire.Conn
	memberID string
	epoch    int32
	interval time.Duration
	target   map[TopicPartition]bool // latest assignment from the coordinator
	owned    map[TopicPartition]*partState
	gen      int
	dirty    bool

	results chan fetchResult
	stop    chan struct{}
	wg      sync.WaitGroup
}

type partState struct {
	pos      int64
	gen      int
	inflight bool
}

type fetchResult struct {
	tp   TopicPartition
	gen  int
	pos  int64
	resp *protocol.FetchResponse
	err  error
}

// NewConsumer joins the group and starts heartbeating.
func (c *Client) NewConsumer(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Group == "" || len(cfg.Topics) == 0 {
		return nil, errors.New("sluice: consumer needs a group and topics")
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1 << 20
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 500 * time.Millisecond
	}
	cs := &Consumer{c: c, cfg: cfg, interval: time.Second, target: map[TopicPartition]bool{},
		owned: map[TopicPartition]*partState{}, results: make(chan fetchResult, 1024), stop: make(chan struct{})}
	if err := cs.heartbeat(ctx); err != nil {
		return nil, err
	}
	cs.wg.Add(1)
	go cs.heartbeatLoop()
	return cs, nil
}

// MemberID is the id the coordinator gave this member.
func (cs *Consumer) MemberID() string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.memberID
}

// Assignment returns the partitions this consumer currently reads.
func (cs *Consumer) Assignment() []TopicPartition {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make([]TopicPartition, 0, len(cs.owned))
	for tp := range cs.owned {
		out = append(out, tp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

func (cs *Consumer) heartbeatLoop() {
	defer cs.wg.Done()
	for {
		cs.mu.Lock()
		d := cs.interval
		cs.mu.Unlock()
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-cs.stop:
			t.Stop()
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = cs.heartbeat(ctx)
		cancel()
	}
}

// heartbeat reports what the member owns and receives its assignment.
func (cs *Consumer) heartbeat(ctx context.Context) error {
	cs.mu.Lock()
	req := &protocol.GroupHeartbeatRequest{GroupID: cs.cfg.Group, MemberID: cs.memberID, MemberEpoch: cs.epoch, Topics: cs.cfg.Topics}
	for _, tp := range sortedTPs(cs.owned) {
		if n := len(req.Owned); n > 0 && req.Owned[n-1].Topic == tp.Topic {
			req.Owned[n-1].Partitions = append(req.Owned[n-1].Partitions, tp.Partition)
		} else {
			req.Owned = append(req.Owned, protocol.TopicPartitions{Topic: tp.Topic, Partitions: []int32{tp.Partition}})
		}
	}
	coord := cs.coord
	cs.mu.Unlock()

	if coord == nil || coord.Err() != nil {
		var err error
		if coord, err = cs.c.coordinatorConn(ctx, cs.cfg.Group); err != nil {
			return err
		}
		cs.mu.Lock()
		cs.coord = coord
		cs.mu.Unlock()
	}
	var resp protocol.GroupHeartbeatResponse
	if err := coord.Do(ctx, protocol.APIGroupHeartbeat, req, &resp); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	switch resp.Error {
	case protocol.ErrNone:
	case protocol.ErrUnknownMember: // timed out: rejoin as a new member; our partitions are gone
		cs.memberID, cs.epoch = "", 0
		cs.target = map[TopicPartition]bool{}
		return resp.Error
	case protocol.ErrNotCoordinator:
		cs.coord = nil
		return resp.Error
	default:
		return resp.Error
	}
	cs.memberID, cs.epoch = resp.MemberID, resp.MemberEpoch
	if resp.HeartbeatIntervalMs > 0 {
		cs.interval = time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond
	}
	cs.target = map[TopicPartition]bool{}
	for _, tp := range resp.Assignment {
		for _, p := range tp.Partitions {
			cs.target[TopicPartition{tp.Topic, p}] = true
		}
	}
	return nil
}

// applyAssignment drops revoked partitions (committing them first with AutoCommit) and starts newly
// assigned ones from their committed offset.
func (cs *Consumer) applyAssignment(ctx context.Context) error {
	cs.mu.Lock()
	var revoked, added []TopicPartition
	for tp := range cs.owned {
		if !cs.target[tp] {
			revoked = append(revoked, tp)
		}
	}
	for tp := range cs.target {
		if _, ok := cs.owned[tp]; !ok {
			added = append(added, tp)
		}
	}
	cs.mu.Unlock()
	if len(revoked) == 0 && len(added) == 0 {
		return nil
	}
	if len(revoked) > 0 {
		if cs.cfg.AutoCommit {
			if err := cs.commit(ctx, revoked); err != nil {
				return err
			}
		}
		cs.mu.Lock()
		for _, tp := range revoked {
			delete(cs.owned, tp)
		}
		cs.mu.Unlock()
	}
	if len(added) > 0 {
		starts, err := cs.startOffsets(ctx, added)
		if err != nil {
			return err
		}
		cs.mu.Lock()
		for _, tp := range added {
			if cs.target[tp] {
				cs.gen++
				cs.owned[tp] = &partState{pos: starts[tp], gen: cs.gen}
			}
		}
		cs.mu.Unlock()
	}
	return nil
}

func (cs *Consumer) startOffsets(ctx context.Context, tps []TopicPartition) (map[TopicPartition]int64, error) {
	cs.mu.Lock()
	coord := cs.coord
	cs.mu.Unlock()
	if coord == nil {
		return nil, protocol.ErrNotCoordinator
	}
	req := &protocol.OffsetFetchRequest{GroupID: cs.cfg.Group}
	for _, tp := range tps {
		req.Partitions = append(req.Partitions, protocol.TopicPartitions{Topic: tp.Topic, Partitions: []int32{tp.Partition}})
	}
	var resp protocol.OffsetFetchResponse
	if err := coord.Do(ctx, protocol.APIOffsetFetch, req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != protocol.ErrNone {
		return nil, resp.Error
	}
	out := map[TopicPartition]int64{}
	for _, o := range resp.Offsets {
		tp := TopicPartition{o.Topic, o.Partition}
		if o.Offset >= 0 {
			out[tp] = o.Offset
			continue
		}
		off, err := cs.reset(ctx, tp)
		if err != nil {
			return nil, err
		}
		out[tp] = off
	}
	return out, nil
}

func (cs *Consumer) reset(ctx context.Context, tp TopicPartition) (int64, error) {
	which := protocol.LatestOffset
	if cs.cfg.FromEarliest {
		which = protocol.EarliestOffset
	}
	return cs.c.ListOffsets(ctx, tp.Topic, tp.Partition, which)
}

// Poll returns the next records from the assigned partitions, waiting up to MaxWait for data. It returns
// as soon as any partition has records; slower partitions' fetches keep running for the next Poll.
func (cs *Consumer) Poll(ctx context.Context) ([]Record, error) {
	if err := cs.applyAssignment(ctx); err != nil {
		return nil, err
	}
	if cs.cfg.AutoCommit {
		if err := cs.Commit(ctx); err != nil {
			return nil, err
		}
	}
	cs.mu.Lock()
	for tp, st := range cs.owned {
		if !st.inflight {
			st.inflight = true
			go cs.fetch(tp, st.gen, st.pos)
		}
	}
	cs.mu.Unlock()

	var results []fetchResult
	timer := time.NewTimer(cs.cfg.MaxWait + 2*time.Second)
	defer timer.Stop()
	select {
	case r := <-cs.results:
		results = append(results, r)
	case <-timer.C:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
drain:
	for {
		select {
		case r := <-cs.results:
			results = append(results, r)
		default:
			break drain
		}
	}

	var out []Record
	for _, r := range results {
		cs.mu.Lock()
		st, ok := cs.owned[r.tp]
		if ok && st.gen == r.gen {
			st.inflight = false
		}
		current := ok && st.gen == r.gen && st.pos == r.pos
		cs.mu.Unlock()
		if !current {
			continue
		}
		if r.err != nil {
			cs.c.invalidate(r.tp.Topic)
			continue
		}
		switch r.resp.Error {
		case protocol.ErrNone:
		case protocol.ErrOffsetOutOfRange:
			if off, err := cs.reset(ctx, r.tp); err == nil {
				cs.setPos(r.tp, r.gen, off)
			}
			continue
		default:
			cs.c.invalidate(r.tp.Topic)
			continue
		}
		var next int64
		var err error
		out, next, err = decodeRecords(out, r.tp.Topic, r.tp.Partition, r.resp.Records, r.pos, nil)
		if err != nil {
			return out, err
		}
		cs.setPos(r.tp, r.gen, max(next, r.pos))
	}
	return out, nil
}

func (cs *Consumer) setPos(tp TopicPartition, gen int, pos int64) {
	cs.mu.Lock()
	if st, ok := cs.owned[tp]; ok && st.gen == gen && pos != st.pos {
		st.pos = pos
		cs.dirty = true
	}
	cs.mu.Unlock()
}

func (cs *Consumer) fetch(tp TopicPartition, gen int, pos int64) {
	ctx, cancel := context.WithTimeout(context.Background(), cs.cfg.MaxWait+10*time.Second)
	defer cancel()
	resp, err := cs.c.Fetch(ctx, tp.Topic, tp.Partition, pos, cs.cfg.MaxBytes, cs.cfg.MaxWait)
	if err != nil {
		time.Sleep(50 * time.Millisecond) // don't spin on a dead broker
	}
	select {
	case cs.results <- fetchResult{tp: tp, gen: gen, pos: pos, resp: resp, err: err}:
	case <-cs.stop:
	}
}

// Commit commits the current position of every assigned partition.
func (cs *Consumer) Commit(ctx context.Context) error {
	cs.mu.Lock()
	if !cs.dirty {
		cs.mu.Unlock()
		return nil
	}
	tps := sortedTPs(cs.owned)
	cs.mu.Unlock()
	return cs.commit(ctx, tps)
}

func (cs *Consumer) commit(ctx context.Context, tps []TopicPartition) error {
	cs.mu.Lock()
	req := &protocol.OffsetCommitRequest{GroupID: cs.cfg.Group, MemberID: cs.memberID, MemberEpoch: cs.epoch}
	for _, tp := range tps {
		if st, ok := cs.owned[tp]; ok {
			req.Offsets = append(req.Offsets, protocol.PartitionOffset{Topic: tp.Topic, Partition: tp.Partition, Offset: st.pos})
		}
	}
	coord := cs.coord
	cs.dirty = false
	cs.mu.Unlock()
	if len(req.Offsets) == 0 || coord == nil {
		return nil
	}
	var resp protocol.OffsetCommitResponse
	if err := coord.Do(ctx, protocol.APIOffsetCommit, req, &resp); err != nil {
		return err
	}
	if resp.Error != protocol.ErrNone {
		return resp.Error
	}
	return nil
}

// Close commits (with AutoCommit), leaves the group and stops heartbeating.
func (cs *Consumer) Close(ctx context.Context) error {
	var err error
	if cs.cfg.AutoCommit {
		cs.mu.Lock()
		cs.dirty = true
		cs.mu.Unlock()
		err = cs.Commit(ctx)
	}
	close(cs.stop)
	cs.wg.Wait()
	cs.mu.Lock()
	coord, req := cs.coord, &protocol.GroupHeartbeatRequest{GroupID: cs.cfg.Group, MemberID: cs.memberID, MemberEpoch: -1}
	cs.mu.Unlock()
	if coord != nil {
		var resp protocol.GroupHeartbeatResponse
		_ = coord.Do(ctx, protocol.APIGroupHeartbeat, req, &resp)
	}
	return err
}

func sortedTPs[V any](m map[TopicPartition]V) []TopicPartition {
	out := make([]TopicPartition, 0, len(m))
	for tp := range m {
		out = append(out, tp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}
