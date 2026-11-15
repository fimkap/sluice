package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
)

// ShareConsumerConfig configures a ShareConsumer.
type ShareConsumerConfig struct {
	Group string
	Topic string
	// MaxRecords per partition per fetch (default 100).
	MaxRecords int
	// MaxWait is how long a fetch waits for records (default 500 ms).
	MaxWait time.Duration
	// FromEarliest starts a new share group at the log start (default: the end).
	FromEarliest bool
}

// ShareConsumer reads a topic with queue semantics (KIP-932): records are acquired with a time-limited
// lock, acknowledged one by one, and any member can take any record. Ordering is not guaranteed.
// Poll and Ack are not safe for concurrent use with each other; Ack may be called from any goroutine
// between Polls only if the caller serializes them.
type ShareConsumer struct {
	c        *Client
	cfg      ShareConsumerConfig
	memberID string

	mu      sync.Mutex
	parts   []*sharePart
	results chan shareResult
	stop    chan struct{}
}

type sharePart struct {
	id       int32
	inflight bool
	acks     []protocol.ShareAck
}

type shareResult struct {
	part *sharePart
	acks []protocol.ShareAck // what this fetch carried, re-queued if it failed in transit
	resp *protocol.ShareFetchResponse
	err  error
}

// NewShareConsumer creates a member of a share group. Members need no join step.
func (c *Client) NewShareConsumer(ctx context.Context, cfg ShareConsumerConfig) (*ShareConsumer, error) {
	if cfg.Group == "" || cfg.Topic == "" {
		return nil, errors.New("sluice: share consumer needs a group and a topic")
	}
	if cfg.MaxRecords <= 0 {
		cfg.MaxRecords = 100
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 500 * time.Millisecond
	}
	n, err := c.Partitions(ctx, cfg.Topic)
	if err != nil {
		return nil, err
	}
	var id [8]byte
	rand.Read(id[:])
	s := &ShareConsumer{c: c, cfg: cfg, memberID: hex.EncodeToString(id[:]), results: make(chan shareResult, 2*n+1), stop: make(chan struct{})}
	for p := 0; p < n; p++ {
		s.parts = append(s.parts, &sharePart{id: int32(p)})
	}
	return s, nil
}

// MemberID identifies this member to the brokers.
func (s *ShareConsumer) MemberID() string { return s.memberID }

// Poll returns acquired records, waiting up to MaxWait. Acknowledgements queued with Ack ride along
// with the next fetch of their partition.
func (s *ShareConsumer) Poll(ctx context.Context) ([]Record, error) {
	s.mu.Lock()
	for _, p := range s.parts {
		if !p.inflight {
			p.inflight = true
			acks := p.acks
			p.acks = nil
			go s.fetch(p, acks)
		}
	}
	s.mu.Unlock()

	var results []shareResult
	timer := time.NewTimer(s.cfg.MaxWait + 2*time.Second)
	defer timer.Stop()
	select {
	case r := <-s.results:
		results = append(results, r)
	case <-timer.C:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
drain:
	for {
		select {
		case r := <-s.results:
			results = append(results, r)
		default:
			break drain
		}
	}

	var out []Record
	for _, r := range results {
		s.mu.Lock()
		r.part.inflight = false
		if r.err != nil {
			r.part.acks = append(r.acks, r.part.acks...)
		}
		s.mu.Unlock()
		if r.err != nil {
			s.c.invalidate(s.cfg.Topic)
			continue
		}
		if r.resp.Error != protocol.ErrNone {
			if r.resp.Error == protocol.ErrNotLeader {
				s.c.invalidate(s.cfg.Topic)
			}
			continue
		}
		if len(r.resp.Acquired) == 0 {
			continue
		}
		ranges := r.resp.Acquired
		keep := func(off int64) (int16, bool) {
			for _, a := range ranges {
				if off >= a.First && off <= a.Last {
					return a.DeliveryCount, true
				}
			}
			return 0, false
		}
		var err error
		if out, _, err = decodeRecords(out, s.cfg.Topic, r.part.id, r.resp.Records, ranges[0].First, keep); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (s *ShareConsumer) fetch(p *sharePart, acks []protocol.ShareAck) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.MaxWait+10*time.Second)
	defer cancel()
	var resp protocol.ShareFetchResponse
	req := &protocol.ShareFetchRequest{GroupID: s.cfg.Group, MemberID: s.memberID, Topic: s.cfg.Topic, Partition: p.id,
		MaxRecords: int32(s.cfg.MaxRecords), MaxWaitMs: int32(s.cfg.MaxWait / time.Millisecond),
		ResetToEarliest: s.cfg.FromEarliest, Acks: acks}
	_, cn, err := s.c.leaderConn(ctx, s.cfg.Topic, p.id)
	if err == nil {
		err = cn.Do(ctx, protocol.APIShareFetch, req, &resp)
	}
	if err != nil {
		time.Sleep(50 * time.Millisecond)
	}
	select {
	case s.results <- shareResult{part: p, acks: acks, resp: &resp, err: err}:
	case <-s.stop:
	}
}

// Ack queues an acknowledgement for a record returned by Poll.
func (s *ShareConsumer) Ack(r Record, t protocol.AckType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if int(r.Partition) >= len(s.parts) {
		return
	}
	p := s.parts[r.Partition]
	if n := len(p.acks); n > 0 && p.acks[n-1].Type == t && p.acks[n-1].Last == r.Offset-1 {
		p.acks[n-1].Last = r.Offset
		return
	}
	p.acks = append(p.acks, protocol.ShareAck{First: r.Offset, Last: r.Offset, Type: t})
}

// Commit sends queued acknowledgements now instead of with the next fetch.
func (s *ShareConsumer) Commit(ctx context.Context) error {
	s.mu.Lock()
	type pending struct {
		id   int32
		acks []protocol.ShareAck
	}
	var todo []pending
	for _, p := range s.parts {
		if len(p.acks) > 0 {
			todo = append(todo, pending{p.id, p.acks})
			p.acks = nil
		}
	}
	s.mu.Unlock()
	var first error
	for _, t := range todo {
		_, cn, err := s.c.leaderConn(ctx, s.cfg.Topic, t.id)
		if err == nil {
			var resp protocol.ShareAcknowledgeResponse
			err = cn.Do(ctx, protocol.APIShareAcknowledge, &protocol.ShareAcknowledgeRequest{
				GroupID: s.cfg.Group, MemberID: s.memberID, Topic: s.cfg.Topic, Partition: t.id, Acks: t.acks}, &resp)
			if err == nil && resp.Error != protocol.ErrNone {
				err = resp.Error
			}
		}
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Close sends queued acknowledgements and stops. Records acquired but not acknowledged become
// available again when their locks expire.
func (s *ShareConsumer) Close(ctx context.Context) error {
	err := s.Commit(ctx)
	close(s.stop)
	return err
}
