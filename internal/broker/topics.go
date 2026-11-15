package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
)

var validTopic = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,200}$`)

type topic struct {
	name       string
	assignment [][]int32    // replicas per partition; the first one leads
	partitions []*partition // nil where this broker hosts no replica
}

// topicManager owns topic metadata (persisted in meta/topics.json) and the local partition replicas.
type topicManager struct {
	b      *Broker
	mu     sync.RWMutex
	topics map[string]*topic
	path   string
}

type topicRecord struct {
	Name       string    `json:"name"`
	Assignment [][]int32 `json:"assignment"`
}

func newTopicManager(b *Broker) *topicManager {
	return &topicManager{b: b, topics: map[string]*topic{}, path: filepath.Join(b.cfg.DataDir, "meta", "topics.json")}
}

func (tm *topicManager) load() error {
	data, err := os.ReadFile(tm.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var recs []topicRecord
	if err := json.Unmarshal(data, &recs); err != nil {
		return fmt.Errorf("parse %s: %w", tm.path, err)
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, r := range recs {
		if _, err := tm.openLocked(r.Name, r.Assignment); err != nil {
			return err
		}
	}
	return nil
}

// assign spreads partitions over the brokers: partition p's replicas are rf consecutive brokers starting
// at (fnv32(name) + p) mod n.
func (tm *topicManager) assign(name string, partitions int32, rf int16) [][]int32 {
	ids := tm.b.cfg.nodeIDs()
	start := int(fnv32(name) % uint32(len(ids)))
	out := make([][]int32, partitions)
	for p := range out {
		reps := make([]int32, rf)
		for i := range reps {
			reps[i] = ids[(start+p+i)%len(ids)]
		}
		out[p] = reps
	}
	return out
}

// create adds a topic locally. It is idempotent for an identical assignment.
func (tm *topicManager) create(name string, assignment [][]int32) (protocol.ErrorCode, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if t, ok := tm.topics[name]; ok {
		if equalAssignment(t.assignment, assignment) {
			return protocol.ErrNone, nil
		}
		return protocol.ErrTopicExists, nil
	}
	if _, err := tm.openLocked(name, assignment); err != nil {
		return protocol.ErrInternal, err
	}
	if err := tm.saveLocked(); err != nil {
		return protocol.ErrInternal, err
	}
	return protocol.ErrNone, nil
}

func (tm *topicManager) openLocked(name string, assignment [][]int32) (*topic, error) {
	t := &topic{name: name, assignment: assignment, partitions: make([]*partition, len(assignment))}
	for p, reps := range assignment {
		if !containsID(reps, tm.b.cfg.NodeID) {
			continue
		}
		part, err := openPartition(tm.b, name, int32(p), reps)
		if err != nil {
			for _, opened := range t.partitions {
				if opened != nil {
					opened.close()
				}
			}
			return nil, err
		}
		t.partitions[p] = part
	}
	tm.topics[name] = t
	return t, nil
}

func (tm *topicManager) saveLocked() error {
	recs := make([]topicRecord, 0, len(tm.topics))
	for _, t := range tm.topics {
		recs = append(recs, topicRecord{Name: t.name, Assignment: t.assignment})
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Name < recs[j].Name })
	data, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(tm.path, data, true)
}

func (tm *topicManager) get(name string) *topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topics[name]
}

// partition returns the local replica of topic/p, or an error code if the topic or partition is unknown
// or this broker doesn't host it.
func (tm *topicManager) partition(name string, p int32) (*partition, protocol.ErrorCode) {
	t := tm.get(name)
	if t == nil || p < 0 || int(p) >= len(t.partitions) {
		return nil, protocol.ErrUnknownTopicOrPartition
	}
	if t.partitions[p] == nil {
		return nil, protocol.ErrNotLeader
	}
	return t.partitions[p], protocol.ErrNone
}

// leaderPartition is like partition but requires this broker to lead it.
func (tm *topicManager) leaderPartition(name string, p int32) (*partition, protocol.ErrorCode) {
	part, code := tm.partition(name, p)
	if code != protocol.ErrNone {
		return nil, code
	}
	if !part.isLeader() {
		return nil, protocol.ErrNotLeader
	}
	return part, protocol.ErrNone
}

func (tm *topicManager) allPartitions() []*partition {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	var out []*partition
	for _, t := range tm.topics {
		for _, p := range t.partitions {
			if p != nil {
				out = append(out, p)
			}
		}
	}
	return out
}

func (tm *topicManager) close() error {
	var first error
	for _, p := range tm.allPartitions() {
		if err := p.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// handleCreateTopic validates, assigns and creates a topic, then forwards it to every peer.
func (b *Broker) handleCreateTopic(r *protocol.CreateTopicRequest) *protocol.CreateTopicResponse {
	if !validTopic.MatchString(r.Name) {
		return &protocol.CreateTopicResponse{Error: protocol.ErrInvalidTopic, Message: "names are 1-200 of [a-zA-Z0-9._-]"}
	}
	assignment := r.Assignment
	if !r.Forwarded {
		if r.Partitions < 1 || r.Partitions > 1024 {
			return &protocol.CreateTopicResponse{Error: protocol.ErrInvalidRequest, Message: "partitions must be 1..1024"}
		}
		if r.ReplicationFactor < 1 || int(r.ReplicationFactor) > len(b.cfg.Peers) {
			return &protocol.CreateTopicResponse{Error: protocol.ErrInvalidRequest,
				Message: fmt.Sprintf("replication factor must be 1..%d", len(b.cfg.Peers))}
		}
		if b.topics.get(r.Name) != nil {
			return &protocol.CreateTopicResponse{Error: protocol.ErrTopicExists}
		}
		assignment = b.topics.assign(r.Name, r.Partitions, r.ReplicationFactor)
	} else if len(assignment) == 0 {
		return &protocol.CreateTopicResponse{Error: protocol.ErrInvalidRequest, Message: "forwarded without assignment"}
	}

	code, err := b.topics.create(r.Name, assignment)
	if err != nil {
		b.cfg.Logger.Error("create topic", "topic", r.Name, "err", err)
		return &protocol.CreateTopicResponse{Error: code, Message: err.Error()}
	}
	if code != protocol.ErrNone || r.Forwarded {
		return &protocol.CreateTopicResponse{Error: code}
	}

	// Forward to every peer. Without a metadata quorum this is best effort: a peer that is down misses the
	// topic, and the error says so.
	ctx, cancel := context.WithTimeout(b.ctx, 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(b.cfg.Peers))
	for _, id := range b.cfg.nodeIDs() {
		if id == b.cfg.NodeID {
			continue
		}
		wg.Add(1)
		go func(id int32) {
			defer wg.Done()
			pc, err := b.peer(ctx, id)
			if err != nil {
				errs <- fmt.Errorf("broker %d: %w", id, err)
				return
			}
			var resp protocol.CreateTopicResponse
			fwd := &protocol.CreateTopicRequest{Name: r.Name, Partitions: r.Partitions, ReplicationFactor: r.ReplicationFactor,
				Forwarded: true, Assignment: assignment}
			if err := pc.Do(ctx, protocol.APICreateTopic, fwd, &resp); err != nil {
				errs <- fmt.Errorf("broker %d: %w", id, err)
				return
			}
			if resp.Error != protocol.ErrNone {
				errs <- fmt.Errorf("broker %d: %v %s", id, resp.Error, resp.Message)
			}
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return &protocol.CreateTopicResponse{Error: protocol.ErrBrokerNotAvailable, Message: err.Error()}
	}
	return &protocol.CreateTopicResponse{}
}

func (b *Broker) handleMetadata(r *protocol.MetadataRequest) *protocol.MetadataResponse {
	resp := &protocol.MetadataResponse{}
	for _, id := range b.cfg.nodeIDs() {
		host, port := splitHostPort(b.cfg.Peers[id])
		resp.Brokers = append(resp.Brokers, protocol.Broker{NodeID: id, Host: host, Port: port})
	}
	names := r.Topics
	if len(names) == 0 {
		b.topics.mu.RLock()
		for n := range b.topics.topics {
			names = append(names, n)
		}
		b.topics.mu.RUnlock()
		sort.Strings(names)
	}
	for _, n := range names {
		t := b.topics.get(n)
		if t == nil {
			resp.Topics = append(resp.Topics, protocol.TopicMetadata{Name: n, Error: protocol.ErrUnknownTopicOrPartition})
			continue
		}
		tmd := protocol.TopicMetadata{Name: n}
		for p, reps := range t.assignment {
			pm := protocol.PartitionMetadata{ID: int32(p), Leader: reps[0], Replicas: reps, ISR: reps}
			if part := t.partitions[p]; part != nil && part.isLeader() {
				pm.ISR = part.isrSnapshot()
			}
			tmd.Partitions = append(tmd.Partitions, pm)
		}
		resp.Topics = append(resp.Topics, tmd)
	}
	return resp
}

func (b *Broker) handleListOffsets(r *protocol.ListOffsetsRequest) *protocol.ListOffsetsResponse {
	p, code := b.topics.leaderPartition(r.Topic, r.Partition)
	if code != protocol.ErrNone {
		return &protocol.ListOffsetsResponse{Error: code}
	}
	switch r.Timestamp {
	case protocol.EarliestOffset:
		return &protocol.ListOffsetsResponse{Offset: p.log.LogStart()}
	case protocol.LatestOffset:
		return &protocol.ListOffsetsResponse{Offset: p.highWatermark().Offset}
	}
	return &protocol.ListOffsetsResponse{Error: protocol.ErrInvalidRequest}
}

func (b *Broker) handleFindCoordinator(r *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	id := b.coordinatorFor(r.Key)
	host, port := splitHostPort(b.cfg.Peers[id])
	return &protocol.FindCoordinatorResponse{NodeID: id, Host: host, Port: port}
}

func containsID(ids []int32, id int32) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

func equalAssignment(a, b [][]int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

// writeFileAtomic replaces path with data: write a temp file, optionally fsync it, rename it over path.
// Without sync the file survives a process crash but not necessarily a power loss.
func writeFileAtomic(path string, data []byte, sync bool) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if sync {
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
