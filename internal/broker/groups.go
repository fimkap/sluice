package broker

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
)

type topicPartition struct {
	topic     string
	partition int32
}

// groupCoordinator runs consumer groups the KIP-848 way: members heartbeat, the coordinator computes the
// assignment, and a partition moves to a new owner only after its old owner reports it released.
type groupCoordinator struct {
	b      *Broker
	mu     sync.Mutex
	groups map[string]*group
	dir    string
}

type group struct {
	id          string
	epoch       int32 // bumps when membership or subscriptions change
	members     map[string]*member
	target      map[string][]topicPartition // computed for targetEpoch
	targetEpoch int32
	offsets     map[topicPartition]int64
	loaded      bool
}

type member struct {
	id       string
	epoch    int32 // the group epoch of the assignment it was last given
	topics   []string
	owned    map[topicPartition]bool // as reported in its last heartbeat
	assigned []topicPartition
	lastSeen time.Time
}

func newGroupCoordinator(b *Broker) *groupCoordinator {
	return &groupCoordinator{b: b, groups: map[string]*group{}, dir: filepath.Join(b.cfg.DataDir, "meta", "groups")}
}

func (gc *groupCoordinator) getLocked(id string) *group {
	g, ok := gc.groups[id]
	if !ok {
		g = &group{id: id, members: map[string]*member{}, offsets: map[topicPartition]int64{}, targetEpoch: -1}
		gc.groups[id] = g
	}
	if !g.loaded {
		gc.loadOffsets(g)
		g.loaded = true
	}
	return g
}

func (gc *groupCoordinator) isCoordinator(group string) bool {
	return gc.b.coordinatorFor(group) == gc.b.cfg.NodeID
}

func (gc *groupCoordinator) heartbeat(r *protocol.GroupHeartbeatRequest) *protocol.GroupHeartbeatResponse {
	if !gc.isCoordinator(r.GroupID) {
		return &protocol.GroupHeartbeatResponse{Error: protocol.ErrNotCoordinator}
	}
	now := time.Now()
	gc.mu.Lock()
	defer gc.mu.Unlock()
	g := gc.getLocked(r.GroupID)
	gc.expireLocked(g, now)

	interval := int32(gc.b.cfg.HeartbeatInterval / time.Millisecond)
	if r.MemberEpoch == -1 { // leave
		if _, ok := g.members[r.MemberID]; ok {
			delete(g.members, r.MemberID)
			g.epoch++
		}
		return &protocol.GroupHeartbeatResponse{MemberID: r.MemberID, MemberEpoch: -1, HeartbeatIntervalMs: interval}
	}

	var m *member
	if r.MemberID == "" {
		m = &member{id: newMemberID(), owned: map[topicPartition]bool{}}
		g.members[m.id] = m
		g.epoch++
	} else if m = g.members[r.MemberID]; m == nil {
		return &protocol.GroupHeartbeatResponse{Error: protocol.ErrUnknownMember}
	}
	m.lastSeen = now
	if !equalStrings(m.topics, r.Topics) {
		m.topics = append([]string(nil), r.Topics...)
		sort.Strings(m.topics)
		g.epoch++
	}
	m.owned = map[topicPartition]bool{}
	for _, tp := range r.Owned {
		for _, p := range tp.Partitions {
			m.owned[topicPartition{tp.Topic, p}] = true
		}
	}

	if g.targetEpoch != g.epoch {
		g.target = gc.computeTarget(g)
		g.targetEpoch = g.epoch
	}
	// Hand out the target minus partitions another live member may still hold: ones it reported owning,
	// and ones we assigned to it that it hasn't reported yet (it may have started consuming them).
	var assignment []topicPartition
	for _, tp := range g.target[m.id] {
		ownedElsewhere := false
		for id, other := range g.members {
			if id != m.id && (other.owned[tp] || containsTP(other.assigned, tp)) {
				ownedElsewhere = true
				break
			}
		}
		if !ownedElsewhere {
			assignment = append(assignment, tp)
		}
	}
	if !equalTPs(assignment, m.assigned) || m.epoch == 0 {
		m.assigned = assignment
		m.epoch = g.epoch
	}
	return &protocol.GroupHeartbeatResponse{MemberID: m.id, MemberEpoch: m.epoch, HeartbeatIntervalMs: interval,
		Assignment: groupByTopic(m.assigned)}
}

// computeTarget assigns every partition of every subscribed topic round-robin over the members that
// subscribe to its topic, in a stable order.
func (gc *groupCoordinator) computeTarget(g *group) map[string][]topicPartition {
	ids := make([]string, 0, len(g.members))
	topics := map[string]bool{}
	for id, m := range g.members {
		ids = append(ids, id)
		for _, t := range m.topics {
			topics[t] = true
		}
	}
	sort.Strings(ids)
	var all []topicPartition
	for t := range topics {
		if tp := gc.b.topics.get(t); tp != nil {
			for p := range tp.assignment {
				all = append(all, topicPartition{t, int32(p)})
			}
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].topic != all[j].topic {
			return all[i].topic < all[j].topic
		}
		return all[i].partition < all[j].partition
	})
	target := map[string][]topicPartition{}
	next := 0
	for _, tp := range all {
		for tries := 0; tries < len(ids); tries++ {
			id := ids[next%len(ids)]
			next++
			if containsString(g.members[id].topics, tp.topic) {
				target[id] = append(target[id], tp)
				break
			}
		}
	}
	return target
}

func (gc *groupCoordinator) expireLocked(g *group, now time.Time) {
	for id, m := range g.members {
		if now.Sub(m.lastSeen) > gc.b.cfg.SessionTimeout {
			delete(g.members, id)
			g.epoch++
			gc.b.cfg.Logger.Info("group member timed out", "group", g.id, "member", id)
		}
	}
}

func (gc *groupCoordinator) commit(r *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	if !gc.isCoordinator(r.GroupID) {
		return &protocol.OffsetCommitResponse{Error: protocol.ErrNotCoordinator}
	}
	gc.mu.Lock()
	defer gc.mu.Unlock()
	g := gc.getLocked(r.GroupID)
	if r.MemberID != "" { // "" commits for a standalone consumer that manages its own partitions
		if _, ok := g.members[r.MemberID]; !ok {
			return &protocol.OffsetCommitResponse{Error: protocol.ErrUnknownMember}
		}
	}
	for _, o := range r.Offsets {
		g.offsets[topicPartition{o.Topic, o.Partition}] = o.Offset
	}
	if err := gc.saveOffsets(g); err != nil {
		gc.b.cfg.Logger.Error("save offsets", "group", g.id, "err", err)
		return &protocol.OffsetCommitResponse{Error: protocol.ErrInternal}
	}
	return &protocol.OffsetCommitResponse{}
}

func (gc *groupCoordinator) fetchOffsets(r *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	if !gc.isCoordinator(r.GroupID) {
		return &protocol.OffsetFetchResponse{Error: protocol.ErrNotCoordinator}
	}
	gc.mu.Lock()
	defer gc.mu.Unlock()
	g := gc.getLocked(r.GroupID)
	resp := &protocol.OffsetFetchResponse{}
	for _, tp := range r.Partitions {
		for _, p := range tp.Partitions {
			off, ok := g.offsets[topicPartition{tp.Topic, p}]
			if !ok {
				off = -1
			}
			resp.Offsets = append(resp.Offsets, protocol.PartitionOffset{Topic: tp.Topic, Partition: p, Offset: off})
		}
	}
	return resp
}

type offsetRecord struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func (gc *groupCoordinator) path(g *group) string {
	return filepath.Join(gc.dir, url.PathEscape(g.id)+".json")
}

func (gc *groupCoordinator) saveOffsets(g *group) error {
	recs := make([]offsetRecord, 0, len(g.offsets))
	for tp, off := range g.offsets {
		recs = append(recs, offsetRecord{tp.topic, tp.partition, off})
	}
	data, err := json.Marshal(recs)
	if err != nil {
		return err
	}
	return writeFileAtomic(gc.path(g), data, false)
}

func (gc *groupCoordinator) loadOffsets(g *group) {
	data, err := os.ReadFile(gc.path(g))
	if err != nil {
		return
	}
	var recs []offsetRecord
	if json.Unmarshal(data, &recs) != nil {
		return
	}
	for _, r := range recs {
		g.offsets[topicPartition{r.Topic, r.Partition}] = r.Offset
	}
}

func newMemberID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func groupByTopic(tps []topicPartition) []protocol.TopicPartitions {
	var out []protocol.TopicPartitions
	for _, tp := range tps {
		if n := len(out); n > 0 && out[n-1].Topic == tp.topic {
			out[n-1].Partitions = append(out[n-1].Partitions, tp.partition)
			continue
		}
		out = append(out, protocol.TopicPartitions{Topic: tp.topic, Partitions: []int32{tp.partition}})
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as, bs := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

func equalTPs(a, b []topicPartition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsTP(v []topicPartition, tp topicPartition) bool {
	for _, x := range v {
		if x == tp {
			return true
		}
	}
	return false
}

func containsString(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}
