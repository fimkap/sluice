// Package client is the Go client for sluice: an admin API, a batching idempotent Producer, a group
// Consumer (ordered, partition-bound) and a ShareConsumer (queue semantics with per-record acks).
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
	"github.com/fimkap/sluice/internal/record"
	"github.com/fimkap/sluice/internal/wire"
)

// Config configures a Client.
type Config struct {
	// Bootstrap lists broker addresses to discover the cluster from. One reachable broker is enough.
	Bootstrap []string
	// ClientID names the client in broker logs.
	ClientID string
}

// Client holds connections to the brokers and a cache of cluster metadata. It is safe for concurrent use.
type Client struct {
	cfg Config

	mu      sync.Mutex
	conns   map[int32]*wire.Conn
	brokers map[int32]string
	leaders map[string][]int32 // topic -> leader per partition
}

// Record is a consumed record.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []record.Header
	// DeliveryCount is how many times a share group delivered this record (1 on first delivery).
	// It is 0 for records from a group Consumer.
	DeliveryCount int16
}

// ErrUnknownTopic is returned when a topic does not exist.
var ErrUnknownTopic = errors.New("sluice: unknown topic")

// New connects to the cluster and loads its metadata.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Bootstrap) == 0 {
		return nil, errors.New("sluice: no bootstrap brokers")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "sluice-go"
	}
	c := &Client{cfg: cfg, conns: map[int32]*wire.Conn{}, brokers: map[int32]string{}, leaders: map[string][]int32{}}
	if err := c.RefreshMetadata(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Close closes every connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cn := range c.conns {
		cn.Close()
	}
	c.conns = map[int32]*wire.Conn{}
	return nil
}

// RefreshMetadata reloads the broker list and partition leaders.
func (c *Client) RefreshMetadata(ctx context.Context, topics ...string) error {
	md, err := c.Metadata(ctx, topics...)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range md.Brokers {
		c.brokers[b.NodeID] = net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))
	}
	for _, t := range md.Topics {
		if t.Error != protocol.ErrNone {
			delete(c.leaders, t.Name)
			continue
		}
		leaders := make([]int32, len(t.Partitions))
		for _, p := range t.Partitions {
			leaders[p.ID] = p.Leader
		}
		c.leaders[t.Name] = leaders
	}
	return nil
}

// Metadata asks any broker for cluster metadata.
func (c *Client) Metadata(ctx context.Context, topics ...string) (*protocol.MetadataResponse, error) {
	var resp protocol.MetadataResponse
	err := c.anyBroker(ctx, func(cn *wire.Conn) error {
		return cn.Do(ctx, protocol.APIMetadata, &protocol.MetadataRequest{Topics: topics}, &resp)
	})
	return &resp, err
}

// anyBroker runs fn against known brokers, then the bootstrap list, until one works.
func (c *Client) anyBroker(ctx context.Context, fn func(*wire.Conn) error) error {
	c.mu.Lock()
	ids := make([]int32, 0, len(c.brokers))
	for id := range c.brokers {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	var last error
	for _, id := range ids {
		cn, err := c.brokerConn(ctx, id)
		if err == nil {
			if err = fn(cn); err == nil {
				return nil
			}
			c.dropConn(id, cn)
		}
		last = err
	}
	for _, addr := range c.cfg.Bootstrap {
		cn, err := wire.Dial(ctx, addr, c.cfg.ClientID)
		if err != nil {
			last = err
			continue
		}
		err = fn(cn)
		cn.Close()
		if err == nil {
			return nil
		}
		last = err
	}
	return fmt.Errorf("sluice: no broker reachable: %w", last)
}

func (c *Client) brokerConn(ctx context.Context, id int32) (*wire.Conn, error) {
	c.mu.Lock()
	if cn, ok := c.conns[id]; ok && cn.Err() == nil {
		c.mu.Unlock()
		return cn, nil
	}
	addr, ok := c.brokers[id]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("sluice: unknown broker %d", id)
	}
	cn, err := wire.Dial(ctx, addr, c.cfg.ClientID)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.conns[id]; ok && old.Err() == nil { // lost a race with another dialer
		cn.Close()
		return old, nil
	}
	c.conns[id] = cn
	return cn, nil
}

func (c *Client) dropConn(id int32, cn *wire.Conn) {
	c.mu.Lock()
	if c.conns[id] == cn {
		delete(c.conns, id)
	}
	c.mu.Unlock()
	cn.Close()
}

// Partitions returns the number of partitions of topic.
func (c *Client) Partitions(ctx context.Context, topic string) (int, error) {
	leaders, err := c.leadersOf(ctx, topic)
	return len(leaders), err
}

func (c *Client) leadersOf(ctx context.Context, topic string) ([]int32, error) {
	c.mu.Lock()
	leaders, ok := c.leaders[topic]
	c.mu.Unlock()
	if ok {
		return leaders, nil
	}
	if err := c.RefreshMetadata(ctx, topic); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if leaders, ok = c.leaders[topic]; !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownTopic, topic)
	}
	return leaders, nil
}

// leaderConn returns a connection to the leader of topic/partition.
func (c *Client) leaderConn(ctx context.Context, topic string, partition int32) (int32, *wire.Conn, error) {
	leaders, err := c.leadersOf(ctx, topic)
	if err != nil {
		return 0, nil, err
	}
	if partition < 0 || int(partition) >= len(leaders) {
		return 0, nil, fmt.Errorf("sluice: %s has no partition %d", topic, partition)
	}
	id := leaders[partition]
	cn, err := c.brokerConn(ctx, id)
	return id, cn, err
}

// invalidate forgets a topic's leaders after a NOT_LEADER or connection error.
func (c *Client) invalidate(topic string) {
	c.mu.Lock()
	delete(c.leaders, topic)
	c.mu.Unlock()
}

// CreateTopic creates a topic with the given partition count and replication factor.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions, replicationFactor int) error {
	var resp protocol.CreateTopicResponse
	err := c.anyBroker(ctx, func(cn *wire.Conn) error {
		return cn.Do(ctx, protocol.APICreateTopic, &protocol.CreateTopicRequest{
			Name: name, Partitions: int32(partitions), ReplicationFactor: int16(replicationFactor)}, &resp)
	})
	if err != nil {
		return err
	}
	if resp.Error != protocol.ErrNone {
		if resp.Message != "" {
			return fmt.Errorf("%w: %s", resp.Error, resp.Message)
		}
		return resp.Error
	}
	c.invalidate(name)
	return nil
}

// ListOffsets returns the earliest (protocol.EarliestOffset) or latest committed (protocol.LatestOffset)
// offset of a partition.
func (c *Client) ListOffsets(ctx context.Context, topic string, partition int32, which int64) (int64, error) {
	_, cn, err := c.leaderConn(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	var resp protocol.ListOffsetsResponse
	if err := cn.Do(ctx, protocol.APIListOffsets, &protocol.ListOffsetsRequest{Topic: topic, Partition: partition, Timestamp: which}, &resp); err != nil {
		return 0, err
	}
	if resp.Error != protocol.ErrNone {
		return 0, resp.Error
	}
	return resp.Offset, nil
}

// Fetch is the raw fetch API: it returns the response with undecoded batches (the last one may be
// partial). Most programs should use a Consumer.
func (c *Client) Fetch(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32, maxWait time.Duration) (*protocol.FetchResponse, error) {
	_, cn, err := c.leaderConn(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	var resp protocol.FetchResponse
	req := &protocol.FetchRequest{ReplicaID: protocol.ConsumerReplicaID, MaxWaitMs: int32(maxWait / time.Millisecond),
		Topic: topic, Partition: partition, FetchOffset: offset, MaxBytes: maxBytes}
	if err := cn.Do(ctx, protocol.APIFetch, req, &resp); err != nil {
		return nil, err
	}
	if resp.Error == protocol.ErrNotLeader {
		c.invalidate(topic)
	}
	return &resp, nil
}

// coordinatorConn returns a connection to the coordinator of a consumer group.
func (c *Client) coordinatorConn(ctx context.Context, group string) (*wire.Conn, error) {
	var resp protocol.FindCoordinatorResponse
	err := c.anyBroker(ctx, func(cn *wire.Conn) error {
		return cn.Do(ctx, protocol.APIFindCoordinator, &protocol.FindCoordinatorRequest{Key: group}, &resp)
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != protocol.ErrNone {
		return nil, resp.Error
	}
	c.mu.Lock()
	c.brokers[resp.NodeID] = net.JoinHostPort(resp.Host, strconv.Itoa(int(resp.Port)))
	c.mu.Unlock()
	return c.brokerConn(ctx, resp.NodeID)
}

// decodeRecords appends the records of every complete batch in data with offset >= from, keeping only
// offsets accepted by keep (nil keeps all). It returns the offset after the last complete batch.
func decodeRecords(dst []Record, topic string, partition int32, data []byte, from int64, keep func(int64) (int16, bool)) ([]Record, int64, error) {
	next := from
	for {
		b, rest, ok := record.Next(data)
		if !ok {
			return dst, next, nil
		}
		data = rest
		if err := b.Validate(); err != nil {
			return dst, next, fmt.Errorf("sluice: corrupt batch at offset %d: %w", b.BaseOffset(), err)
		}
		err := b.Records(func(r record.Record) error {
			if r.Offset < from {
				return nil
			}
			var dc int16
			if keep != nil {
				var ok bool
				if dc, ok = keep(r.Offset); !ok {
					return nil
				}
			}
			dst = append(dst, Record{Topic: topic, Partition: partition, Offset: r.Offset,
				Timestamp: time.UnixMilli(r.Timestamp), Key: r.Key, Value: r.Value, Headers: r.Headers, DeliveryCount: dc})
			return nil
		})
		if err != nil {
			return dst, next, err
		}
		next = b.NextOffset()
	}
}
