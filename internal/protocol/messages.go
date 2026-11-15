package protocol

// Produce appends one batch to one partition.
type ProduceRequest struct {
	Topic     string
	Partition int32
	Acks      int16
	TimeoutMs int32
	Batch     []byte
}

func (m *ProduceRequest) Encode(w *Writer) {
	w.String(m.Topic)
	w.Int32(m.Partition)
	w.Int16(m.Acks)
	w.Int32(m.TimeoutMs)
	w.Bytes(m.Batch)
}

func (m *ProduceRequest) Decode(r *Reader) error {
	m.Topic, m.Partition, m.Acks, m.TimeoutMs, m.Batch = r.String(), r.Int32(), r.Int16(), r.Int32(), r.Bytes()
	return r.done()
}

type ProduceResponse struct {
	Error      ErrorCode
	BaseOffset int64
	LogStart   int64
}

func (m *ProduceResponse) Encode(w *Writer) {
	w.Int16(int16(m.Error))
	w.Int64(m.BaseOffset)
	w.Int64(m.LogStart)
}

func (m *ProduceResponse) Decode(r *Reader) error {
	m.Error, m.BaseOffset, m.LogStart = ErrorCode(r.Int16()), r.Int64(), r.Int64()
	return r.done()
}

// Fetch reads batches. Consumers (ReplicaID -1) read up to the high watermark; followers read up to the
// log end offset, and their FetchOffset reports their acknowledged log end offset.
type FetchRequest struct {
	ReplicaID   int32
	MaxWaitMs   int32
	MinBytes    int32
	Topic       string
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

func (m *FetchRequest) Encode(w *Writer) {
	w.Int32(m.ReplicaID)
	w.Int32(m.MaxWaitMs)
	w.Int32(m.MinBytes)
	w.String(m.Topic)
	w.Int32(m.Partition)
	w.Int64(m.FetchOffset)
	w.Int32(m.MaxBytes)
}

func (m *FetchRequest) Decode(r *Reader) error {
	m.ReplicaID, m.MaxWaitMs, m.MinBytes = r.Int32(), r.Int32(), r.Int32()
	m.Topic, m.Partition, m.FetchOffset, m.MaxBytes = r.String(), r.Int32(), r.Int64(), r.Int32()
	return r.done()
}

// FetchResponse carries raw batches; the last one may be truncated. The broker may write Records
// straight from the segment file after EncodeHeader.
type FetchResponse struct {
	Error         ErrorCode
	HighWatermark int64
	LogStart      int64
	LogEnd        int64
	Records       []byte
}

// FetchResponseHeaderSize is the encoded size of a FetchResponse without its records.
const FetchResponseHeaderSize = 2 + 8 + 8 + 8 + 4

// EncodeHeader writes everything but the record bytes, announcing recordsLen of them.
func (m *FetchResponse) EncodeHeader(w *Writer, recordsLen int) {
	w.Int16(int16(m.Error))
	w.Int64(m.HighWatermark)
	w.Int64(m.LogStart)
	w.Int64(m.LogEnd)
	w.Int32(int32(recordsLen))
}

func (m *FetchResponse) Encode(w *Writer) {
	m.EncodeHeader(w, len(m.Records))
	w.B = append(w.B, m.Records...)
}

func (m *FetchResponse) Decode(r *Reader) error {
	m.Error, m.HighWatermark, m.LogStart, m.LogEnd = ErrorCode(r.Int16()), r.Int64(), r.Int64(), r.Int64()
	m.Records = r.Bytes()
	return r.done()
}

type ListOffsetsRequest struct {
	Topic     string
	Partition int32
	Timestamp int64 // LatestOffset or EarliestOffset
}

func (m *ListOffsetsRequest) Encode(w *Writer) {
	w.String(m.Topic)
	w.Int32(m.Partition)
	w.Int64(m.Timestamp)
}

func (m *ListOffsetsRequest) Decode(r *Reader) error {
	m.Topic, m.Partition, m.Timestamp = r.String(), r.Int32(), r.Int64()
	return r.done()
}

type ListOffsetsResponse struct {
	Error  ErrorCode
	Offset int64
}

func (m *ListOffsetsResponse) Encode(w *Writer) { w.Int16(int16(m.Error)); w.Int64(m.Offset) }

func (m *ListOffsetsResponse) Decode(r *Reader) error {
	m.Error, m.Offset = ErrorCode(r.Int16()), r.Int64()
	return r.done()
}

// MetadataRequest asks for the cluster and the given topics (all topics if empty).
type MetadataRequest struct{ Topics []string }

func (m *MetadataRequest) Encode(w *Writer) { w.Strings(m.Topics) }

func (m *MetadataRequest) Decode(r *Reader) error {
	m.Topics = r.Strings()
	return r.done()
}

type Broker struct {
	NodeID int32
	Host   string
	Port   int32
}

type PartitionMetadata struct {
	ID       int32
	Error    ErrorCode
	Leader   int32
	Replicas []int32
	ISR      []int32
}

type TopicMetadata struct {
	Name       string
	Error      ErrorCode
	Partitions []PartitionMetadata
}

type MetadataResponse struct {
	Brokers []Broker
	Topics  []TopicMetadata
}

func (m *MetadataResponse) Encode(w *Writer) {
	w.ArrayLen(len(m.Brokers))
	for _, b := range m.Brokers {
		w.Int32(b.NodeID)
		w.String(b.Host)
		w.Int32(b.Port)
	}
	w.ArrayLen(len(m.Topics))
	for _, t := range m.Topics {
		w.String(t.Name)
		w.Int16(int16(t.Error))
		w.ArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.Int32(p.ID)
			w.Int16(int16(p.Error))
			w.Int32(p.Leader)
			w.Int32s(p.Replicas)
			w.Int32s(p.ISR)
		}
	}
}

func (m *MetadataResponse) Decode(r *Reader) error {
	m.Brokers = make([]Broker, r.ArrayLen(10))
	for i := range m.Brokers {
		m.Brokers[i] = Broker{NodeID: r.Int32(), Host: r.String(), Port: r.Int32()}
	}
	m.Topics = make([]TopicMetadata, r.ArrayLen(8))
	for i := range m.Topics {
		t := &m.Topics[i]
		t.Name, t.Error = r.String(), ErrorCode(r.Int16())
		t.Partitions = make([]PartitionMetadata, r.ArrayLen(18))
		for j := range t.Partitions {
			p := &t.Partitions[j]
			p.ID, p.Error, p.Leader = r.Int32(), ErrorCode(r.Int16()), r.Int32()
			p.Replicas, p.ISR = r.Int32s(), r.Int32s()
		}
	}
	return r.done()
}

// CreateTopicRequest creates a topic on every broker. A client sends it with Forwarded false; the broker
// that receives it computes the assignment and forwards it to its peers with Forwarded true.
type CreateTopicRequest struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Forwarded         bool
	Assignment        [][]int32 // replicas per partition, set when Forwarded
}

func (m *CreateTopicRequest) Encode(w *Writer) {
	w.String(m.Name)
	w.Int32(m.Partitions)
	w.Int16(m.ReplicationFactor)
	w.Bool(m.Forwarded)
	w.ArrayLen(len(m.Assignment))
	for _, a := range m.Assignment {
		w.Int32s(a)
	}
}

func (m *CreateTopicRequest) Decode(r *Reader) error {
	m.Name, m.Partitions, m.ReplicationFactor, m.Forwarded = r.String(), r.Int32(), r.Int16(), r.Bool()
	if n := r.ArrayLen(4); n > 0 {
		m.Assignment = make([][]int32, n)
		for i := range m.Assignment {
			m.Assignment[i] = r.Int32s()
		}
	}
	return r.done()
}

type CreateTopicResponse struct {
	Error   ErrorCode
	Message string
}

func (m *CreateTopicResponse) Encode(w *Writer) { w.Int16(int16(m.Error)); w.String(m.Message) }

func (m *CreateTopicResponse) Decode(r *Reader) error {
	m.Error, m.Message = ErrorCode(r.Int16()), r.String()
	return r.done()
}

type InitProducerIDRequest struct{}

func (m *InitProducerIDRequest) Encode(*Writer)         {}
func (m *InitProducerIDRequest) Decode(r *Reader) error { return r.done() }

type InitProducerIDResponse struct {
	Error      ErrorCode
	ProducerID int64
	Epoch      int16
}

func (m *InitProducerIDResponse) Encode(w *Writer) {
	w.Int16(int16(m.Error))
	w.Int64(m.ProducerID)
	w.Int16(m.Epoch)
}

func (m *InitProducerIDResponse) Decode(r *Reader) error {
	m.Error, m.ProducerID, m.Epoch = ErrorCode(r.Int16()), r.Int64(), r.Int16()
	return r.done()
}

type FindCoordinatorRequest struct{ Key string }

func (m *FindCoordinatorRequest) Encode(w *Writer) { w.String(m.Key) }

func (m *FindCoordinatorRequest) Decode(r *Reader) error {
	m.Key = r.String()
	return r.done()
}

type FindCoordinatorResponse struct {
	Error  ErrorCode
	NodeID int32
	Host   string
	Port   int32
}

func (m *FindCoordinatorResponse) Encode(w *Writer) {
	w.Int16(int16(m.Error))
	w.Int32(m.NodeID)
	w.String(m.Host)
	w.Int32(m.Port)
}

func (m *FindCoordinatorResponse) Decode(r *Reader) error {
	m.Error, m.NodeID, m.Host, m.Port = ErrorCode(r.Int16()), r.Int32(), r.String(), r.Int32()
	return r.done()
}

// TopicPartitions lists partitions of one topic.
type TopicPartitions struct {
	Topic      string
	Partitions []int32
}

func writeTopicPartitions(w *Writer, v []TopicPartitions) {
	w.ArrayLen(len(v))
	for _, tp := range v {
		w.String(tp.Topic)
		w.Int32s(tp.Partitions)
	}
}

func readTopicPartitions(r *Reader) []TopicPartitions {
	n := r.ArrayLen(6)
	if n == 0 {
		return nil
	}
	v := make([]TopicPartitions, n)
	for i := range v {
		v[i] = TopicPartitions{Topic: r.String(), Partitions: r.Int32s()}
	}
	return v
}

// GroupHeartbeatRequest joins (MemberID "", MemberEpoch 0), stays in (current epoch) or leaves
// (MemberEpoch -1) a consumer group. Owned lists the partitions the member still holds.
type GroupHeartbeatRequest struct {
	GroupID     string
	MemberID    string
	MemberEpoch int32
	Topics      []string
	Owned       []TopicPartitions
}

func (m *GroupHeartbeatRequest) Encode(w *Writer) {
	w.String(m.GroupID)
	w.String(m.MemberID)
	w.Int32(m.MemberEpoch)
	w.Strings(m.Topics)
	writeTopicPartitions(w, m.Owned)
}

func (m *GroupHeartbeatRequest) Decode(r *Reader) error {
	m.GroupID, m.MemberID, m.MemberEpoch = r.String(), r.String(), r.Int32()
	m.Topics, m.Owned = r.Strings(), readTopicPartitions(r)
	return r.done()
}

type GroupHeartbeatResponse struct {
	Error               ErrorCode
	MemberID            string
	MemberEpoch         int32
	HeartbeatIntervalMs int32
	Assignment          []TopicPartitions
}

func (m *GroupHeartbeatResponse) Encode(w *Writer) {
	w.Int16(int16(m.Error))
	w.String(m.MemberID)
	w.Int32(m.MemberEpoch)
	w.Int32(m.HeartbeatIntervalMs)
	writeTopicPartitions(w, m.Assignment)
}

func (m *GroupHeartbeatResponse) Decode(r *Reader) error {
	m.Error, m.MemberID, m.MemberEpoch, m.HeartbeatIntervalMs = ErrorCode(r.Int16()), r.String(), r.Int32(), r.Int32()
	m.Assignment = readTopicPartitions(r)
	return r.done()
}

// PartitionOffset is a committed offset for one partition. Offset -1 means none.
type PartitionOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

func writeOffsets(w *Writer, v []PartitionOffset) {
	w.ArrayLen(len(v))
	for _, o := range v {
		w.String(o.Topic)
		w.Int32(o.Partition)
		w.Int64(o.Offset)
	}
}

func readOffsets(r *Reader) []PartitionOffset {
	n := r.ArrayLen(14)
	if n == 0 {
		return nil
	}
	v := make([]PartitionOffset, n)
	for i := range v {
		v[i] = PartitionOffset{Topic: r.String(), Partition: r.Int32(), Offset: r.Int64()}
	}
	return v
}

type OffsetCommitRequest struct {
	GroupID     string
	MemberID    string
	MemberEpoch int32
	Offsets     []PartitionOffset
}

func (m *OffsetCommitRequest) Encode(w *Writer) {
	w.String(m.GroupID)
	w.String(m.MemberID)
	w.Int32(m.MemberEpoch)
	writeOffsets(w, m.Offsets)
}

func (m *OffsetCommitRequest) Decode(r *Reader) error {
	m.GroupID, m.MemberID, m.MemberEpoch, m.Offsets = r.String(), r.String(), r.Int32(), readOffsets(r)
	return r.done()
}

type OffsetCommitResponse struct{ Error ErrorCode }

func (m *OffsetCommitResponse) Encode(w *Writer) { w.Int16(int16(m.Error)) }

func (m *OffsetCommitResponse) Decode(r *Reader) error {
	m.Error = ErrorCode(r.Int16())
	return r.done()
}

type OffsetFetchRequest struct {
	GroupID    string
	Partitions []TopicPartitions
}

func (m *OffsetFetchRequest) Encode(w *Writer) {
	w.String(m.GroupID)
	writeTopicPartitions(w, m.Partitions)
}

func (m *OffsetFetchRequest) Decode(r *Reader) error {
	m.GroupID, m.Partitions = r.String(), readTopicPartitions(r)
	return r.done()
}

type OffsetFetchResponse struct {
	Error   ErrorCode
	Offsets []PartitionOffset
}

func (m *OffsetFetchResponse) Encode(w *Writer) { w.Int16(int16(m.Error)); writeOffsets(w, m.Offsets) }

func (m *OffsetFetchResponse) Decode(r *Reader) error {
	m.Error, m.Offsets = ErrorCode(r.Int16()), readOffsets(r)
	return r.done()
}

// ShareAck acknowledges the offsets First..Last (inclusive) of a share partition.
type ShareAck struct {
	First, Last int64
	Type        AckType
}

func writeAcks(w *Writer, v []ShareAck) {
	w.ArrayLen(len(v))
	for _, a := range v {
		w.Int64(a.First)
		w.Int64(a.Last)
		w.Int8(int8(a.Type))
	}
}

func readAcks(r *Reader) []ShareAck {
	n := r.ArrayLen(17)
	if n == 0 {
		return nil
	}
	v := make([]ShareAck, n)
	for i := range v {
		v[i] = ShareAck{First: r.Int64(), Last: r.Int64(), Type: AckType(r.Int8())}
	}
	return v
}

// ShareFetchRequest acquires up to MaxRecords records of one partition for a share group, after applying
// Acks. ResetToEarliest decides where a brand-new share partition starts (default: the latest offset).
type ShareFetchRequest struct {
	GroupID         string
	MemberID        string
	Topic           string
	Partition       int32
	MaxRecords      int32
	MaxWaitMs       int32
	ResetToEarliest bool
	Acks            []ShareAck
}

func (m *ShareFetchRequest) Encode(w *Writer) {
	w.String(m.GroupID)
	w.String(m.MemberID)
	w.String(m.Topic)
	w.Int32(m.Partition)
	w.Int32(m.MaxRecords)
	w.Int32(m.MaxWaitMs)
	w.Bool(m.ResetToEarliest)
	writeAcks(w, m.Acks)
}

func (m *ShareFetchRequest) Decode(r *Reader) error {
	m.GroupID, m.MemberID, m.Topic, m.Partition = r.String(), r.String(), r.String(), r.Int32()
	m.MaxRecords, m.MaxWaitMs, m.ResetToEarliest, m.Acks = r.Int32(), r.Int32(), r.Bool(), readAcks(r)
	return r.done()
}

// AcquiredRange is a run of offsets acquired by a ShareFetch, with their delivery count.
type AcquiredRange struct {
	First, Last   int64
	DeliveryCount int16
}

// ShareFetchResponse returns the acquired ranges and the batches that contain them. Batches can hold
// records outside the ranges; the client must deliver only acquired offsets.
type ShareFetchResponse struct {
	Error    ErrorCode
	AckError ErrorCode
	Acquired []AcquiredRange
	Records  []byte
}

func (m *ShareFetchResponse) Encode(w *Writer) {
	w.Int16(int16(m.Error))
	w.Int16(int16(m.AckError))
	w.ArrayLen(len(m.Acquired))
	for _, a := range m.Acquired {
		w.Int64(a.First)
		w.Int64(a.Last)
		w.Int16(a.DeliveryCount)
	}
	w.Bytes(m.Records)
}

func (m *ShareFetchResponse) Decode(r *Reader) error {
	m.Error, m.AckError = ErrorCode(r.Int16()), ErrorCode(r.Int16())
	if n := r.ArrayLen(18); n > 0 {
		m.Acquired = make([]AcquiredRange, n)
		for i := range m.Acquired {
			m.Acquired[i] = AcquiredRange{First: r.Int64(), Last: r.Int64(), DeliveryCount: r.Int16()}
		}
	}
	m.Records = r.Bytes()
	return r.done()
}

type ShareAcknowledgeRequest struct {
	GroupID   string
	MemberID  string
	Topic     string
	Partition int32
	Acks      []ShareAck
}

func (m *ShareAcknowledgeRequest) Encode(w *Writer) {
	w.String(m.GroupID)
	w.String(m.MemberID)
	w.String(m.Topic)
	w.Int32(m.Partition)
	writeAcks(w, m.Acks)
}

func (m *ShareAcknowledgeRequest) Decode(r *Reader) error {
	m.GroupID, m.MemberID, m.Topic, m.Partition, m.Acks = r.String(), r.String(), r.String(), r.Int32(), readAcks(r)
	return r.done()
}

type ShareAcknowledgeResponse struct{ Error ErrorCode }

func (m *ShareAcknowledgeResponse) Encode(w *Writer) { w.Int16(int16(m.Error)) }

func (m *ShareAcknowledgeResponse) Decode(r *Reader) error {
	m.Error = ErrorCode(r.Int16())
	return r.done()
}

// NewRequest returns an empty request message for key, or nil for an unknown key.
func NewRequest(key APIKey) Message {
	switch key {
	case APIProduce:
		return &ProduceRequest{}
	case APIFetch:
		return &FetchRequest{}
	case APIListOffsets:
		return &ListOffsetsRequest{}
	case APIMetadata:
		return &MetadataRequest{}
	case APICreateTopic:
		return &CreateTopicRequest{}
	case APIInitProducerID:
		return &InitProducerIDRequest{}
	case APIFindCoordinator:
		return &FindCoordinatorRequest{}
	case APIGroupHeartbeat:
		return &GroupHeartbeatRequest{}
	case APIOffsetCommit:
		return &OffsetCommitRequest{}
	case APIOffsetFetch:
		return &OffsetFetchRequest{}
	case APIShareFetch:
		return &ShareFetchRequest{}
	case APIShareAcknowledge:
		return &ShareAcknowledgeRequest{}
	}
	return nil
}

// NewResponse returns an empty response message for key, or nil for an unknown key.
func NewResponse(key APIKey) Message {
	switch key {
	case APIProduce:
		return &ProduceResponse{}
	case APIFetch:
		return &FetchResponse{}
	case APIListOffsets:
		return &ListOffsetsResponse{}
	case APIMetadata:
		return &MetadataResponse{}
	case APICreateTopic:
		return &CreateTopicResponse{}
	case APIInitProducerID:
		return &InitProducerIDResponse{}
	case APIFindCoordinator:
		return &FindCoordinatorResponse{}
	case APIGroupHeartbeat:
		return &GroupHeartbeatResponse{}
	case APIOffsetCommit:
		return &OffsetCommitResponse{}
	case APIOffsetFetch:
		return &OffsetFetchResponse{}
	case APIShareFetch:
		return &ShareFetchResponse{}
	case APIShareAcknowledge:
		return &ShareAcknowledgeResponse{}
	}
	return nil
}
