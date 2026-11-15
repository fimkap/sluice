package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

// samples holds one populated request and response per API.
func samples() map[APIKey][2]Message {
	return map[APIKey][2]Message{
		APIProduce: {
			&ProduceRequest{Topic: "orders", Partition: 3, Acks: AcksAll, TimeoutMs: 5000, Batch: []byte{1, 2, 3}},
			&ProduceResponse{Error: ErrNone, BaseOffset: 1 << 40, LogStart: 7},
		},
		APIFetch: {
			&FetchRequest{ReplicaID: 2, MaxWaitMs: 500, MinBytes: 1, Topic: "t", Partition: 0, FetchOffset: 99, MaxBytes: 1 << 20},
			&FetchResponse{Error: ErrOffsetOutOfRange, HighWatermark: 10, LogStart: 1, LogEnd: 12, Records: []byte("raw")},
		},
		APIListOffsets: {
			&ListOffsetsRequest{Topic: "t", Partition: 1, Timestamp: EarliestOffset},
			&ListOffsetsResponse{Offset: 42},
		},
		APIMetadata: {
			&MetadataRequest{Topics: []string{"a", "b"}},
			&MetadataResponse{
				Brokers: []Broker{{NodeID: 1, Host: "b1", Port: 9092}, {NodeID: 2, Host: "b2", Port: 9093}},
				Topics: []TopicMetadata{{Name: "a", Partitions: []PartitionMetadata{
					{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1}},
					{ID: 1, Error: ErrNotLeader, Leader: 2, Replicas: []int32{2, 1}, ISR: []int32{2, 1}},
				}}},
			},
		},
		APICreateTopic: {
			&CreateTopicRequest{Name: "n", Partitions: 2, ReplicationFactor: 3, Forwarded: true, Assignment: [][]int32{{1, 2, 3}, {2, 3, 1}}},
			&CreateTopicResponse{Error: ErrTopicExists, Message: "exists"},
		},
		APIInitProducerID: {
			&InitProducerIDRequest{},
			&InitProducerIDResponse{ProducerID: 77, Epoch: 1},
		},
		APIFindCoordinator: {
			&FindCoordinatorRequest{Key: "g"},
			&FindCoordinatorResponse{NodeID: 3, Host: "h", Port: 1},
		},
		APIGroupHeartbeat: {
			&GroupHeartbeatRequest{GroupID: "g", MemberID: "m", MemberEpoch: 4, Topics: []string{"t"},
				Owned: []TopicPartitions{{Topic: "t", Partitions: []int32{0, 2}}}},
			&GroupHeartbeatResponse{MemberID: "m", MemberEpoch: 5, HeartbeatIntervalMs: 1000,
				Assignment: []TopicPartitions{{Topic: "t", Partitions: []int32{1}}}},
		},
		APIOffsetCommit: {
			&OffsetCommitRequest{GroupID: "g", MemberID: "m", MemberEpoch: 2, Offsets: []PartitionOffset{{Topic: "t", Partition: 1, Offset: 100}}},
			&OffsetCommitResponse{Error: ErrUnknownMember},
		},
		APIOffsetFetch: {
			&OffsetFetchRequest{GroupID: "g", Partitions: []TopicPartitions{{Topic: "t", Partitions: []int32{0, 1}}}},
			&OffsetFetchResponse{Offsets: []PartitionOffset{{Topic: "t", Partition: 0, Offset: -1}, {Topic: "t", Partition: 1, Offset: 5}}},
		},
		APIShareFetch: {
			&ShareFetchRequest{GroupID: "g", MemberID: "m", Topic: "t", Partition: 0, MaxRecords: 10, MaxWaitMs: 100,
				ResetToEarliest: true, Acks: []ShareAck{{First: 1, Last: 3, Type: AckAccept}, {First: 4, Last: 4, Type: AckRenew}}},
			&ShareFetchResponse{AckError: ErrInvalidRecordState, Acquired: []AcquiredRange{{First: 5, Last: 9, DeliveryCount: 2}}, Records: []byte{9}},
		},
		APIShareAcknowledge: {
			&ShareAcknowledgeRequest{GroupID: "g", MemberID: "m", Topic: "t", Partition: 0, Acks: []ShareAck{{First: 0, Last: 0, Type: AckReject}}},
			&ShareAcknowledgeResponse{},
		},
	}
}

func TestRoundTripEveryAPI(t *testing.T) {
	for key, pair := range samples() {
		for i, msg := range pair {
			var fresh Message
			if i == 0 {
				fresh = NewRequest(key)
			} else {
				fresh = NewResponse(key)
			}
			w := &Writer{}
			msg.Encode(w)
			if err := fresh.Decode(NewReader(w.B)); err != nil {
				t.Fatalf("%v[%d]: decode: %v", key, i, err)
			}
			if !reflect.DeepEqual(msg, fresh) {
				t.Fatalf("%v[%d]: round trip mismatch\n got %+v\nwant %+v", key, i, fresh, msg)
			}
			// Truncated input must fail cleanly.
			if len(w.B) > 0 {
				if err := NewRequest(key).Decode(NewReader(w.B[:len(w.B)-1])); err == nil && i == 0 {
					t.Fatalf("%v: truncated request decoded without error", key)
				}
			}
		}
	}
}

func TestFrames(t *testing.T) {
	req := &ProduceRequest{Topic: "t", Batch: []byte("x")}
	frame := EncodeRequest(RequestHeader{APIKey: APIProduce, Version: 0, CorrelationID: 9, ClientID: "c"}, req)

	body, err := ReadFrame(bufio.NewReader(bytes.NewReader(frame)), nil)
	if err != nil {
		t.Fatal(err)
	}
	h, payload, err := DecodeRequestHeader(body)
	if err != nil || h.APIKey != APIProduce || h.CorrelationID != 9 || h.ClientID != "c" {
		t.Fatalf("header %+v %v", h, err)
	}
	var got ProduceRequest
	if err := got.Decode(NewReader(payload)); err != nil || got.Topic != "t" {
		t.Fatalf("payload %+v %v", got, err)
	}

	resp := EncodeResponse(9, &ProduceResponse{BaseOffset: 3})
	body, _ = ReadFrame(bytes.NewReader(resp), nil)
	id, rest, err := DecodeResponse(body)
	if err != nil || id != 9 {
		t.Fatal("response correlation id")
	}
	var pr ProduceResponse
	if err := pr.Decode(NewReader(rest)); err != nil || pr.BaseOffset != 3 {
		t.Fatal("response payload")
	}

	huge := binary.BigEndian.AppendUint32(nil, MaxFrameSize+1)
	if _, err := ReadFrame(bytes.NewReader(huge), nil); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized frame: %v", err)
	}
}

func TestFetchHeaderMatchesEncode(t *testing.T) {
	m := &FetchResponse{HighWatermark: 5, Records: []byte("abcdef")}
	full := &Writer{}
	m.Encode(full)
	hdr := &Writer{}
	m.EncodeHeader(hdr, len(m.Records))
	if len(hdr.B) != FetchResponseHeaderSize || !bytes.Equal(append(hdr.B, m.Records...), full.B) {
		t.Fatal("EncodeHeader + raw records must equal Encode")
	}
}

func TestHostileArrayLengthIsRejected(t *testing.T) {
	w := &Writer{}
	w.Int32(1 << 30) // a billion strings in a few bytes
	if err := (&MetadataRequest{}).Decode(NewReader(w.B)); err == nil {
		t.Fatal("expected an error")
	}
}

func TestRetriable(t *testing.T) {
	if !ErrNotLeader.Retriable() || ErrCorruptMessage.Retriable() {
		t.Fatal("retriable classification")
	}
	if ErrNotLeader.Error() != "sluice: NOT_LEADER" {
		t.Fatal(ErrNotLeader.Error())
	}
}

func FuzzDecode(f *testing.F) {
	for key, pair := range samples() {
		for _, m := range pair {
			w := &Writer{}
			m.Encode(w)
			f.Add(int16(key), w.B)
		}
	}
	f.Fuzz(func(t *testing.T, key int16, data []byte) {
		if m := NewRequest(APIKey(key)); m != nil {
			_ = m.Decode(NewReader(data))
		}
		if m := NewResponse(APIKey(key)); m != nil {
			_ = m.Decode(NewReader(data))
		}
		_, _, _ = DecodeRequestHeader(data)
	})
}
