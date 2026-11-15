package protocol

import "fmt"

// APIKey identifies a request type.
type APIKey int16

const (
	APIProduce          APIKey = 0
	APIFetch            APIKey = 1
	APIListOffsets      APIKey = 2
	APIMetadata         APIKey = 3
	APICreateTopic      APIKey = 4
	APIInitProducerID   APIKey = 5
	APIFindCoordinator  APIKey = 6
	APIGroupHeartbeat   APIKey = 7
	APIOffsetCommit     APIKey = 8
	APIOffsetFetch      APIKey = 9
	APIShareFetch       APIKey = 10
	APIShareAcknowledge APIKey = 11
)

var apiNames = map[APIKey]string{
	APIProduce: "Produce", APIFetch: "Fetch", APIListOffsets: "ListOffsets", APIMetadata: "Metadata",
	APICreateTopic: "CreateTopic", APIInitProducerID: "InitProducerId", APIFindCoordinator: "FindCoordinator",
	APIGroupHeartbeat: "GroupHeartbeat", APIOffsetCommit: "OffsetCommit", APIOffsetFetch: "OffsetFetch",
	APIShareFetch: "ShareFetch", APIShareAcknowledge: "ShareAcknowledge",
}

func (k APIKey) String() string {
	if n, ok := apiNames[k]; ok {
		return n
	}
	return fmt.Sprintf("APIKey(%d)", int16(k))
}

// ErrorCode is the error carried in responses. ErrNone means success.
type ErrorCode int16

const (
	ErrNone                    ErrorCode = 0
	ErrUnknownTopicOrPartition ErrorCode = 1
	ErrNotLeader               ErrorCode = 2
	ErrCorruptMessage          ErrorCode = 3
	ErrInvalidRequest          ErrorCode = 4
	ErrRequestTimedOut         ErrorCode = 5
	ErrNotEnoughReplicas       ErrorCode = 6
	ErrOffsetOutOfRange        ErrorCode = 7
	ErrOutOfOrderSequence      ErrorCode = 8
	ErrUnknownMember           ErrorCode = 9
	ErrStaleMemberEpoch        ErrorCode = 10
	ErrTopicExists             ErrorCode = 11
	ErrNotCoordinator          ErrorCode = 12
	ErrInvalidRecordState      ErrorCode = 13
	ErrMessageTooLarge         ErrorCode = 14
	ErrInvalidTopic            ErrorCode = 15
	ErrBrokerNotAvailable      ErrorCode = 16
	ErrInternal                ErrorCode = 17
)

var errorNames = map[ErrorCode]string{
	ErrNone: "NONE", ErrUnknownTopicOrPartition: "UNKNOWN_TOPIC_OR_PARTITION", ErrNotLeader: "NOT_LEADER",
	ErrCorruptMessage: "CORRUPT_MESSAGE", ErrInvalidRequest: "INVALID_REQUEST", ErrRequestTimedOut: "REQUEST_TIMED_OUT",
	ErrNotEnoughReplicas: "NOT_ENOUGH_REPLICAS", ErrOffsetOutOfRange: "OFFSET_OUT_OF_RANGE",
	ErrOutOfOrderSequence: "OUT_OF_ORDER_SEQUENCE", ErrUnknownMember: "UNKNOWN_MEMBER",
	ErrStaleMemberEpoch: "STALE_MEMBER_EPOCH", ErrTopicExists: "TOPIC_ALREADY_EXISTS",
	ErrNotCoordinator: "NOT_COORDINATOR", ErrInvalidRecordState: "INVALID_RECORD_STATE",
	ErrMessageTooLarge: "MESSAGE_TOO_LARGE", ErrInvalidTopic: "INVALID_TOPIC",
	ErrBrokerNotAvailable: "BROKER_NOT_AVAILABLE", ErrInternal: "INTERNAL",
}

func (e ErrorCode) String() string {
	if n, ok := errorNames[e]; ok {
		return n
	}
	return fmt.Sprintf("ErrorCode(%d)", int16(e))
}

// Error makes an ErrorCode usable as a Go error.
func (e ErrorCode) Error() string { return "sluice: " + e.String() }

// Retriable reports whether a client may retry the same request.
func (e ErrorCode) Retriable() bool {
	switch e {
	case ErrNotLeader, ErrRequestTimedOut, ErrNotEnoughReplicas, ErrBrokerNotAvailable, ErrNotCoordinator:
		return true
	}
	return false
}

// Acks values for Produce.
const (
	AcksNone   int16 = 0
	AcksLeader int16 = 1
	AcksAll    int16 = -1
)

// Special timestamps for ListOffsets.
const (
	LatestOffset   int64 = -1
	EarliestOffset int64 = -2
)

// ConsumerReplicaID marks a fetch from a consumer rather than a follower.
const ConsumerReplicaID int32 = -1

// AckType is a share-group acknowledgement.
type AckType int8

const (
	AckAccept  AckType = 1 // processed: never deliver again
	AckRelease AckType = 2 // give it back: deliver again (to anyone)
	AckReject  AckType = 3 // unprocessable: archive without redelivery
	AckRenew   AckType = 4 // still working on it: extend the lock (Kafka 4.2)
)

func (a AckType) String() string {
	switch a {
	case AckAccept:
		return "ACCEPT"
	case AckRelease:
		return "RELEASE"
	case AckReject:
		return "REJECT"
	case AckRenew:
		return "RENEW"
	}
	return fmt.Sprintf("AckType(%d)", int8(a))
}
