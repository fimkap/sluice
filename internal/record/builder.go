package record

import (
	"encoding/binary"
	"math"
)

// Builder accumulates records into one batch. The zero value is not usable; call NewBuilder.
type Builder struct {
	buf          []byte
	count        int32
	firstTS      int64
	maxTS        int64
	producerID   int64
	epoch        int16
	baseSequence int32
}

// NewBuilder starts a batch. Pass NoProducerID and NoSequence for a non-idempotent producer.
func NewBuilder(producerID int64, epoch int16, baseSequence int32) *Builder {
	b := &Builder{buf: make([]byte, HeaderSize, 1024)}
	b.Reset(producerID, epoch, baseSequence)
	return b
}

// Reset empties the builder, keeping its buffer.
func (b *Builder) Reset(producerID int64, epoch int16, baseSequence int32) {
	b.buf = b.buf[:HeaderSize]
	b.count = 0
	b.firstTS, b.maxTS = 0, math.MinInt64
	b.producerID, b.epoch, b.baseSequence = producerID, epoch, baseSequence
}

// Count is the number of records added so far.
func (b *Builder) Count() int { return int(b.count) }

// Size is the encoded size of the batch so far.
func (b *Builder) Size() int { return len(b.buf) }

// Append adds a record with a timestamp in milliseconds. key or value may be nil.
func (b *Builder) Append(timestamp int64, key, value []byte, headers []Header) {
	if b.count == 0 {
		b.firstTS = timestamp
	}
	if timestamp > b.maxTS {
		b.maxTS = timestamp
	}
	tsDelta := timestamp - b.firstTS
	offDelta := int64(b.count)

	body := 1 + varintLen(tsDelta) + varintLen(offDelta) + bytesLen(key) + bytesLen(value) +
		varintLen(int64(len(headers)))
	for _, h := range headers {
		body += bytesLen([]byte(h.Key)) + bytesLen(h.Value)
	}

	b.buf = binary.AppendVarint(b.buf, int64(body))
	b.buf = append(b.buf, 0) // attributes
	b.buf = binary.AppendVarint(b.buf, tsDelta)
	b.buf = binary.AppendVarint(b.buf, offDelta)
	b.buf = appendBytes(b.buf, key)
	b.buf = appendBytes(b.buf, value)
	b.buf = binary.AppendVarint(b.buf, int64(len(headers)))
	for _, h := range headers {
		b.buf = appendBytes(b.buf, []byte(h.Key))
		b.buf = appendBytes(b.buf, h.Value)
	}
	b.count++
}

// Build fills in the header and returns the batch with base offset 0. The returned slice is a copy, so
// the builder can be reset and reused.
func (b *Builder) Build() Batch {
	out := make(Batch, len(b.buf))
	copy(out, b.buf)
	h := out[:HeaderSize]
	binary.BigEndian.PutUint64(h[offBaseOffset:], 0)
	binary.BigEndian.PutUint32(h[offBatchLength:], uint32(len(out)-LogOverhead))
	h[offMagic] = Magic
	binary.BigEndian.PutUint16(h[offAttributes:], 0)
	binary.BigEndian.PutUint32(h[offLastOffsetDelta:], uint32(b.count-1))
	binary.BigEndian.PutUint64(h[offFirstTimestamp:], uint64(b.firstTS))
	binary.BigEndian.PutUint64(h[offMaxTimestamp:], uint64(b.maxTS))
	binary.BigEndian.PutUint64(h[offProducerID:], uint64(b.producerID))
	binary.BigEndian.PutUint16(h[offProducerEpoch:], uint16(b.epoch))
	binary.BigEndian.PutUint32(h[offBaseSequence:], uint32(b.baseSequence))
	binary.BigEndian.PutUint32(h[offRecordCount:], uint32(b.count))
	binary.BigEndian.PutUint32(h[offCRC:], out.ComputeCRC())
	return out
}

func varintLen(v int64) int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutVarint(tmp[:], v)
}

func bytesLen(p []byte) int {
	if p == nil {
		return varintLen(-1)
	}
	return varintLen(int64(len(p))) + len(p)
}

func appendBytes(dst, p []byte) []byte {
	if p == nil {
		return binary.AppendVarint(dst, -1)
	}
	dst = binary.AppendVarint(dst, int64(len(p)))
	return append(dst, p...)
}
