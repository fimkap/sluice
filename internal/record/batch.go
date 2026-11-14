// Package record implements the sluice record batch: the unit that producers send, the log stores and
// consumers fetch. The bytes are identical in all three places, so the broker never re-encodes a batch.
//
// Layout (big-endian), see docs/design.md:
//
//	off size field
//	  0   8  baseOffset       assigned by the leader; not covered by the CRC
//	  8   4  batchLength      bytes after this field
//	 12   4  crc              CRC-32C of bytes [16, end)
//	 16   1  magic = 1
//	 17   2  attributes
//	 19   4  lastOffsetDelta
//	 23   8  firstTimestamp
//	 31   8  maxTimestamp
//	 39   8  producerId
//	 47   2  producerEpoch
//	 49   4  baseSequence
//	 53   4  recordCount
//	 57   …  records
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// HeaderSize is the fixed size of a batch header.
	HeaderSize = 57
	// LogOverhead is the part of the header before batchLength's payload: baseOffset + batchLength.
	LogOverhead = 12
	// Magic is the only batch format version this code reads and writes.
	Magic = 1
	// MaxBatchSize bounds a single batch; larger batches are rejected as corrupt.
	MaxBatchSize = 64 << 20

	offBaseOffset      = 0
	offBatchLength     = 8
	offCRC             = 12
	offMagic           = 16
	offAttributes      = 17
	offLastOffsetDelta = 19
	offFirstTimestamp  = 23
	offMaxTimestamp    = 31
	offProducerID      = 39
	offProducerEpoch   = 47
	offBaseSequence    = 49
	offRecordCount     = 53

	// NoProducerID marks a batch from a non-idempotent producer.
	NoProducerID = -1
	// NoSequence marks a batch without sequence numbers.
	NoSequence = -1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors returned by validation. They all mean the batch must not be stored or delivered.
var (
	ErrShortBatch = errors.New("record: batch shorter than its header")
	ErrBadLength  = errors.New("record: batch length does not match the buffer")
	ErrBadMagic   = errors.New("record: unsupported batch magic")
	ErrBadCRC     = errors.New("record: CRC mismatch")
	ErrBadRecords = errors.New("record: malformed records")
)

// Batch is a complete encoded record batch. Accessors assume the header is present; call ValidateHeader
// (or Validate) on untrusted input first.
type Batch []byte

func (b Batch) BaseOffset() int64 { return int64(binary.BigEndian.Uint64(b[offBaseOffset:])) }

// SetBaseOffset assigns the batch's first offset. The CRC does not cover it, so it stays valid.
func (b Batch) SetBaseOffset(o int64) { binary.BigEndian.PutUint64(b[offBaseOffset:], uint64(o)) }

func (b Batch) BatchLength() int32 { return int32(binary.BigEndian.Uint32(b[offBatchLength:])) }

// Size is the total encoded size, header included.
func (b Batch) Size() int { return LogOverhead + int(b.BatchLength()) }

func (b Batch) CRC() uint32            { return binary.BigEndian.Uint32(b[offCRC:]) }
func (b Batch) Magic() int8            { return int8(b[offMagic]) }
func (b Batch) Attributes() int16      { return int16(binary.BigEndian.Uint16(b[offAttributes:])) }
func (b Batch) LastOffsetDelta() int32 { return int32(binary.BigEndian.Uint32(b[offLastOffsetDelta:])) }
func (b Batch) FirstTimestamp() int64  { return int64(binary.BigEndian.Uint64(b[offFirstTimestamp:])) }
func (b Batch) MaxTimestamp() int64    { return int64(binary.BigEndian.Uint64(b[offMaxTimestamp:])) }
func (b Batch) ProducerID() int64      { return int64(binary.BigEndian.Uint64(b[offProducerID:])) }
func (b Batch) ProducerEpoch() int16   { return int16(binary.BigEndian.Uint16(b[offProducerEpoch:])) }
func (b Batch) BaseSequence() int32    { return int32(binary.BigEndian.Uint32(b[offBaseSequence:])) }
func (b Batch) RecordCount() int32     { return int32(binary.BigEndian.Uint32(b[offRecordCount:])) }

// LastOffset is the offset of the batch's last record.
func (b Batch) LastOffset() int64 { return b.BaseOffset() + int64(b.LastOffsetDelta()) }

// NextOffset is the offset that follows the batch.
func (b Batch) NextOffset() int64 { return b.LastOffset() + 1 }

// LastSequence is the sequence number of the last record, or NoSequence.
func (b Batch) LastSequence() int32 {
	if b.BaseSequence() == NoSequence {
		return NoSequence
	}
	return b.BaseSequence() + b.LastOffsetDelta()
}

// ComputeCRC returns the CRC-32C over the part of the batch the checksum covers.
func (b Batch) ComputeCRC() uint32 { return crc32.Checksum(b[offMagic:], castagnoli) }

// ValidateHeader checks everything that can be checked without reading the records: sizes, magic and
// the record count. It is cheap and makes the accessors safe to call.
func (b Batch) ValidateHeader() error {
	if len(b) < HeaderSize {
		return ErrShortBatch
	}
	n := b.BatchLength()
	if n < HeaderSize-LogOverhead || n > MaxBatchSize || int(n)+LogOverhead != len(b) {
		return ErrBadLength
	}
	if b.Magic() != Magic {
		return ErrBadMagic
	}
	count := b.RecordCount()
	if count <= 0 || b.LastOffsetDelta() != count-1 {
		return fmt.Errorf("%w: recordCount %d, lastOffsetDelta %d", ErrBadRecords, count, b.LastOffsetDelta())
	}
	return nil
}

// Validate checks the header, the CRC and that the records decode to exactly recordCount entries with
// consecutive offset deltas. The broker runs it on every produced batch.
func (b Batch) Validate() error {
	if err := b.ValidateHeader(); err != nil {
		return err
	}
	if b.ComputeCRC() != b.CRC() {
		return ErrBadCRC
	}
	var i int32
	err := b.Records(func(r Record) error {
		if r.Offset != b.BaseOffset()+int64(i) {
			return fmt.Errorf("%w: offset delta %d at index %d", ErrBadRecords, r.Offset-b.BaseOffset(), i)
		}
		i++
		return nil
	})
	if err != nil {
		return err
	}
	if i != b.RecordCount() {
		return fmt.Errorf("%w: %d records, header says %d", ErrBadRecords, i, b.RecordCount())
	}
	return nil
}

// Next splits the first complete batch off buf. ok is false when buf holds only part of a batch (a fetch
// may end with a truncated one). A complete batch is returned even if it is corrupt; validate it.
func Next(buf []byte) (batch Batch, rest []byte, ok bool) {
	if len(buf) < LogOverhead {
		return nil, buf, false
	}
	n := int(int32(binary.BigEndian.Uint32(buf[offBatchLength:])))
	if n < 0 || n > MaxBatchSize {
		return nil, buf, false
	}
	size := LogOverhead + n
	if len(buf) < size {
		return nil, buf, false
	}
	return Batch(buf[:size:size]), buf[size:], true
}

// PeekHeader reads the offset range and size of a batch from its first bytes, without the records.
// buf must hold at least PeekSize bytes.
func PeekHeader(buf []byte) (baseOffset int64, size int, lastOffsetDelta int32) {
	baseOffset = int64(binary.BigEndian.Uint64(buf[offBaseOffset:]))
	size = LogOverhead + int(int32(binary.BigEndian.Uint32(buf[offBatchLength:])))
	lastOffsetDelta = int32(binary.BigEndian.Uint32(buf[offLastOffsetDelta:]))
	return
}

// PeekSize is how many bytes PeekHeader needs.
const PeekSize = offLastOffsetDelta + 4
