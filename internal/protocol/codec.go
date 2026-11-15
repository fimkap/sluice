// Package protocol defines sluice's binary protocol: length-prefixed frames carrying requests and
// responses, matched by correlation id. All integers are big-endian.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize bounds a frame; larger frames close the connection.
const MaxFrameSize = 100 << 20

var (
	// ErrMalformed means a message could not be decoded.
	ErrMalformed = errors.New("protocol: malformed message")
	// ErrFrameTooLarge means a frame announced a size above MaxFrameSize.
	ErrFrameTooLarge = errors.New("protocol: frame too large")
)

// Writer appends encoded values to a byte slice.
type Writer struct{ B []byte }

func (w *Writer) Int8(v int8)   { w.B = append(w.B, byte(v)) }
func (w *Writer) Int16(v int16) { w.B = binary.BigEndian.AppendUint16(w.B, uint16(v)) }
func (w *Writer) Int32(v int32) { w.B = binary.BigEndian.AppendUint32(w.B, uint32(v)) }
func (w *Writer) Int64(v int64) { w.B = binary.BigEndian.AppendUint64(w.B, uint64(v)) }

func (w *Writer) Bool(v bool) {
	if v {
		w.Int8(1)
	} else {
		w.Int8(0)
	}
}

// String writes an int16 length and the bytes.
func (w *Writer) String(s string) {
	w.Int16(int16(len(s)))
	w.B = append(w.B, s...)
}

// Bytes writes an int32 length (-1 for nil) and the bytes.
func (w *Writer) Bytes(p []byte) {
	if p == nil {
		w.Int32(-1)
		return
	}
	w.Int32(int32(len(p)))
	w.B = append(w.B, p...)
}

// ArrayLen writes an array's element count.
func (w *Writer) ArrayLen(n int) { w.Int32(int32(n)) }

func (w *Writer) Int32s(v []int32) {
	w.ArrayLen(len(v))
	for _, x := range v {
		w.Int32(x)
	}
}

func (w *Writer) Strings(v []string) {
	w.ArrayLen(len(v))
	for _, s := range v {
		w.String(s)
	}
}

// Reader decodes values from a byte slice. The first error sticks; check Err once at the end.
type Reader struct {
	b   []byte
	err error
}

// NewReader returns a Reader over b. Byte slices it returns alias b.
func NewReader(b []byte) *Reader { return &Reader{b: b} }

// Err returns the first decoding error.
func (r *Reader) Err() error { return r.err }

// Remaining is the number of undecoded bytes.
func (r *Reader) Remaining() int { return len(r.b) }

func (r *Reader) fail() {
	if r.err == nil {
		r.err = ErrMalformed
	}
	r.b = nil
}

func (r *Reader) take(n int) []byte {
	if r.err != nil || n < 0 || n > len(r.b) {
		r.fail()
		return nil
	}
	p := r.b[:n:n]
	r.b = r.b[n:]
	return p
}

func (r *Reader) Int8() int8 {
	p := r.take(1)
	if p == nil {
		return 0
	}
	return int8(p[0])
}

func (r *Reader) Int16() int16 {
	p := r.take(2)
	if p == nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(p))
}

func (r *Reader) Int32() int32 {
	p := r.take(4)
	if p == nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(p))
}

func (r *Reader) Int64() int64 {
	p := r.take(8)
	if p == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(p))
}

func (r *Reader) Bool() bool { return r.Int8() != 0 }

func (r *Reader) String() string {
	n := int(r.Int16())
	if n < 0 {
		r.fail()
		return ""
	}
	return string(r.take(n))
}

func (r *Reader) Bytes() []byte {
	n := int(r.Int32())
	if r.err != nil || n == -1 {
		return nil
	}
	return r.take(n)
}

// ArrayLen reads an element count, rejecting counts that cannot fit in the remaining bytes at minSize
// bytes per element. This keeps a hostile count from forcing a huge allocation.
func (r *Reader) ArrayLen(minSize int) int {
	n := int(r.Int32())
	if r.err != nil {
		return 0
	}
	if n < 0 || (minSize > 0 && n > len(r.b)/minSize) {
		r.fail()
		return 0
	}
	return n
}

func (r *Reader) Int32s() []int32 {
	n := r.ArrayLen(4)
	if n == 0 {
		return nil
	}
	v := make([]int32, n)
	for i := range v {
		v[i] = r.Int32()
	}
	return v
}

func (r *Reader) Strings() []string {
	n := r.ArrayLen(2)
	if n == 0 {
		return nil
	}
	v := make([]string, n)
	for i := range v {
		v[i] = r.String()
	}
	return v
}

// done returns the sticky error, or ErrMalformed if bytes are left over.
func (r *Reader) done() error {
	if r.err != nil {
		return r.err
	}
	if len(r.b) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(r.b))
	}
	return nil
}

// ReadFrame reads one frame body. buf is reused when it is large enough.
func ReadFrame(rd io.Reader, buf []byte) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if uint32(cap(buf)) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if _, err := io.ReadFull(rd, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// RequestHeader starts every request body.
type RequestHeader struct {
	APIKey        APIKey
	Version       int16
	CorrelationID int32
	ClientID      string
}

func (h *RequestHeader) encode(w *Writer) {
	w.Int16(int16(h.APIKey))
	w.Int16(h.Version)
	w.Int32(h.CorrelationID)
	w.String(h.ClientID)
}

// DecodeRequestHeader splits a request frame into its header and payload.
func DecodeRequestHeader(frame []byte) (RequestHeader, []byte, error) {
	r := NewReader(frame)
	h := RequestHeader{APIKey: APIKey(r.Int16()), Version: r.Int16(), CorrelationID: r.Int32(), ClientID: r.String()}
	return h, r.b, r.err
}

// Message is a request or response payload.
type Message interface {
	Encode(w *Writer)
	Decode(r *Reader) error
}

// EncodeRequest returns a complete frame: size, header and payload.
func EncodeRequest(h RequestHeader, m Message) []byte {
	w := Writer{B: make([]byte, 4, 64)}
	h.encode(&w)
	m.Encode(&w)
	binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4))
	return w.B
}

// EncodeResponse returns a complete frame: size, correlation id and payload.
func EncodeResponse(correlationID int32, m Message) []byte {
	w := Writer{B: make([]byte, 4, 64)}
	w.Int32(correlationID)
	m.Encode(&w)
	binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4))
	return w.B
}

// DecodeResponse splits a response frame into its correlation id and payload.
func DecodeResponse(frame []byte) (int32, []byte, error) {
	if len(frame) < 4 {
		return 0, nil, ErrMalformed
	}
	return int32(binary.BigEndian.Uint32(frame)), frame[4:], nil
}
