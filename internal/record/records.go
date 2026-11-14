package record

import (
	"encoding/binary"
	"fmt"
)

// Header is a record header: a string key and an opaque value.
type Header struct {
	Key   string
	Value []byte
}

// Record is one decoded record. Key, Value and header values alias the batch buffer; copy them if the
// buffer is reused.
type Record struct {
	Offset    int64
	Timestamp int64 // milliseconds since the epoch
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Records calls fn for every record in the batch, in order. It stops at the first error from fn or from
// decoding. It does not check the CRC; Validate does.
func (b Batch) Records(fn func(Record) error) error {
	if len(b) < HeaderSize {
		return ErrShortBatch
	}
	base, firstTS := b.BaseOffset(), b.FirstTimestamp()
	d := decoder{buf: b[HeaderSize:]}
	for n := b.RecordCount(); n > 0 && len(d.buf) > 0; n-- {
		length := d.varint()
		if d.err != nil || length < 0 || int64(len(d.buf)) < length {
			return fmt.Errorf("%w: bad record length", ErrBadRecords)
		}
		rd := decoder{buf: d.buf[:length]}
		d.buf = d.buf[length:]

		var r Record
		_ = rd.byte() // attributes, unused
		r.Timestamp = firstTS + rd.varint()
		r.Offset = base + rd.varint()
		r.Key = rd.bytes()
		r.Value = rd.bytes()
		if hc := rd.varint(); hc > 0 {
			if hc > int64(len(rd.buf)) { // each header needs at least two bytes
				return fmt.Errorf("%w: header count %d", ErrBadRecords, hc)
			}
			r.Headers = make([]Header, 0, hc)
			for ; hc > 0; hc-- {
				k := rd.bytes()
				v := rd.bytes()
				r.Headers = append(r.Headers, Header{Key: string(k), Value: v})
			}
		}
		if rd.err != nil || len(rd.buf) != 0 {
			return fmt.Errorf("%w: record at offset %d", ErrBadRecords, r.Offset)
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	if len(d.buf) != 0 {
		return fmt.Errorf("%w: %d trailing bytes", ErrBadRecords, len(d.buf))
	}
	return nil
}

// AppendRecords decodes the batch and appends its records to dst.
func (b Batch) AppendRecords(dst []Record) ([]Record, error) {
	err := b.Records(func(r Record) error {
		dst = append(dst, r)
		return nil
	})
	return dst, err
}

type decoder struct {
	buf []byte
	err error
}

func (d *decoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.buf)
	if n <= 0 {
		d.err = ErrBadRecords
		return 0
	}
	d.buf = d.buf[n:]
	return v
}

func (d *decoder) byte() byte {
	if d.err != nil || len(d.buf) == 0 {
		d.err = ErrBadRecords
		return 0
	}
	c := d.buf[0]
	d.buf = d.buf[1:]
	return c
}

// bytes reads a varint length and that many bytes; -1 means nil.
func (d *decoder) bytes() []byte {
	n := d.varint()
	if d.err != nil {
		return nil
	}
	if n == -1 {
		return nil
	}
	if n < 0 || n > int64(len(d.buf)) {
		d.err = ErrBadRecords
		return nil
	}
	v := d.buf[:n:n]
	d.buf = d.buf[n:]
	return v
}
