package record

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func buildSample(t testing.TB, n int) Batch {
	t.Helper()
	b := NewBuilder(42, 3, 100)
	for i := 0; i < n; i++ {
		var key []byte
		if i%3 != 0 {
			key = []byte(fmt.Sprintf("key-%d", i))
		}
		var hdrs []Header
		if i%2 == 0 {
			hdrs = []Header{{Key: "trace", Value: []byte{byte(i)}}, {Key: "empty", Value: nil}}
		}
		b.Append(int64(1_700_000_000_000+i*10), key, []byte(fmt.Sprintf("value-%d", i)), hdrs)
	}
	return b.Build()
}

func TestRoundTrip(t *testing.T) {
	batch := buildSample(t, 25)
	batch.SetBaseOffset(1000)
	if err := batch.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := batch.RecordCount(); got != 25 {
		t.Fatalf("RecordCount = %d", got)
	}
	if batch.ProducerID() != 42 || batch.ProducerEpoch() != 3 || batch.BaseSequence() != 100 {
		t.Fatalf("producer fields: %d %d %d", batch.ProducerID(), batch.ProducerEpoch(), batch.BaseSequence())
	}
	if batch.LastSequence() != 124 || batch.NextOffset() != 1025 {
		t.Fatalf("LastSequence %d NextOffset %d", batch.LastSequence(), batch.NextOffset())
	}
	if batch.FirstTimestamp() != 1_700_000_000_000 || batch.MaxTimestamp() != 1_700_000_000_240 {
		t.Fatalf("timestamps %d %d", batch.FirstTimestamp(), batch.MaxTimestamp())
	}

	recs, err := batch.AppendRecords(nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range recs {
		if r.Offset != int64(1000+i) {
			t.Errorf("record %d offset %d", i, r.Offset)
		}
		if r.Timestamp != int64(1_700_000_000_000+i*10) {
			t.Errorf("record %d timestamp %d", i, r.Timestamp)
		}
		wantKey := []byte(fmt.Sprintf("key-%d", i))
		if i%3 == 0 {
			wantKey = nil
		}
		if !bytes.Equal(r.Key, wantKey) || (wantKey == nil) != (r.Key == nil) {
			t.Errorf("record %d key %q", i, r.Key)
		}
		if string(r.Value) != fmt.Sprintf("value-%d", i) {
			t.Errorf("record %d value %q", i, r.Value)
		}
		if i%2 == 0 {
			if len(r.Headers) != 2 || r.Headers[0].Key != "trace" || r.Headers[0].Value[0] != byte(i) || r.Headers[1].Value != nil {
				t.Errorf("record %d headers %+v", i, r.Headers)
			}
		} else if len(r.Headers) != 0 {
			t.Errorf("record %d unexpected headers", i)
		}
	}
}

// The leader assigns offsets by overwriting baseOffset; the CRC must not cover it.
func TestBaseOffsetOutsideCRC(t *testing.T) {
	batch := buildSample(t, 3)
	crc := batch.CRC()
	batch.SetBaseOffset(1 << 40)
	if batch.ComputeCRC() != crc {
		t.Fatal("CRC changed after assigning the base offset")
	}
	if err := batch.Validate(); err != nil {
		t.Fatal(err)
	}
}

// Flipping any single bit the CRC covers must be detected.
func TestEveryBitFlipIsDetected(t *testing.T) {
	batch := buildSample(t, 4)
	for i := offMagic; i < len(batch); i++ {
		for bit := 0; bit < 8; bit++ {
			c := append(Batch(nil), batch...)
			c[i] ^= 1 << bit
			if err := c.Validate(); err == nil {
				t.Fatalf("flip at byte %d bit %d not detected", i, bit)
			}
		}
	}
}

func TestValidateRejects(t *testing.T) {
	good := buildSample(t, 2)
	cases := map[string]struct {
		mutate func(Batch) Batch
		want   error
	}{
		"short":      {func(b Batch) Batch { return b[:HeaderSize-1] }, ErrShortBatch},
		"truncated":  {func(b Batch) Batch { return b[:len(b)-1] }, ErrBadLength},
		"bad magic":  {func(b Batch) Batch { b[offMagic] = 9; return b }, ErrBadMagic},
		"bad count":  {func(b Batch) Batch { b[offRecordCount+3]++; return b }, ErrBadRecords},
		"bad length": {func(b Batch) Batch { b[offBatchLength] = 0x7f; return b }, ErrBadLength},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			b := tc.mutate(append(Batch(nil), good...))
			if err := b.Validate(); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestNextSplitsAndStopsAtPartial(t *testing.T) {
	a, b := buildSample(t, 2), buildSample(t, 5)
	buf := append(append(append([]byte{}, a...), b...), b[:20]...)

	first, rest, ok := Next(buf)
	if !ok || !bytes.Equal(first, a) {
		t.Fatal("first batch")
	}
	second, rest, ok := Next(rest)
	if !ok || !bytes.Equal(second, b) {
		t.Fatal("second batch")
	}
	if _, rest2, ok := Next(rest); ok || len(rest2) != 20 {
		t.Fatal("partial batch should not be returned")
	}

	base, size, lod := PeekHeader(b)
	if base != 0 || size != len(b) || lod != 4 {
		t.Fatalf("PeekHeader: %d %d %d", base, size, lod)
	}
}

func TestBuilderReuse(t *testing.T) {
	bl := NewBuilder(NoProducerID, 0, NoSequence)
	bl.Append(1, nil, []byte("a"), nil)
	first := bl.Build()
	bl.Reset(NoProducerID, 0, NoSequence)
	bl.Append(2, nil, []byte("b"), nil)
	bl.Append(3, nil, []byte("c"), nil)
	second := bl.Build()
	if first.RecordCount() != 1 || second.RecordCount() != 2 {
		t.Fatal("counts")
	}
	if err := first.Validate(); err != nil {
		t.Fatal("first batch was clobbered by reuse:", err)
	}
	if second.LastSequence() != NoSequence || second.ProducerID() != NoProducerID {
		t.Fatal("non-idempotent fields")
	}
}

func FuzzDecodeBatch(f *testing.F) {
	f.Add([]byte(buildSample(f, 3)))
	f.Add([]byte(buildSample(f, 1)))
	f.Add(make([]byte, HeaderSize))
	f.Fuzz(func(t *testing.T, data []byte) {
		b := Batch(data)
		if b.ValidateHeader() != nil {
			return
		}
		_ = b.Validate()
		_ = b.Records(func(Record) error { return nil })
		_, _, _ = Next(data)
	})
}

func BenchmarkValidate1KB(b *testing.B) {
	bl := NewBuilder(NoProducerID, 0, NoSequence)
	val := bytes.Repeat([]byte("x"), 100)
	for bl.Size() < 1024 {
		bl.Append(1, nil, val, nil)
	}
	batch := bl.Build()
	b.SetBytes(int64(len(batch)))
	for b.Loop() {
		if err := batch.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}
