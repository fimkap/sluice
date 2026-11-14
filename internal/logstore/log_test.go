package logstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fimkap/sluice/internal/record"
)

// batchOf builds a batch of n records whose values encode (tag, i), so reads can be checked.
func batchOf(tag string, n, valueSize int) record.Batch {
	b := record.NewBuilder(record.NoProducerID, 0, record.NoSequence)
	for i := 0; i < n; i++ {
		v := []byte(fmt.Sprintf("%s-%d-", tag, i))
		for len(v) < valueSize {
			v = append(v, 'x')
		}
		b.Append(time.Now().UnixMilli(), nil, v, nil)
	}
	return b.Build()
}

func mustOpen(t *testing.T, dir string, cfg Config) *Log {
	t.Helper()
	l, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func mustAppend(t *testing.T, l *Log, b record.Batch) AppendResult {
	t.Helper()
	res, err := l.Append(b)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return res
}

// readAll reads every record from offset up to the log end and returns their offsets in order.
func readAll(t *testing.T, l *Log, from int64) []int64 {
	t.Helper()
	var offsets []int64
	next := from
	for next < l.LEO().Offset {
		r, err := l.Read(next, 16<<10, l.LEO())
		if err != nil {
			t.Fatalf("Read(%d): %v", next, err)
		}
		if r == nil {
			t.Fatalf("Read(%d) returned nothing before LEO %d", next, l.LEO().Offset)
		}
		data, err := r.ReadAll(nil)
		r.Release()
		if err != nil {
			t.Fatal(err)
		}
		for {
			b, rest, ok := record.Next(data)
			if !ok {
				break
			}
			data = rest
			if err := b.Validate(); err != nil {
				t.Fatalf("batch at %d: %v", b.BaseOffset(), err)
			}
			_ = b.Records(func(rec record.Record) error {
				if rec.Offset >= next {
					offsets = append(offsets, rec.Offset)
				}
				return nil
			})
			next = b.NextOffset()
		}
	}
	return offsets
}

func assertContiguous(t *testing.T, offsets []int64, from, to int64) {
	t.Helper()
	if int64(len(offsets)) != to-from {
		t.Fatalf("got %d offsets, want %d (from %d to %d)", len(offsets), to-from, from, to)
	}
	for i, o := range offsets {
		if o != from+int64(i) {
			t.Fatalf("offset %d at index %d, want %d", o, i, from+int64(i))
		}
	}
}

func TestAppendAndRead(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{})
	for i := 0; i < 50; i++ {
		res := mustAppend(t, l, batchOf("a", 4, 20))
		if res.Base != int64(i*4) || res.End.Offset != int64(i*4+4) {
			t.Fatalf("append %d: base %d end %d", i, res.Base, res.End.Offset)
		}
	}
	assertContiguous(t, readAll(t, l, 0), 0, 200)
	// Reading from the middle of a batch starts with that batch.
	r, err := l.Read(10, 1<<20, l.LEO())
	if err != nil || r == nil {
		t.Fatal(err)
	}
	defer r.Release()
	if r.Start.Offset != 8 {
		t.Fatalf("batch containing 10 starts at %d, want 8", r.Start.Offset)
	}
	if r, err := l.Read(200, 100, l.LEO()); r != nil || err != nil {
		t.Fatal("reading at the log end should return nothing")
	}
	if _, err := l.Read(201, 100, l.LEO()); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read past end: %v", err)
	}
}

func TestSegmentsRollAndIndexLookups(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Config{SegmentBytes: 4096, IndexIntervalBytes: 300})
	for i := 0; i < 200; i++ {
		mustAppend(t, l, batchOf("s", 3, 30))
	}
	st := l.Stats()
	if st.Segments < 5 {
		t.Fatalf("expected many segments, got %d", st.Segments)
	}
	// Every offset must resolve to the batch that contains it.
	for o := int64(0); o < 600; o += 7 {
		r, err := l.Read(o, 1, l.LEO())
		if err != nil || r == nil {
			t.Fatalf("Read(%d): %v", o, err)
		}
		if r.Start.Offset > o || o >= r.Start.Offset+3 {
			t.Fatalf("Read(%d) started at batch %d", o, r.Start.Offset)
		}
		r.Release()
	}
	assertContiguous(t, readAll(t, l, 0), 0, 600)
}

func TestRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		mustAppend(t, l, batchOf("r", 5, 40))
	}
	l.Close()

	// Simulate a crash in the middle of a write: half a batch at the end of the active segment.
	path, _ := segmentPaths(dir, 0)
	half := batchOf("torn", 5, 40)
	half.SetBaseOffset(50)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write(half[:len(half)/2])
	f.Close()

	l2 := mustOpen(t, dir, Config{})
	if got := l2.LEO().Offset; got != 50 {
		t.Fatalf("LEO after recovery = %d, want 50", got)
	}
	if l2.Stats().TruncatedOnRecovery != int64(len(half)/2) {
		t.Fatalf("truncated %d bytes, want %d", l2.Stats().TruncatedOnRecovery, len(half)/2)
	}
	res := mustAppend(t, l2, batchOf("after", 2, 10))
	if res.Base != 50 {
		t.Fatalf("append after recovery got base %d", res.Base)
	}
	assertContiguous(t, readAll(t, l2, 0), 0, 52)
}

func TestRecoveryStopsAtCorruptBatch(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Config{})
	var positions []int64
	for i := 0; i < 6; i++ {
		positions = append(positions, l.LEO().Pos)
		mustAppend(t, l, batchOf("c", 2, 64))
	}
	l.Close()

	// Flip a byte inside the fourth batch's records: its CRC no longer matches.
	path, _ := segmentPaths(dir, 0)
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	var b [1]byte
	f.ReadAt(b[:], positions[3]+70)
	b[0] ^= 0xff
	f.WriteAt(b[:], positions[3]+70)
	f.Close()

	l2 := mustOpen(t, dir, Config{})
	if got := l2.LEO().Offset; got != 6 {
		t.Fatalf("LEO = %d, want 6 (three good batches of two)", got)
	}
	assertContiguous(t, readAll(t, l2, 0), 0, 6)
}

func TestRecoveryRebuildsMissingIndex(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Config{SegmentBytes: 2048, IndexIntervalBytes: 256})
	for i := 0; i < 60; i++ {
		mustAppend(t, l, batchOf("i", 2, 50))
	}
	l.Close()
	_, idx := segmentPaths(dir, 0) // the first, closed segment
	if err := os.Remove(idx); err != nil {
		t.Fatal(err)
	}
	l2 := mustOpen(t, dir, Config{SegmentBytes: 2048, IndexIntervalBytes: 256})
	assertContiguous(t, readAll(t, l2, 0), 0, 120)
}

func TestRetentionKeepsActiveAndMovesLogStart(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{SegmentBytes: 2048, RetentionBytes: 4096})
	for i := 0; i < 100; i++ {
		mustAppend(t, l, batchOf("k", 2, 50))
	}
	before := l.Stats().Segments
	deleted := l.EnforceRetention(time.Now())
	if deleted == 0 || l.Stats().Segments != before-deleted {
		t.Fatalf("deleted %d of %d segments", deleted, before)
	}
	if l.Stats().SizeBytes < 4096 {
		t.Fatalf("kept %d bytes, want at least 4096", l.Stats().SizeBytes)
	}
	start := l.LogStart()
	if start == 0 {
		t.Fatal("log start did not move")
	}
	if _, err := l.Read(0, 100, l.LEO()); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("reading a deleted offset: %v", err)
	}
	assertContiguous(t, readAll(t, l, start), start, 200)

	// Age-based: everything but the active segment is older than a zero-length window.
	l.cfg.RetentionBytes = 0
	l.cfg.RetentionAge = time.Nanosecond
	l.EnforceRetention(time.Now().Add(time.Second))
	if l.Stats().Segments != 1 {
		t.Fatalf("age retention left %d segments", l.Stats().Segments)
	}
}

// A read holds its segment: retention deleting the file must not break it.
func TestReadSurvivesRetention(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{SegmentBytes: 1024, RetentionBytes: 1})
	for i := 0; i < 20; i++ {
		mustAppend(t, l, batchOf("h", 2, 100))
	}
	r, err := l.Read(0, 1<<20, l.LEO())
	if err != nil || r == nil {
		t.Fatal(err)
	}
	if l.EnforceRetention(time.Now()) == 0 {
		t.Fatal("expected retention to delete the segment being read")
	}
	data, err := r.ReadAll(nil)
	r.Release()
	if err != nil {
		t.Fatal(err)
	}
	b, _, ok := record.Next(data)
	if !ok || b.Validate() != nil || b.BaseOffset() != 0 {
		t.Fatal("could not read the deleted segment through the held reference")
	}
}

func TestTruncateTo(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Config{SegmentBytes: 1024})
	for i := 0; i < 40; i++ {
		mustAppend(t, l, batchOf("t", 4, 30)) // offsets 0..159
	}
	if err := l.TruncateTo(100); err != nil { // a batch boundary
		t.Fatal(err)
	}
	if l.LEO().Offset != 100 {
		t.Fatalf("LEO %d", l.LEO().Offset)
	}
	if err := l.TruncateTo(42); err != nil { // inside the batch 40..43
		t.Fatal(err)
	}
	if l.LEO().Offset != 40 {
		t.Fatalf("LEO after mid-batch truncate %d, want 40", l.LEO().Offset)
	}
	if res := mustAppend(t, l, batchOf("u", 1, 5)); res.Base != 40 {
		t.Fatalf("append after truncate at %d", res.Base)
	}
	assertContiguous(t, readAll(t, l, 0), 0, 41)

	// Survives a reopen.
	l.Close()
	l2 := mustOpen(t, dir, Config{SegmentBytes: 1024})
	assertContiguous(t, readAll(t, l2, 0), 0, 41)
}

func TestAppendReplicaRequiresLogEnd(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{})
	b := batchOf("r", 2, 5)
	b.SetBaseOffset(5)
	if _, err := l.AppendReplica(b); !errors.Is(err, ErrOffsetMismatch) {
		t.Fatalf("got %v", err)
	}
	b.SetBaseOffset(0)
	if res, err := l.AppendReplica(b); err != nil || res.End.Offset != 2 {
		t.Fatalf("got %v %v", res, err)
	}
}

func TestReadStopsAtLimit(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{})
	var hw Position
	for i := 0; i < 10; i++ {
		res := mustAppend(t, l, batchOf("l", 2, 10))
		if i == 4 {
			hw = res.End // offsets 0..9 committed
		}
	}
	r, err := l.Read(0, 1<<20, hw)
	if err != nil || r == nil {
		t.Fatal(err)
	}
	data, _ := r.ReadAll(nil)
	r.Release()
	var last int64 = -1
	for {
		b, rest, ok := record.Next(data)
		if !ok {
			break
		}
		last, data = b.LastOffset(), rest
	}
	if last != 9 || len(data) != 0 {
		t.Fatalf("read up to %d with %d stray bytes, want up to 9", last, len(data))
	}
	if r, _ := l.Read(10, 100, hw); r != nil {
		t.Fatal("read past the limit")
	}
}

func TestPositionOf(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{SegmentBytes: 1024})
	var ends []Position
	for i := 0; i < 30; i++ {
		ends = append(ends, mustAppend(t, l, batchOf("p", 3, 40)).End)
	}
	for _, want := range ends {
		got, err := l.PositionOf(want.Offset)
		if err != nil {
			t.Fatalf("PositionOf(%d): %v", want.Offset, err)
		}
		// A segment end and the next segment's start are the same offset; either is a valid answer.
		if got.Offset != want.Offset {
			t.Fatalf("PositionOf(%d) = %+v", want.Offset, got)
		}
	}
	if _, err := l.PositionOf(4); err == nil {
		t.Fatal("offset inside a batch should be rejected")
	}
}

func TestGroupCommitSharesFsyncs(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{Fsync: FsyncGroup})
	const writers, each = 32, 25
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				res, err := l.Append(batchOf("g", 1, 100))
				if err == nil {
					err = l.WaitAcked(nil, res.End.Offset)
				}
				if err != nil {
					errs <- err
					return
				}
				if l.Durable().Offset < res.End.Offset {
					errs <- fmt.Errorf("acked before durable")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	st := l.Stats()
	if st.Appends != writers*each {
		t.Fatalf("appends %d", st.Appends)
	}
	if st.Fsyncs >= st.Appends {
		t.Fatalf("group commit did not share fsyncs: %d fsyncs for %d appends", st.Fsyncs, st.Appends)
	}
	t.Logf("%d appends, %d fsyncs (%.1f appends per fsync)", st.Appends, st.Fsyncs, float64(st.Appends)/float64(st.Fsyncs))
}

func TestAlwaysSyncsEachAppend(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{Fsync: FsyncAlways})
	for i := 0; i < 20; i++ {
		res := mustAppend(t, l, batchOf("a", 1, 10))
		if l.Durable().Offset != res.End.Offset {
			t.Fatal("FsyncAlways returned before the data was durable")
		}
	}
	if st := l.Stats(); st.Fsyncs != 20 {
		t.Fatalf("fsyncs %d, want 20", st.Fsyncs)
	}
}

func TestIntervalPolicyEventuallySyncs(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{Fsync: FsyncInterval, FsyncInterval: 5 * time.Millisecond})
	res := mustAppend(t, l, batchOf("i", 1, 10))
	if l.Acked().Offset != res.End.Offset {
		t.Fatal("interval policy acknowledges on write")
	}
	deadline := time.Now().Add(2 * time.Second)
	for l.Durable().Offset < res.End.Offset {
		if time.Now().After(deadline) {
			t.Fatal("interval fsync never ran")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOnChangeFires(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	l := mustOpen(t, t.TempDir(), Config{OnChange: func() { mu.Lock(); calls++; mu.Unlock() }})
	mustAppend(t, l, batchOf("o", 1, 1))
	mu.Lock()
	defer mu.Unlock()
	if calls == 0 {
		t.Fatal("OnChange not called")
	}
}

func TestConcurrentAppendAndRead(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Config{SegmentBytes: 8192, Fsync: FsyncGroup})
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			res, err := l.Append(batchOf("c", 3, 50))
			if err != nil {
				t.Error(err)
				return
			}
			_ = l.WaitAcked(nil, res.End.Offset)
		}
		close(done)
	}()
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var next int64
			for {
				select {
				case <-done:
					if next >= 900 {
						return
					}
				default:
				}
				rr, err := l.Read(next, 4096, l.Acked())
				if err != nil {
					t.Error(err)
					return
				}
				if rr == nil {
					continue
				}
				data, _ := rr.ReadAll(nil)
				rr.Release()
				for {
					b, rest, ok := record.Next(data)
					if !ok {
						break
					}
					if err := b.Validate(); err != nil {
						t.Errorf("concurrent read saw a bad batch at %d: %v", b.BaseOffset(), err)
						return
					}
					next, data = b.NextOffset(), rest
				}
			}
		}()
	}
	wg.Wait()
}

func TestSegmentFileNames(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Config{SegmentBytes: 512})
	for i := 0; i < 20; i++ {
		mustAppend(t, l, batchOf("n", 1, 100))
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(matches) < 2 || filepath.Base(matches[0]) != "00000000000000000000.log" {
		t.Fatalf("segment files: %v", matches)
	}
}
