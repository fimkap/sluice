package logstore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fimkap/sluice/internal/record"
)

// Log is one partition's log. Appends are serialized; reads run concurrently with appends.
type Log struct {
	dir string
	cfg Config

	mu       sync.RWMutex
	segments []*segment // ascending base offsets; the last one is active
	leo      Position   // log end offset: where the next batch goes
	closed   bool

	durMu   sync.Mutex
	durable Position      // everything before durable.Offset is on stable storage
	durCh   chan struct{} // closed and replaced when durable advances

	flushCh chan struct{}
	closing chan struct{}
	wg      sync.WaitGroup

	stats struct {
		appends, appendedBytes, fsyncs, fsyncNanos atomic.Uint64
		truncatedOnRecovery                        atomic.Int64
	}
}

// Stats is a snapshot of the log's counters.
type Stats struct {
	Appends             uint64
	AppendedBytes       uint64
	Fsyncs              uint64
	FsyncTime           time.Duration
	TruncatedOnRecovery int64 // bytes dropped from a torn or corrupt tail when the log was opened
	Segments            int
	SizeBytes           int64
}

// Open opens the log in dir, creating it if needed, and recovers the active segment.
func Open(dir string, cfg Config) (*Log, error) {
	cfg = cfg.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	bases, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	l := &Log{dir: dir, cfg: cfg, durCh: make(chan struct{}), flushCh: make(chan struct{}, 1), closing: make(chan struct{})}

	for i, base := range bases {
		last := i == len(bases)-1
		s, truncated, err := openSegment(dir, base, cfg.IndexIntervalBytes, last)
		if err != nil {
			l.closeSegments()
			return nil, fmt.Errorf("open segment %d: %w", base, err)
		}
		if truncated > 0 {
			l.stats.truncatedOnRecovery.Add(truncated)
			cfg.Logger.Warn("truncated a torn or corrupt tail", "dir", dir, "segment", base, "bytes", truncated)
		}
		if i > 0 {
			l.segments[i-1].next = base
		}
		l.segments = append(l.segments, s)
	}
	if len(l.segments) == 0 {
		s, err := createSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, s)
		syncDir(dir)
	}
	a := l.active()
	l.leo = Position{Offset: a.next, Segment: a.base, Pos: a.size}
	l.durable = l.leo // whatever survived recovery is on disk

	switch cfg.Fsync {
	case FsyncGroup:
		l.wg.Add(1)
		go l.flushLoop()
	case FsyncInterval:
		l.wg.Add(1)
		go l.intervalLoop()
	}
	return l, nil
}

func listSegments(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

func (l *Log) active() *segment { return l.segments[len(l.segments)-1] }

// AppendResult describes an appended batch.
type AppendResult struct {
	Base int64    // first offset of the batch
	End  Position // the new log end offset
}

// Append assigns the next offsets to b (in place) and appends it. Under FsyncAlways it returns after the
// data is synced; otherwise call WaitAcked to wait for the policy's acknowledgement.
func (l *Log) Append(b record.Batch) (AppendResult, error) { return l.append(b, true) }

// AppendReplica appends a batch whose offsets were assigned by the leader. It must start exactly at the
// log end offset.
func (l *Log) AppendReplica(b record.Batch) (AppendResult, error) { return l.append(b, false) }

func (l *Log) append(b record.Batch, assign bool) (AppendResult, error) {
	if int64(len(b)) > l.cfg.SegmentBytes {
		return AppendResult{}, ErrBatchTooLarge
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return AppendResult{}, ErrClosed
	}
	a := l.active()
	if a.size > 0 && a.size+int64(len(b)) > l.cfg.SegmentBytes {
		if err := l.roll(); err != nil {
			l.mu.Unlock()
			return AppendResult{}, fmt.Errorf("roll segment: %w", err)
		}
		a = l.active()
	}
	base := l.leo.Offset
	if assign {
		b.SetBaseOffset(base)
	} else if b.BaseOffset() != base {
		l.mu.Unlock()
		return AppendResult{}, fmt.Errorf("%w: batch at %d, log end at %d", ErrOffsetMismatch, b.BaseOffset(), base)
	}
	pos := a.size
	if _, err := a.log.WriteAt(b, pos); err != nil {
		l.mu.Unlock()
		return AppendResult{}, err
	}
	n := len(a.index)
	a.maybeIndex(base, pos, len(b), l.cfg.IndexIntervalBytes)
	if err := a.appendIndexEntry(n); err != nil {
		a.index = a.index[:n]
		l.mu.Unlock()
		return AppendResult{}, err
	}
	a.size += int64(len(b))
	a.next = b.NextOffset()
	a.lastAppend = time.Now()
	l.leo = Position{Offset: a.next, Segment: a.base, Pos: a.size}
	end := l.leo
	l.stats.appends.Add(1)
	l.stats.appendedBytes.Add(uint64(len(b)))

	if l.cfg.Fsync == FsyncAlways {
		err := l.sync(a)
		if err == nil {
			l.setDurable(end)
		}
		l.mu.Unlock()
		l.changed()
		return AppendResult{Base: base, End: end}, err
	}
	l.mu.Unlock()
	if l.cfg.Fsync == FsyncGroup {
		select {
		case l.flushCh <- struct{}{}:
		default: // a flush is already pending and will cover this append
		}
	}
	l.changed()
	return AppendResult{Base: base, End: end}, nil
}

// roll syncs and closes off the active segment and starts a new one at the log end offset. Caller holds mu.
func (l *Log) roll() error {
	a := l.active()
	if err := l.sync(a); err != nil {
		return err
	}
	if err := syncData(a.idx); err != nil {
		return err
	}
	s, err := createSegment(l.dir, l.leo.Offset)
	if err != nil {
		return err
	}
	syncDir(l.dir)
	l.segments = append(l.segments, s)
	l.leo = Position{Offset: s.base, Segment: s.base, Pos: 0}
	l.setDurable(l.leo) // everything before the new segment was just synced
	return nil
}

func (l *Log) sync(s *segment) error {
	start := time.Now()
	err := syncData(s.log)
	l.stats.fsyncs.Add(1)
	l.stats.fsyncNanos.Add(uint64(time.Since(start)))
	return err
}

// flushLoop implements group commit: each wake-up syncs everything written so far, once.
func (l *Log) flushLoop() {
	defer l.wg.Done()
	for {
		select {
		case <-l.flushCh:
		case <-l.closing:
			return
		}
		if d := l.cfg.GroupCommitDelay; d > 0 {
			select {
			case <-time.After(d):
			case <-l.closing:
				return
			}
		}
		l.syncToEnd()
	}
}

func (l *Log) intervalLoop() {
	defer l.wg.Done()
	t := time.NewTicker(l.cfg.FsyncInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			l.syncToEnd()
		case <-l.closing:
			return
		}
	}
}

// syncToEnd syncs the active segment and marks everything written before the sync started as durable.
func (l *Log) syncToEnd() {
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return
	}
	target := l.leo
	a := l.active()
	a.acquire()
	l.mu.RUnlock()
	defer a.release()

	if target.Offset <= l.Durable().Offset {
		return
	}
	if err := l.sync(a); err != nil {
		l.cfg.Logger.Error("fsync failed", "dir", l.dir, "err", err)
		return
	}
	l.setDurable(target)
	l.changed()
}

func (l *Log) setDurable(p Position) {
	l.durMu.Lock()
	if p.Offset > l.durable.Offset {
		l.durable = p
		close(l.durCh)
		l.durCh = make(chan struct{})
	}
	l.durMu.Unlock()
}

func (l *Log) changed() {
	if l.cfg.OnChange != nil {
		l.cfg.OnChange()
	}
}

// Durable is the position up to which data is on stable storage.
func (l *Log) Durable() Position {
	l.durMu.Lock()
	defer l.durMu.Unlock()
	return l.durable
}

// LEO is the log end offset.
func (l *Log) LEO() Position {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leo
}

// Acked is the acknowledged log end offset: the durable position under FsyncAlways and FsyncGroup, the
// written one otherwise. Replication and the high watermark are computed from it.
func (l *Log) Acked() Position {
	if l.cfg.Fsync.Durable() {
		return l.Durable()
	}
	return l.LEO()
}

// LogStart is the first offset still in the log.
func (l *Log) LogStart() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].base
}

// Policy returns the fsync policy.
func (l *Log) Policy() FsyncPolicy { return l.cfg.Fsync }

// WaitAcked blocks until offsets before end are acknowledged under the log's policy, the channel done is
// closed, or the log closes.
func (l *Log) WaitAcked(done <-chan struct{}, end int64) error {
	if !l.cfg.Fsync.Durable() {
		return nil
	}
	for {
		l.durMu.Lock()
		d, ch := l.durable.Offset, l.durCh
		l.durMu.Unlock()
		if d >= end {
			return nil
		}
		select {
		case <-ch:
		case <-done:
			return fmt.Errorf("logstore: wait for durability: %w", ErrClosed)
		case <-l.closing:
			return ErrClosed
		}
	}
}

// ReadResult is a byte range of one segment holding whole batches, except that it may end in a partial
// batch when it was cut at maxBytes. Release it when done; until then the file stays readable even if
// retention deletes the segment.
type ReadResult struct {
	seg   *segment
	Start Position // the first batch: its base offset and position
	Len   int64
}

// File is the segment file; read it at [Start.Pos, Start.Pos+Len) (e.g. with sendfile).
func (r *ReadResult) File() *os.File { return r.seg.log }

// ReadAll copies the range into dst (grown as needed) and returns it.
func (r *ReadResult) ReadAll(dst []byte) ([]byte, error) {
	if int64(cap(dst)) < r.Len {
		dst = make([]byte, r.Len)
	}
	dst = dst[:r.Len]
	if _, err := r.seg.log.ReadAt(dst, r.Start.Pos); err != nil && err != io.EOF {
		return nil, err
	}
	return dst, nil
}

// Release drops the read's reference to the segment.
func (r *ReadResult) Release() {
	if r != nil && r.seg != nil {
		r.seg.release()
		r.seg = nil
	}
}

// Read returns up to maxBytes of batches starting with the batch that contains offset, stopping at limit
// (the high watermark for consumers, the log end offset for followers). It returns (nil, nil) when there
// is nothing to read yet. The first batch is returned whole even if it is larger than maxBytes.
func (l *Log) Read(offset int64, maxBytes int, limit Position) (*ReadResult, error) {
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return nil, ErrClosed
	}
	if offset < l.segments[0].base || offset > l.leo.Offset {
		start, end := l.segments[0].base, l.leo.Offset
		l.mu.RUnlock()
		return nil, fmt.Errorf("%w: %d not in [%d, %d]", ErrOffsetOutOfRange, offset, start, end)
	}
	if offset >= limit.Offset || offset >= l.leo.Offset {
		l.mu.RUnlock()
		return nil, nil
	}
	i := sort.Search(len(l.segments), func(i int) bool { return l.segments[i].base > offset }) - 1
	s := l.segments[i]
	end := s.size
	if limit.Segment == s.base && limit.Pos < end {
		end = limit.Pos
	}
	from := s.lookup(offset)
	s.acquire()
	l.mu.RUnlock()

	pos, base, found, err := s.findBatch(offset, from, end)
	if err != nil || !found {
		s.release()
		return nil, err
	}
	n := end - pos
	if n > int64(maxBytes) {
		var hdr [record.PeekSize]byte
		if _, err := s.log.ReadAt(hdr[:], pos); err != nil {
			s.release()
			return nil, err
		}
		_, first, _ := record.PeekHeader(hdr[:])
		n = max(int64(maxBytes), int64(first))
	}
	return &ReadResult{seg: s, Start: Position{Offset: base, Segment: s.base, Pos: pos}, Len: n}, nil
}

// PositionOf resolves offset, which must be a batch boundary or the log end offset, to a Position.
func (l *Log) PositionOf(offset int64) (Position, error) {
	if leo := l.LEO(); offset == leo.Offset {
		return leo, nil
	}
	r, err := l.Read(offset, 1, Position{Offset: offset + 1, Segment: -1})
	if err != nil {
		return Position{}, err
	}
	if r == nil {
		return Position{}, fmt.Errorf("%w: %d", ErrOffsetOutOfRange, offset)
	}
	defer r.Release()
	if r.Start.Offset != offset {
		return Position{}, fmt.Errorf("logstore: offset %d is inside the batch at %d", offset, r.Start.Offset)
	}
	return r.Start, nil
}

// TruncateTo removes everything from offset on. If offset falls inside a batch, that whole batch goes.
// A follower uses it when its log is ahead of its leader's.
func (l *Log) TruncateTo(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if offset >= l.leo.Offset {
		return nil
	}
	if offset <= l.segments[0].base {
		for _, s := range l.segments {
			s.remove()
		}
		s, err := createSegment(l.dir, offset)
		if err != nil {
			return err
		}
		l.segments = []*segment{s}
		l.leo = Position{Offset: offset, Segment: offset}
		l.resetDurable(l.leo)
		syncDir(l.dir)
		return nil
	}
	i := sort.Search(len(l.segments), func(i int) bool { return l.segments[i].base > offset }) - 1
	for _, s := range l.segments[i+1:] {
		s.remove()
	}
	l.segments = l.segments[:i+1]
	s := l.segments[i]
	pos, base, found, err := s.findBatch(offset, s.lookup(offset), s.size)
	if err != nil {
		return err
	}
	if !found { // offset is past the segment's data: keep it all
		pos, base = s.size, s.next
	}
	if err := s.log.Truncate(pos); err != nil {
		return err
	}
	keep := sort.Search(len(s.index), func(j int) bool { return int64(s.index[j].pos) >= pos })
	s.index = s.index[:keep]
	s.sinceIndex = l.cfg.IndexIntervalBytes // index the next append
	if err := s.writeIndex(); err != nil {
		return err
	}
	s.size, s.next = pos, base
	l.leo = Position{Offset: base, Segment: s.base, Pos: pos}
	if err := syncData(s.log); err != nil {
		return err
	}
	l.resetDurable(l.leo)
	return nil
}

// resetDurable moves the durable position backwards after a truncation. Caller holds mu.
func (l *Log) resetDurable(p Position) {
	l.durMu.Lock()
	if l.durable.Offset > p.Offset {
		l.durable = p
	}
	l.durMu.Unlock()
}

// EnforceRetention deletes the oldest closed segments that fall outside the size or age limit and
// returns how many it deleted.
func (l *Log) EnforceRetention(now time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0
	}
	var total int64
	for _, s := range l.segments {
		total += s.size
	}
	deleted := 0
	for len(l.segments) > 1 {
		s := l.segments[0]
		bySize := l.cfg.RetentionBytes > 0 && total-s.size >= l.cfg.RetentionBytes
		byAge := l.cfg.RetentionAge > 0 && now.Sub(s.lastAppend) > l.cfg.RetentionAge
		if !bySize && !byAge {
			break
		}
		total -= s.size
		s.remove()
		l.segments = l.segments[1:]
		deleted++
	}
	return deleted
}

// Stats returns a snapshot of the counters.
func (l *Log) Stats() Stats {
	st := Stats{
		Appends:             l.stats.appends.Load(),
		AppendedBytes:       l.stats.appendedBytes.Load(),
		Fsyncs:              l.stats.fsyncs.Load(),
		FsyncTime:           time.Duration(l.stats.fsyncNanos.Load()),
		TruncatedOnRecovery: l.stats.truncatedOnRecovery.Load(),
	}
	l.mu.RLock()
	st.Segments = len(l.segments)
	for _, s := range l.segments {
		st.SizeBytes += s.size
	}
	l.mu.RUnlock()
	return st
}

// Dir returns the log's directory.
func (l *Log) Dir() string { return l.dir }

// Close stops background syncing, syncs the active segment and closes the files. Reads in flight keep
// their segments open until released.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.closing)
	l.mu.Unlock()
	l.wg.Wait()

	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.active()
	err := syncData(a.log)
	if err == nil {
		err = syncData(a.idx)
	}
	l.setDurable(l.leo)
	l.closeSegments()
	return err
}

func (l *Log) closeSegments() {
	for _, s := range l.segments {
		s.release()
	}
}

// Remove closes the log and deletes its directory.
func (l *Log) Remove() error {
	l.Close()
	return os.RemoveAll(filepath.Clean(l.dir))
}
