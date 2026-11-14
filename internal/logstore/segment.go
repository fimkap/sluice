package logstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/fimkap/sluice/internal/record"
)

const indexEntrySize = 8

type indexEntry struct {
	rel uint32 // offset - segment base
	pos uint32 // byte position of the batch that starts at that offset
}

// segment is one .log file and its sparse .index. Fields other than refs are guarded by Log.mu.
type segment struct {
	base       int64
	logPath    string
	idxPath    string
	log        *os.File
	idx        *os.File
	size       int64 // bytes of valid log data
	next       int64 // offset after the last batch
	index      []indexEntry
	sinceIndex int // log bytes appended since the last index entry
	lastAppend time.Time

	refs atomic.Int32 // the log holds one reference; every in-flight read holds one more
}

func segmentPaths(dir string, base int64) (string, string) {
	name := fmt.Sprintf("%020d", base)
	return filepath.Join(dir, name+".log"), filepath.Join(dir, name+".index")
}

func createSegment(dir string, base int64) (*segment, error) {
	logPath, idxPath := segmentPaths(dir, base)
	lf, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	xf, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		lf.Close()
		return nil, err
	}
	s := &segment{base: base, logPath: logPath, idxPath: idxPath, log: lf, idx: xf, next: base, lastAppend: time.Now()}
	s.refs.Store(1)
	return s, nil
}

// openSegment opens an existing segment. A closed segment (last=false) is trusted: its index is loaded,
// or rebuilt if it doesn't match the log, and next is set by the caller. The active segment (last=true)
// is scanned batch by batch with CRC checks and truncated at the first bad batch; truncated reports how
// many bytes were dropped.
func openSegment(dir string, base int64, indexInterval int, last bool) (s *segment, truncated int64, err error) {
	logPath, idxPath := segmentPaths(dir, base)
	lf, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, err
	}
	xf, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		lf.Close()
		return nil, 0, err
	}
	st, err := lf.Stat()
	if err != nil {
		lf.Close()
		xf.Close()
		return nil, 0, err
	}
	s = &segment{base: base, logPath: logPath, idxPath: idxPath, log: lf, idx: xf, size: st.Size(), next: base, lastAppend: st.ModTime()}
	s.refs.Store(1)

	if !last {
		if s.loadIndex() == nil {
			return s, 0, nil
		}
		// The index is missing or doesn't match the log: rebuild it from the (trusted) log.
		if _, _, err := s.rebuild(indexInterval, false); err != nil {
			s.closeFiles()
			return nil, 0, err
		}
		return s, 0, s.writeIndex()
	}

	end, next, err := s.rebuild(indexInterval, true)
	if err != nil {
		s.closeFiles()
		return nil, 0, err
	}
	truncated = s.size - end
	if truncated > 0 {
		if err := lf.Truncate(end); err != nil {
			s.closeFiles()
			return nil, 0, err
		}
		if err := syncData(lf); err != nil {
			s.closeFiles()
			return nil, 0, err
		}
	}
	s.size, s.next = end, next
	return s, truncated, s.writeIndex()
}

// loadIndex reads the index file and checks it against the log size.
func (s *segment) loadIndex() error {
	data, err := io.ReadAll(io.NewSectionReader(s.idx, 0, 1<<40))
	if err != nil {
		return err
	}
	if len(data) == 0 && s.size > 0 {
		return fmt.Errorf("empty index for %d log bytes", s.size)
	}
	n := len(data) / indexEntrySize
	entries := make([]indexEntry, 0, n)
	for i := 0; i < n; i++ {
		e := indexEntry{
			rel: binary.BigEndian.Uint32(data[i*indexEntrySize:]),
			pos: binary.BigEndian.Uint32(data[i*indexEntrySize+4:]),
		}
		if int64(e.pos) >= s.size || (i > 0 && (e.rel <= entries[i-1].rel || e.pos <= entries[i-1].pos)) {
			return fmt.Errorf("index entry %d out of order", i)
		}
		entries = append(entries, e)
	}
	if len(entries) > 0 && entries[0].pos != 0 {
		return fmt.Errorf("first index entry at %d", entries[0].pos)
	}
	s.index = entries
	return nil
}

// rebuild scans the log from the start and rebuilds the in-memory index. With verify, every batch must
// pass the CRC check and continue the offset sequence; the scan stops at the first one that doesn't.
// It returns the end of the valid data and the next offset.
func (s *segment) rebuild(indexInterval int, verify bool) (end, next int64, err error) {
	s.index = s.index[:0]
	s.sinceIndex = 0
	next = s.base
	r := bufio.NewReaderSize(io.NewSectionReader(s.log, 0, s.size), 1<<20)
	buf := make([]byte, 0, 64<<10)
	var pos int64
	for pos < s.size {
		if s.size-pos < record.HeaderSize {
			break
		}
		buf = buf[:record.LogOverhead]
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, 0, err
		}
		length := int64(int32(binary.BigEndian.Uint32(buf[8:12])))
		size := record.LogOverhead + length
		if length < record.HeaderSize-record.LogOverhead || length > record.MaxBatchSize || pos+size > s.size {
			break
		}
		if cap(buf) < int(size) {
			buf = append(make([]byte, 0, size), buf...)
		}
		buf = buf[:size]
		if _, err := io.ReadFull(r, buf[record.LogOverhead:]); err != nil {
			return 0, 0, err
		}
		b := record.Batch(buf)
		if verify && (b.ValidateHeader() != nil || b.ComputeCRC() != b.CRC() || b.BaseOffset() != next) {
			break
		}
		if b.ValidateHeader() != nil {
			break
		}
		s.maybeIndex(b.BaseOffset(), pos, int(size), indexInterval)
		next = b.NextOffset()
		pos += size
	}
	return pos, next, nil
}

// maybeIndex records an index entry for a batch at pos if enough bytes passed since the last one.
func (s *segment) maybeIndex(baseOffset, pos int64, size, interval int) {
	if len(s.index) == 0 || s.sinceIndex >= interval {
		s.index = append(s.index, indexEntry{rel: uint32(baseOffset - s.base), pos: uint32(pos)})
		s.sinceIndex = 0
	}
	s.sinceIndex += size
}

// writeIndex replaces the index file with the in-memory index.
func (s *segment) writeIndex() error {
	buf := make([]byte, len(s.index)*indexEntrySize)
	for i, e := range s.index {
		binary.BigEndian.PutUint32(buf[i*indexEntrySize:], e.rel)
		binary.BigEndian.PutUint32(buf[i*indexEntrySize+4:], e.pos)
	}
	if err := s.idx.Truncate(0); err != nil {
		return err
	}
	if _, err := s.idx.WriteAt(buf, 0); err != nil {
		return err
	}
	return nil
}

// appendIndexEntry persists the newest in-memory entry if maybeIndex just added one.
func (s *segment) appendIndexEntry(before int) error {
	if len(s.index) == before {
		return nil
	}
	e := s.index[len(s.index)-1]
	var b [indexEntrySize]byte
	binary.BigEndian.PutUint32(b[:], e.rel)
	binary.BigEndian.PutUint32(b[4:], e.pos)
	_, err := s.idx.WriteAt(b[:], int64(len(s.index)-1)*indexEntrySize)
	return err
}

// lookup returns the position to start scanning from for offset: the last indexed batch at or before it.
func (s *segment) lookup(offset int64) int64 {
	rel := offset - s.base
	i := sort.Search(len(s.index), func(i int) bool { return int64(s.index[i].rel) > rel })
	if i == 0 {
		return 0
	}
	return int64(s.index[i-1].pos)
}

// findBatch scans batch headers from pos up to end for the batch containing offset. It returns the batch's
// position and base offset, or found=false if the range ends first. It only reads headers.
func (s *segment) findBatch(offset, pos, end int64) (batchPos, batchBase int64, found bool, err error) {
	var hdr [record.PeekSize]byte
	for pos+record.PeekSize <= end {
		if _, err := s.log.ReadAt(hdr[:], pos); err != nil {
			return 0, 0, false, err
		}
		base, size, lod := record.PeekHeader(hdr[:])
		if size < record.HeaderSize {
			return 0, 0, false, fmt.Errorf("logstore: bad batch header at %s:%d", s.logPath, pos)
		}
		if base+int64(lod) >= offset {
			return pos, base, true, nil
		}
		pos += int64(size)
	}
	return 0, 0, false, nil
}

func (s *segment) acquire() { s.refs.Add(1) }

func (s *segment) release() {
	if s.refs.Add(-1) == 0 {
		s.closeFiles()
	}
}

func (s *segment) closeFiles() {
	s.log.Close()
	s.idx.Close()
}

// remove unlinks the files and drops the log's reference. Readers that still hold the files keep working
// until they release them; the kernel frees the data then.
func (s *segment) remove() {
	os.Remove(s.logPath)
	os.Remove(s.idxPath)
	s.release()
}
