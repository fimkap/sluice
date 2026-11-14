// Package logstore is the partition log: an append-only sequence of record batches split into segment
// files, with a sparse offset index, crash recovery, retention and a choice of fsync policies.
package logstore

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// FsyncPolicy decides when appended data is forced to stable storage, and so when an append counts as
// acknowledged. See docs/design.md, "Durability policies".
type FsyncPolicy uint8

const (
	// FsyncNone never syncs on append; segments are synced when they roll and on Close.
	FsyncNone FsyncPolicy = iota
	// FsyncInterval syncs the active segment every Config.FsyncInterval; appends don't wait for it.
	FsyncInterval
	// FsyncAlways syncs after every append, under the log lock.
	FsyncAlways
	// FsyncGroup lets concurrent appends share one sync: a flusher goroutine syncs everything written so
	// far and releases every waiter it covers.
	FsyncGroup
)

var policyNames = [...]string{"none", "interval", "always", "group"}

func (p FsyncPolicy) String() string {
	if int(p) < len(policyNames) {
		return policyNames[p]
	}
	return fmt.Sprintf("FsyncPolicy(%d)", p)
}

// Durable reports whether an acknowledged append under this policy is on stable storage.
func (p FsyncPolicy) Durable() bool { return p == FsyncAlways || p == FsyncGroup }

// ParseFsyncPolicy parses "none", "interval", "always" or "group".
func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	for i, n := range policyNames {
		if n == s {
			return FsyncPolicy(i), nil
		}
	}
	return 0, fmt.Errorf("unknown fsync policy %q (want none, interval, always or group)", s)
}

// Config tunes a Log. Zero values take the defaults below.
type Config struct {
	SegmentBytes       int64         // roll the active segment at this size (default 64 MiB, max 1 GiB)
	IndexIntervalBytes int           // one index entry per this many log bytes (default 4 KiB)
	Fsync              FsyncPolicy   // default FsyncNone
	FsyncInterval      time.Duration // for FsyncInterval (default 100 ms)
	GroupCommitDelay   time.Duration // FsyncGroup: wait this long before syncing to gather more (default 0)
	RetentionBytes     int64         // keep at least this many bytes; <= 0 keeps everything
	RetentionAge       time.Duration // delete segments whose last append is older; <= 0 keeps everything

	// OnChange is called, without locks held, after the log end offset or the acknowledged offset moves.
	OnChange func()
	Logger   *slog.Logger
}

const maxSegmentBytes = 1 << 30 // positions in the index are uint32

func (c Config) withDefaults() Config {
	if c.SegmentBytes <= 0 {
		c.SegmentBytes = 64 << 20
	}
	if c.SegmentBytes > maxSegmentBytes {
		c.SegmentBytes = maxSegmentBytes
	}
	if c.IndexIntervalBytes <= 0 {
		c.IndexIntervalBytes = 4 << 10
	}
	if c.FsyncInterval <= 0 {
		c.FsyncInterval = 100 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c
}

// Errors.
var (
	ErrOffsetOutOfRange = errors.New("logstore: offset out of range")
	ErrOffsetMismatch   = errors.New("logstore: replica batch does not start at the log end offset")
	ErrClosed           = errors.New("logstore: log closed")
	ErrBatchTooLarge    = errors.New("logstore: batch larger than a segment")
)

// Position locates an offset in the log: the offset, the base offset of the segment that holds it and the
// byte position of its batch in that segment. The log end offset is a Position just past the last batch.
type Position struct {
	Offset  int64
	Segment int64
	Pos     int64
}
