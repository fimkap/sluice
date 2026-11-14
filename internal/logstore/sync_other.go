//go:build !linux

package logstore

import "os"

// syncData forces a file's data to stable storage. On macOS, File.Sync issues F_FULLFSYNC, which also
// flushes the drive's write cache; plain fsync(2) there does not.
func syncData(f *os.File) error { return f.Sync() }
