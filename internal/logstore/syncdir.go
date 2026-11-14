package logstore

import "os"

// syncDir makes a newly created segment file's directory entry durable. Best effort: some platforms
// refuse to sync a directory, and the next segment roll retries anyway.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}
