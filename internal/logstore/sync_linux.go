//go:build linux

package logstore

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncData forces a file's data (and the metadata needed to read it back, such as its size) to stable
// storage. fdatasync skips unrelated metadata such as the modification time.
func syncData(f *os.File) error {
	for {
		err := unix.Fdatasync(int(f.Fd()))
		if err != unix.EINTR {
			return err
		}
	}
}
