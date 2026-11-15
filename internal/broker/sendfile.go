package broker

import (
	"io"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// sendFile copies n bytes of f starting at off to the socket with sendfile(2): the kernel moves the
// pages from the page cache to the socket without a round trip through user space. The file offset is
// not used, so concurrent fetches can share one *os.File.
func sendFile(tc *net.TCPConn, f *os.File, off, n int64) (int64, error) {
	rc, err := tc.SyscallConn()
	if err != nil {
		return 0, err
	}
	src := int(f.Fd())
	var written int64
	var serr error
	err = rc.Write(func(fd uintptr) bool {
		for written < n {
			o := off + written
			chunk := int(min(n-written, 1<<30))
			cnt, e := unix.Sendfile(int(fd), src, &o, chunk)
			if cnt > 0 {
				written += int64(cnt)
			}
			switch {
			case e == unix.EAGAIN:
				return false // wait until the socket is writable again
			case e == unix.EINTR:
				continue
			case e != nil:
				serr = e
				return true
			case cnt == 0:
				serr = io.ErrUnexpectedEOF // the file is shorter than expected (truncated)
				return true
			}
		}
		return true
	})
	if serr != nil {
		return written, serr
	}
	return written, err
}
