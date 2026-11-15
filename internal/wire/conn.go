// Package wire is a pipelined client connection: many goroutines can have requests in flight on one
// TCP connection, and responses are matched by correlation id. The broker uses it to talk to its peers,
// and the client package builds on it.
package wire

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fimkap/sluice/internal/protocol"
)

// ErrConnClosed is returned for requests on a closed connection.
var ErrConnClosed = errors.New("wire: connection closed")

// Conn is a pipelined protocol connection. It is safe for concurrent use.
type Conn struct {
	nc       net.Conn
	clientID string
	out      chan []byte
	nextID   atomic.Int32

	mu      sync.Mutex
	pending map[int32]chan []byte
	err     error
	closed  chan struct{}
	once    sync.Once
}

// Dial connects to addr.
func Dial(ctx context.Context, addr, clientID string) (*Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	return New(nc, clientID), nil
}

// New wraps an established connection.
func New(nc net.Conn, clientID string) *Conn {
	c := &Conn{nc: nc, clientID: clientID, out: make(chan []byte, 512), pending: map[int32]chan []byte{}, closed: make(chan struct{})}
	go c.writeLoop()
	go c.readLoop()
	return c
}

// Addr is the remote address.
func (c *Conn) Addr() string { return c.nc.RemoteAddr().String() }

// Done is closed when the connection fails or is closed.
func (c *Conn) Done() <-chan struct{} { return c.closed }

// Err returns why the connection closed, or nil while it is open.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close closes the connection and fails every pending request.
func (c *Conn) Close() error {
	c.fail(ErrConnClosed)
	return nil
}

func (c *Conn) fail(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		pending := c.pending
		c.pending = map[int32]chan []byte{}
		c.mu.Unlock()
		close(c.closed)
		c.nc.Close()
		for _, ch := range pending {
			close(ch)
		}
	})
}

func (c *Conn) writeLoop() {
	bw := bufio.NewWriterSize(c.nc, 64<<10)
	for {
		select {
		case frame := <-c.out:
			if _, err := bw.Write(frame); err != nil {
				c.fail(err)
				return
			}
			// Coalesce: flush only when no more frames are queued.
			if len(c.out) == 0 {
				if err := bw.Flush(); err != nil {
					c.fail(err)
					return
				}
			}
		case <-c.closed:
			return
		}
	}
}

func (c *Conn) readLoop() {
	br := bufio.NewReaderSize(c.nc, 64<<10)
	for {
		body, err := protocol.ReadFrame(br, nil)
		if err != nil {
			c.fail(err)
			return
		}
		id, payload, err := protocol.DecodeResponse(body)
		if err != nil {
			c.fail(err)
			return
		}
		c.mu.Lock()
		ch, ok := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if ok {
			ch <- payload // buffered, never blocks
		}
	}
}

// Do sends req and decodes the response into resp. It returns ctx's error if ctx ends first; a late
// response is then discarded.
func (c *Conn) Do(ctx context.Context, key protocol.APIKey, req, resp protocol.Message) error {
	id := c.nextID.Add(1)
	ch := make(chan []byte, 1)
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrConnClosed, err)
	}
	c.pending[id] = ch
	c.mu.Unlock()

	frame := protocol.EncodeRequest(protocol.RequestHeader{APIKey: key, CorrelationID: id, ClientID: c.clientID}, req)
	select {
	case c.out <- frame:
	case <-c.closed:
		return ErrConnClosed
	case <-ctx.Done():
		c.forget(id)
		return ctx.Err()
	}
	select {
	case payload, ok := <-ch:
		if !ok {
			return fmt.Errorf("%w: %v", ErrConnClosed, c.Err())
		}
		return resp.Decode(protocol.NewReader(payload))
	case <-ctx.Done():
		c.forget(id)
		return ctx.Err()
	}
}

// Send writes a request that gets no response (Produce with acks=0).
func (c *Conn) Send(ctx context.Context, key protocol.APIKey, req protocol.Message) error {
	frame := protocol.EncodeRequest(protocol.RequestHeader{APIKey: key, CorrelationID: c.nextID.Add(1), ClientID: c.clientID}, req)
	select {
	case c.out <- frame:
		return nil
	case <-c.closed:
		return ErrConnClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Conn) forget(id int32) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}
