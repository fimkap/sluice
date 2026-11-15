package broker

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/fimkap/sluice/internal/logstore"
	"github.com/fimkap/sluice/internal/protocol"
)

// outMsg is one response. If records is set, its bytes follow the frame (a fetch response): the writer
// sends them with sendfile(2) when zero-copy is on, and releases them either way.
type outMsg struct {
	frame   []byte
	records *logstore.ReadResult
}

// conn serves one client connection. Requests are decoded and applied in order on the read loop;
// anything that waits (durability, replication, long-polls) completes on its own goroutine, and a single
// writer goroutine sends responses in completion order.
type conn struct {
	b      *Broker
	nc     net.Conn
	out    chan outMsg
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func newConn(b *Broker, nc net.Conn) *conn {
	return &conn{b: b, nc: nc, out: make(chan outMsg, 256), closed: make(chan struct{})}
}

func (c *conn) close() {
	c.once.Do(func() {
		close(c.closed)
		c.nc.Close()
	})
}

func (c *conn) serve() {
	c.wg.Add(1)
	go c.writeLoop()
	c.readLoop()
	c.close()
	c.wg.Wait()
}

func (c *conn) readLoop() {
	br := bufio.NewReaderSize(c.nc, 64<<10)
	log := c.b.cfg.Logger
	for {
		// A fresh buffer per frame: handlers that complete later keep references into it.
		body, err := protocol.ReadFrame(br, nil)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Debug("read failed", "remote", c.nc.RemoteAddr().String(), "err", err)
			}
			return
		}
		h, payload, err := protocol.DecodeRequestHeader(body)
		if err != nil {
			log.Warn("bad request header", "remote", c.nc.RemoteAddr().String())
			return
		}
		req := protocol.NewRequest(h.APIKey)
		if req == nil {
			log.Warn("unknown API", "key", int16(h.APIKey), "remote", c.nc.RemoteAddr().String())
			return
		}
		if err := req.Decode(protocol.NewReader(payload)); err != nil {
			log.Warn("malformed request", "api", h.APIKey.String(), "err", err)
			return
		}
		if m := c.b.metrics.requests[h.APIKey]; m != nil {
			m.Inc()
		}
		c.dispatch(h, req)
	}
}

func (c *conn) dispatch(h protocol.RequestHeader, req protocol.Message) {
	b := c.b
	switch r := req.(type) {
	case *protocol.ProduceRequest:
		b.handleProduce(c, h, r) // appends here, in order; waits on its own goroutine
	case *protocol.FetchRequest:
		c.async(func() { b.handleFetch(c, h, r) })
	case *protocol.ShareFetchRequest:
		c.async(func() { b.handleShareFetch(c, h, r) })
	case *protocol.CreateTopicRequest:
		c.async(func() { c.reply(h, b.handleCreateTopic(r)) })
	case *protocol.MetadataRequest:
		c.reply(h, b.handleMetadata(r))
	case *protocol.ListOffsetsRequest:
		c.reply(h, b.handleListOffsets(r))
	case *protocol.InitProducerIDRequest:
		c.reply(h, &protocol.InitProducerIDResponse{ProducerID: b.nextProducerID()})
	case *protocol.FindCoordinatorRequest:
		c.reply(h, b.handleFindCoordinator(r))
	case *protocol.GroupHeartbeatRequest:
		c.reply(h, b.groups.heartbeat(r))
	case *protocol.OffsetCommitRequest:
		c.reply(h, b.groups.commit(r))
	case *protocol.OffsetFetchRequest:
		c.reply(h, b.groups.fetchOffsets(r))
	case *protocol.ShareAcknowledgeRequest:
		c.reply(h, b.handleShareAcknowledge(r))
	}
}

// async runs a handler that may wait, tracked so serve can wait for it on shutdown.
func (c *conn) async(fn func()) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		fn()
	}()
}

func (c *conn) reply(h protocol.RequestHeader, m protocol.Message) {
	c.send(outMsg{frame: protocol.EncodeResponse(h.CorrelationID, m)})
}

func (c *conn) send(m outMsg) {
	select {
	case c.out <- m:
	case <-c.closed:
		m.records.Release()
	}
}

func (c *conn) writeLoop() {
	defer c.wg.Done()
	bw := bufio.NewWriterSize(c.nc, 64<<10)
	var copyBuf []byte
	fail := func() {
		c.close()
	}
	for {
		select {
		case m := <-c.out:
			if _, err := bw.Write(m.frame); err != nil {
				m.records.Release()
				fail()
				return
			}
			if m.records != nil {
				n, err := c.writeRecords(bw, m.records, &copyBuf)
				m.records.Release()
				c.b.metrics.fetchBytes.Add(uint64(n))
				if err != nil {
					fail()
					return
				}
			}
			if len(c.out) == 0 {
				if err := bw.Flush(); err != nil {
					fail()
					return
				}
			}
		case <-c.closed:
			for {
				select {
				case m := <-c.out:
					m.records.Release()
				default:
					return
				}
			}
		}
	}
}

// writeRecords sends a fetch response's record bytes, with sendfile when zero-copy is on and the
// connection is TCP, otherwise by reading them into a buffer first.
func (c *conn) writeRecords(bw *bufio.Writer, rr *logstore.ReadResult, buf *[]byte) (int64, error) {
	if c.b.cfg.ZeroCopy {
		if tc, ok := c.nc.(*net.TCPConn); ok {
			if err := bw.Flush(); err != nil {
				return 0, err
			}
			n, err := sendFile(tc, rr.File(), rr.Start.Pos, rr.Len)
			c.b.metrics.zeroCopyBytes.Add(uint64(n))
			return n, err
		}
	}
	data, err := rr.ReadAll(*buf)
	if err != nil {
		return 0, err
	}
	*buf = data
	n, err := bw.Write(data)
	return int64(n), err
}
