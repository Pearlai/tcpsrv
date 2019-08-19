package tcpsrv

import (
	"bufio"
	"net"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

//trying to get files in repo...

// Reader Buffer size
var readerSize = 1024

// Client is a struct wrapper for net.Conn
// Includes the server, the readerWithBwuffer and whether it was created fromPool
type Client struct {
	conn             net.Conn
	Server           *server
	fromPool         bool
	readerWithBuffer *bufio.Reader
	MessageQueue     chan kafka.Message
}

// A Client Pool to reuse clients and their buffers.
// Will create a new Client with a nil conn if one is not found
// Client is *reset* after, giving a conn
var clientPool = sync.Pool{
	New: func() interface{} {
		c := newClient(nil, nil)
		c.fromPool = true
		return c
	},
}

// Create a new Client from a connection.
// Creates a new reader for the client, with a size of var readerSize
func newClient(c net.Conn, s *server) *Client {
	var client = &Client{
		conn:             c,
		Server:           s,
		readerWithBuffer: bufio.NewReaderSize(c, readerSize),
	}
	return client
}

// getClient retrieves a client from, the clientPool, resets the client and returns it for reuse
// A client from the pool could be a nil conn, hence why it needs reset. Reset also discards its buffer and prepares for next read
func getClient(c net.Conn, s *server) *Client {
	client := clientPool.Get().(*Client)
	client.Reset(c, s)
	return client
}

// Read Client data from channel
func (c *Client) listen() {
	c.Server.onNewClientCallback(c)

	for {
		select {
		case <-c.Server.context.Done():
			return
		default:
			// message, err := c.readerWithBuffer.ReadString('\n')
			bytes, err := c.readerWithBuffer.ReadBytes('\n')
			// var buf []byte // should probably be outside the loop
			// bytes, err := c.readerWithBuffer.Read(buf) // this one is confusing
			if err != nil {
				c.conn.Close()
				c.Server.onClientConnectionClosed(c, err)
				return
			}
			c.Server.onNewMessage(c, bytes)
		}
	}
}

// KafkaPush writes messages to kafka
func (c *Client) KafkaPush() {
	for {
		select {
		case <-c.Server.context.Done():
			return
		case message := <-c.MessageQueue:
			c.Server.KafkaConn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			_, err := c.Server.KafkaConn.WriteMessages(message)
			if err == nil {
				c.Server.Metrics.AddMessagesProduced(atIncrement)
			} else {
				c.Server.logger.Warn("Failed to produce a message to kafka")
				c.Server.logger.Warn(err)
				c.Server.logger.Debug(err)
			}
		}
	}
}

// Send text message to Client
func (c *Client) Send(message string) error {
	_, err := c.conn.Write([]byte(message))
	return err
}

// SendBytes sends bytes to the Client via an array of bytes
func (c *Client) SendBytes(b []byte) error {
	_, err := c.conn.Write(b)
	return err
}

// Reset takes a client and reuses it with a new connection
// It discards the clients buffer and points the reader to a new connection to read from
func (c *Client) Reset(netConn net.Conn, s *server) {
	c.conn = netConn
	c.Server = s
	c.readerWithBuffer.Discard(c.readerWithBuffer.Buffered())
	c.readerWithBuffer.Reset(netConn)
	// c.SetID("") // Do we need this? If so use a GUID?
	// Not sure on this
	// s.initOptimize()
}

// Conn gets a clients net.Conn connection
func (c *Client) Conn() net.Conn {
	return c.conn
}

// Close signals the client the connection is closed (shutdown), then closes the connection (fd) freeing resources
func (c *Client) Close() error {
	var err error
	if c.conn != nil {
		err = c.conn.Close()
	}
	if c.fromPool {
		c.conn = nil
		clientPool.Put(c)
	}
	return err
}
