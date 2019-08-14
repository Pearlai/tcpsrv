package tcpsrv

import (
	"bufio"
	"crypto/tls"
	"log"
	"net"
	"sync"
)

// Client is a struct wrapper for net.Conn
// Includes the server, the readerWithBwuffer and whether it was created fromPool
type Client struct {
	conn             net.Conn
	Server           *server
	fromPool         bool
	readerWithBuffer *bufio.Reader
}

// server is a TCP Server struct
// Hold the address, config and callback functions for clients
type server struct {
	address                  string // Address to open connection: localhost:9999
	config                   *tls.Config
	onNewClientCallback      func(c *Client)
	onClientConnectionClosed func(c *Client, err error)
	onNewMessage             func(c *Client, bytes []byte)
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

// Reader Buffer size
var readerSize = 1024

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

// OnNewClient Called right after server starts listening new Client
func (s *server) OnNewClient(callback func(c *Client)) {
	s.onNewClientCallback = callback
}

// OnClientConnectionClosed Called right after connection closed
func (s *server) OnClientConnectionClosed(callback func(c *Client, err error)) {
	s.onClientConnectionClosed = callback
}

// OnNewMessage Called when Client receives new message
func (s *server) OnNewMessage(callback func(c *Client, bytes []byte)) {
	s.onNewMessage = callback
}

// Listen starts network server
// Infinite loop, always accepting connections if they exist
// On accept, a new Client is made, and goroutine created to act on each client
func (s *server) Listen() {
	var listener net.Listener
	var err error
	if s.config == nil {
		listener, err = net.Listen("tcp", s.address)
	} else {
		listener, err = tls.Listen("tcp", s.address, s.config)
	}
	if err != nil {
		log.Fatal("Error starting TCP server.")
	}
	defer listener.Close()

	for {
		conn, _ := listener.Accept()
		client := newClient(conn, s) // change to getClient if reusing buffers
		go client.listen()           // goroutine
	}
}

/*
	Linter ceating a warning about exported functions returning unexported types being annoying.
	Solution: interfaces!
	Reference: https://stackoverflow.com/a/46786355/4783213
*/

// NewServer creates new tcp server instance
func NewServer(address string) *server {
	log.Println("Creating server with address", address)
	server := &server{
		address: address,
		config:  nil,
	}

	server.OnNewClient(func(c *Client) {})
	server.OnNewMessage(func(c *Client, bytes []byte) {})
	server.OnClientConnectionClosed(func(c *Client, err error) {})
	return server
}

/*
	Linter ceating a warning about exported functions returning unexported types being annoying.
	Solution: interfaces!
	Reference: https://stackoverflow.com/a/46786355/4783213
*/

// NewServerWithTLS creates new tcp server instance with TLS
func NewServerWithTLS(address string, certFile string, keyFile string) *server {
	log.Println("Creating server with address", address)
	cert, _ := tls.LoadX509KeyPair(certFile, keyFile)
	config := tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	server := &server{
		address: address,
		config:  &config,
	}

	server.OnNewClient(func(c *Client) {})
	server.OnNewMessage(func(c *Client, bytes []byte) {})
	server.OnClientConnectionClosed(func(c *Client, err error) {})

	return server
}
