package tcpsrv

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// server is a TCP Server struct
// Hold the address, config and callback functions for clients
type server struct {
	address                  string // Address to open connection: localhost:9999
	context                  context.Context
	config                   *tls.Config
	onNewClientCallback      func(c *Client)
	onClientConnectionClosed func(c *Client, err error)
	onNewMessage             func(c *Client, bytes []byte)
	Metrics                  *metrics
	MessageQueue             chan kafka.Message
	logger                   *logrus.Logger
	logInterval              time.Duration
	KafkaConn                *kafka.Conn
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

func (s *server) Logger() *logrus.Logger {
	return s.logger
}

func (s *server) SetLogger(logger *logrus.Logger) {
	s.logger = logger
}

func (s *server) LogInterval() time.Duration {
	return s.logInterval
}

func (s *server) SetLogInterval(interval time.Duration) {
	s.logInterval = interval
}

func (s *server) logRoutine() {
	for {
		select {
		case <-s.context.Done():
			return
		default:
			time.Sleep(s.logInterval)
			s.logger.WithFields(logrus.Fields{
				"CLIENTS_CONNECTED": s.Metrics.ClientConnections(),
				"MESSAGES_READ":     s.Metrics.MessagesRead(),
				"MESSAGES_PRODUCED": s.Metrics.MessagesProduced(),
				"MESSAGES_DROPPED":  s.Metrics.MessagesDropped(),
			}).Info()
			// reset metrics
			s.Metrics.Reset()
		}
	}
}

// KafkaPush writes messages to kafka
func (s *server) KafkaPush() {
	for {
		select {
		case <-s.context.Done():
			return
		case message := <-s.MessageQueue:
			s.KafkaConn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			_, err := s.KafkaConn.WriteMessages(message)
			if err == nil {
				s.Metrics.AddMessagesProduced(atIncrement)
			} else {
				s.logger.Warn("Failed to produce a message to kafka")
				s.logger.Warn(err)
				s.logger.Debug(err)
			}
		}
	}
}

// Listen starts network server
// Infinite loop, always accepting connections if they exist
// On accept, a new Client is made, and goroutine created to act on each client
func (s *server) Listen() {
	var (
		err      error
		listener net.Listener
	)

	// Listen with TLS or not
	if s.config == nil {
		listener, err = net.Listen("tcp", s.address)
	} else {
		listener, err = tls.Listen("tcp", s.address, s.config)
	}
	if err != nil {
		s.logger.Fatal("Error starting TCP server.")
	}
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		s.logger.Fatal("Could not wrap as TCP Listener")
	}

	s.newKafkaConn()
	go s.logRoutine()

	defer func() {
		tcpListener.Close()
		s.KafkaConn.Close()
		s.logger.Info("Server has shutdown. OK, I love you, Buh Bye!")
	}()

	for {
		tcpListener.SetDeadline(time.Now().Add(time.Second * 2))
		select {
		case <-s.context.Done():
			return
		default:
			conn, err := tcpListener.Accept()
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				// Checks for timeout error, we arent worried about accept errors here
				continue
			}
			client := newClient(conn, s) // change to getClient if reusing buffers
			go client.listen()           // goroutine
		}
	}
}

func (s *server) newKafkaConn() (conn *kafka.Conn, err error) {
	topic := "test-raw-A"
	partition := 0
	host := "ec2-52-31-37-1.eu-west-1.compute.amazonaws.com:9092"
	s.KafkaConn, err = kafka.DialLeader(s.context, "tcp", host, topic, partition)
	if err != nil {
		s.logger.Panic(err)
	}
	s.logger.Infof("Created a connection to Kafka Topic %v:%v at %v", topic, partition, host)
	return
}

/*
	Linter ceating a warning about exported functions returning unexported types being annoying.
	Solution: interfaces!
	Reference: https://stackoverflow.com/a/46786355/4783213
*/

// NewServer creates new tcp server instance
func NewServer(ctx context.Context, address string) *server {
	logrus.Info("Creating server with address ", address)

	logInterval, err := strconv.Atoi(GetEnv("LOG_INTERVAL", "5"))
	if err != nil {
		logrus.Panic(err)
	}

	server := &server{
		address:     address,
		config:      nil,
		logger:      logrus.New(),
		logInterval: time.Duration(logInterval) * time.Second,
		Metrics:     &metrics{},
		context:     ctx,
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
func NewServerWithTLS(ctx context.Context, address string, certFile string, keyFile string) *server {
	logrus.Info("Creating server with address", address)
	cert, _ := tls.LoadX509KeyPair(certFile, keyFile)
	config := tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	logInterval, err := strconv.Atoi(GetEnv("LOG_INTERVAL", "5"))
	if err != nil {
		logrus.Panic(err)
	}

	server := &server{
		address:     address,
		config:      &config,
		logger:      logrus.New(),
		logInterval: time.Duration(logInterval) * time.Second,
		Metrics:     &metrics{},
		context:     ctx,
	}

	server.OnNewClient(func(c *Client) {})
	server.OnNewMessage(func(c *Client, bytes []byte) {})
	server.OnClientConnectionClosed(func(c *Client, err error) {})

	return server
}
