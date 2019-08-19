package tcpsrv

import "sync/atomic"

var (
	atIncrement = uint32(1)
	atDecrement = ^uint32(0)
	atZero      = uint32(0)
)

//trying to get files in repo...

// metrics is the current metric status of the server, connections, messages etc
type metrics struct {
	clientConnections uint32
	messagesRead      uint32
	messagesProduced  uint32
	messagesDropped   uint32
}

// need getters than load atomically
// need setters that swap atomically
// need atomical additional functions

// Reset will set Message metrics back to 0 but leave clientConnections metrics alone
func (m *metrics) Reset() {
	m.SwapMessagesRead(atZero)
	m.SwapMessagesProduced(atZero)
	m.SwapMessagesDropped(atZero)
}

// ClientConnections gets the current number of connected clients
func (m *metrics) ClientConnections() uint32 {
	return atomic.LoadUint32(&m.clientConnections)
}

// MessagesRead gets the current number of messages read since the last log loop
func (m *metrics) MessagesRead() uint32 {
	return atomic.LoadUint32(&m.messagesRead)
}

// MessagesProduced gets the current number of messages produced to the kafka server since the last log loop
func (m *metrics) MessagesProduced() uint32 {
	return atomic.LoadUint32(&m.messagesProduced)
}

// MessagesDropped gets the current number of messages dropped by the queue since the last log loop
func (m *metrics) MessagesDropped() uint32 {
	return atomic.LoadUint32(&m.messagesDropped)
}

// SwapClientConnection sets the current number of connected clients and returns the previous value
func (m *metrics) SwapClientConnection(new uint32) uint32 {
	return atomic.SwapUint32(&m.clientConnections, new)
}

// SwapMessagesRead sets the current number of messages read and returns the previous value since the last log loop
func (m *metrics) SwapMessagesRead(new uint32) uint32 {
	return atomic.SwapUint32(&m.messagesRead, new)
}

// SwapMessagesProduced sets the current number of messages produced to the kafka server and returns the previous value since the last log loop
func (m *metrics) SwapMessagesProduced(new uint32) uint32 {
	return atomic.SwapUint32(&m.messagesProduced, new)
}

// SwapMessagesDropped sets the current number of messages dropped by the queue and returns the previous value since the last log loop
func (m *metrics) SwapMessagesDropped(new uint32) uint32 {
	return atomic.SwapUint32(&m.messagesDropped, new)
}

// AddClientConnection sets the current number of connected clients and returns the previous value
func (m *metrics) AddClientConnection(u uint32) uint32 {
	return atomic.AddUint32(&m.clientConnections, u)
}

// AddMessagesRead sets the current number of messages read and returns the previous value since the last log loop
func (m *metrics) AddMessagesRead(u uint32) uint32 {
	return atomic.AddUint32(&m.messagesRead, u)
}

// AddMessagesProduced sets the current number of messages produced to the kafka server and returns the previous value since the last log loop
func (m *metrics) AddMessagesProduced(u uint32) uint32 {
	return atomic.AddUint32(&m.messagesProduced, u)
}

// AddMessagesDropped sets the current number of messages dropped by the queue and returns the previous value since the last log loop
func (m *metrics) AddMessagesDropped(u uint32) uint32 {
	return atomic.AddUint32(&m.messagesDropped, u)
}
