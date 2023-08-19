package gmq

// Error type of general-mq.
type GmqError int

// Status of connections and queues.
type Status int

// general-mq error.
const (
	// The queue does not have a message handler.
	NoMsgHandler GmqError = iota
	// The connection is not connected or the queue (topic) is not connected (declared/subscribed).
	NotConnected
	// The queue is a receiver that cannot send messages.
	QueueIsReceiver
)

// Status definitions for connections and queues.
const (
	// The connection/queue is closing.
	Closing Status = iota
	// The connection/queue is closed by the program.
	Closed
	// Connecting to the message broker or the queue.
	Connecting
	// Connected to the message broker or queue.
	Connected
	// The connection/ is not connected. It will retry connecting to the broker or queue
	// automatically.
	Disconnected
)

// Common constants.
const (
	// Identifier length of inner handlers.
	idSize = 24
)

func (e GmqError) String() string {
	switch e {
	case NoMsgHandler:
		return "no message handler"
	case NotConnected:
		return "not connected"
	case QueueIsReceiver:
		return "this queue is a receiver"
	}
	return "unknown error"
}

func (s Status) String() string {
	switch s {
	case Closing:
		return "closing"
	case Closed:
		return "closed"
	case Connecting:
		return "connecting"
	case Connected:
		return "connected"
	case Disconnected:
		return "disconnected"
	}
	return "unknown"
}
