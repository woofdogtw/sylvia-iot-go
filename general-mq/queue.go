package gmq

import (
	"github.com/dlclark/regexp2"
)

// The event handler for queues.
type QueueHandler interface {
	// Triggered by `ConnectionStatus`.
	OnStatus(queue GmqQueue, status Status)

	// Triggered when there are errors.
	OnError(queue GmqQueue, err error)

	// Triggered for new incoming `Message`s.
	OnMessage(queue GmqQueue, message Message)
}

// The operations for queues.
type GmqQueue interface {
	// To get the queue name.
	Name() string

	// Is the queue a receiver.
	IsRecv() bool

	// To get the connection status.
	Status() Status

	// To set the queue event handler.
	SetHandler(handler QueueHandler)

	// To remove the queue event handler.
	ClearHandler()

	// To connect to the message queue. The `GmqQueue` will connect to the queue using a go-routine
	// report status with `Status`.
	Connect() error

	// To close the connection.
	Close() error

	// To send a message (for senders only).
	SendMsg(payload []byte) error
}

// The operations for incoming messages.
type Message interface {
	// To get the payload.
	Payload() []byte

	// Use this if the message is processed successfully.
	Ack() error

	// To requeue the message and the broker will send the message in the future.
	//
	// `Note`: only AMQP or protocols that support requeuing are effective.
	Nack() error
}

const (
	// The accepted pattern of the queue name.
	queueNamePattern = "^[a-z0-9_-]+([\\.]{1}[a-z0-9_-]+)*$"
)

// To validate the queue name.
func nameValidate(name string) bool {
	regexp, err := regexp2.Compile(queueNamePattern, regexp2.None)
	if err != nil || regexp == nil {
		return false
	}
	match, _ := regexp.MatchString(name)
	return match
}
