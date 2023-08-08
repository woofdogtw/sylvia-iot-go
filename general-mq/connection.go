package gmq

// The event handler for connections.
type ConnectionHandler interface {
	// Triggered by `ConnectionStatus`.
	OnStatus(handlerID string, conn GmqConnection, status Status)

	// Triggered when there are errors.
	OnError(handlerID string, conn GmqConnection, err error)
}

// The operations for connections.
type GmqConnection interface {
	// To get the connection status.
	Status() Status

	// To add a connection event handler. This will return an identifier for applications to manage
	// handlers.
	AddHandler(handler ConnectionHandler) string

	// To remove a handler with an idenfier from `AddHandler`.
	RemoveHandler(id string)

	// To connect to the message broker. The `GmqConnection` will connect to the broker using
	// a go-routine and report status with `Status`.
	Connect() error

	// To close the connection.
	Close() error
}
