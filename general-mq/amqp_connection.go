package gmq

import (
	"fmt"
	"maps"
	"net/url"
	"sync"
	"time"

	randomString "github.com/delphinus/random-string"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Manages an AMQP connection.
type AmqpConnection struct {
	// Options of the connection.
	opts amqpInnerOptions
	// Connection status.
	status Status
	// Connection status mutex for changing `status`, `conn` and `evChannel`.
	statusMutex sync.RWMutex
	// Hold the connection instance.
	conn *amqp.Connection
	// Event handlers.
	handlers map[string]ConnectionHandler
	// Handler mutex.
	handlersMutex sync.RWMutex
	// The event loop channel.
	evChannel chan Status
}

// The connection options.
type AmqpConnectionOptions struct {
	// Connection URI. Use `amqp|amqps://username:password@host:port/vhost` format.
	//
	// Default is `amqp://localhost/%2f`.
	URI string
	// Connection timeout in milliseconds.
	//
	// Default or zero value is `3000`.
	ConnectTimeoutMS uint64
	// Time in milliseconds from disconnection to reconnection.
	//
	// Default or zero value is `1000`.
	ReconnectMS uint64
}

// The validated options for management.
type amqpInnerOptions struct {
	// The host URI with format `amqp|amqps://username:password@host:port`.
	host string
	// The virtual host.
	vhost string
	// Connection timeout in milliseconds.
	connectTimeout time.Duration
	// Time in milliseconds from disconnection to reconnection.
	reconnect time.Duration
}

var _ GmqConnection = (*AmqpConnection)(nil)

// Create a AMQP connection instance.
func NewAmqpConnection(opts AmqpConnectionOptions) (*AmqpConnection, error) {
	if opts.URI == "" {
		opts.URI = "amqp://localhost"
	}
	uri, err := amqp.ParseURI(opts.URI)
	if err != nil {
		return nil, err
	}
	if opts.ConnectTimeoutMS == 0 {
		opts.ConnectTimeoutMS = 3000
	}
	if opts.ReconnectMS == 0 {
		opts.ReconnectMS = 1000
	}

	return &AmqpConnection{
		opts: amqpInnerOptions{
			host: fmt.Sprintf("%s://%s:%s@%s:%d",
				uri.Scheme, url.QueryEscape(uri.Username), url.QueryEscape(uri.Password),
				uri.Host, uri.Port),
			vhost:          uri.Vhost,
			connectTimeout: (time.Duration(opts.ConnectTimeoutMS) * time.Millisecond),
			reconnect:      (time.Duration(opts.ReconnectMS) * time.Millisecond),
		},
		status:   Closed,
		handlers: map[string]ConnectionHandler{},
	}, nil
}

func (c *AmqpConnection) Status() Status {
	c.statusMutex.RLock()
	defer c.statusMutex.RUnlock()
	return c.status
}

func (c *AmqpConnection) AddHandler(handler ConnectionHandler) string {
	id := randomString.Generate(idSize)
	c.handlersMutex.Lock()
	c.handlers[id] = handler
	c.handlersMutex.Unlock()
	return id
}

func (c *AmqpConnection) RemoveHandler(id string) {
	c.handlersMutex.Lock()
	delete(c.handlers, id)
	c.handlersMutex.Unlock()
}

func (c *AmqpConnection) Connect() error {
	c.statusMutex.Lock()
	if c.evChannel != nil {
		c.statusMutex.Unlock()
		return nil
	}
	c.status = Connecting
	c.evChannel = createAmqpConnectionEventLoop(c)
	c.statusMutex.Unlock()

	c.evChannel <- Connecting
	return nil
}

func (c *AmqpConnection) Close() error {
	var conn *amqp.Connection
	var err error

	c.statusMutex.Lock()
	if c.evChannel == nil {
		c.statusMutex.Unlock()
		return nil
	}
	c.status = Closing
	c.evChannel <- Closing
	c.evChannel = nil
	conn = c.conn
	c.conn = nil
	c.statusMutex.Unlock()

	if conn != nil {
		err = conn.Close()
	}

	c.setStatus(Closed)

	for id, handler := range c.cloneHandlers() {
		go handler.OnStatus(id, c, Closed)
	}

	return err
}

// To get the raw AMQP connection instance for channel declaration.
func (c *AmqpConnection) getRawConnection() *amqp.Connection {
	c.statusMutex.RLock()
	defer c.statusMutex.RUnlock()
	return c.conn
}

// Set status with mutex.
func (c *AmqpConnection) setStatus(status Status) {
	c.statusMutex.Lock()
	c.status = status
	c.statusMutex.Unlock()
}

// Clone handlers under lock for safe iteration.
func (c *AmqpConnection) cloneHandlers() map[string]ConnectionHandler {
	c.handlersMutex.RLock()
	defer c.handlersMutex.RUnlock()

	copied := make(map[string]ConnectionHandler, len(c.handlers))
	maps.Copy(copied, c.handlers)
	return copied
}

func createAmqpConnectionEventLoop(c *AmqpConnection) chan Status {
	ch := make(chan Status, 2)

	go func() {
		var connChannel chan *amqp.Error
		for {
			switch <-ch {
			case Closed, Closing:
				return
			case Connecting:
				config := amqp.Config{
					Vhost: c.opts.vhost,
					Dial:  amqp.DefaultDial(c.opts.connectTimeout),
				}
				conn, err := amqp.DialConfig(c.opts.host, config)
				if err != nil {
					time.Sleep(c.opts.reconnect)
					ch <- Connecting
					continue
				}
				c.statusMutex.Lock()
				if c.status == Closed || c.status == Closing {
					c.statusMutex.Unlock()
					return
				}
				c.status = Connected
				c.conn = conn
				c.statusMutex.Unlock()

				connChannel = conn.NotifyClose(make(chan *amqp.Error, 1))

				for id, handler := range c.cloneHandlers() {
					go handler.OnStatus(id, c, Connected)
				}
				ch <- Connected
			case Connected:
				<-connChannel
				c.statusMutex.Lock()
				if c.status == Closed || c.status == Closing {
					c.statusMutex.Unlock()
					return
				}
				c.status = Connecting
				conn := c.conn
				c.conn = nil
				c.statusMutex.Unlock()

				for id, handler := range c.cloneHandlers() {
					go handler.OnStatus(id, c, Connecting)
				}
				if conn != nil {
					_ = conn.Close()
				}
				ch <- Connecting
			case Disconnected:
				c.statusMutex.Lock()
				if c.status == Closed || c.status == Closing {
					c.statusMutex.Unlock()
					return
				}
				c.status = Connecting
				c.statusMutex.Unlock()

				for id, handler := range c.cloneHandlers() {
					go handler.OnStatus(id, c, Connecting)
				}
				ch <- Connecting
			}
		}
	}()

	return ch
}
