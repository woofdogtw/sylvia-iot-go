package gmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Manages an AMQP queue.
type AmqpQueue struct {
	// Options of the queue.
	opts AmqpQueueOptions
	// The associated `AmqpConnection`. Cannot be nil.
	conn *AmqpConnection
	// Hold the channel instance.
	channel *amqp.Channel
	// Queue status.
	status Status
	// Queue status mutex for changing `status`, `channel` and `evChannel`.
	statusMutex sync.Mutex
	// The event handler.
	handler QueueHandler
	// The event loop channel.
	evChannel chan Status
}

// The queue options.
type AmqpQueueOptions struct {
	// The queue name that is used to map a AMQP queue (unicast) or an exchange (broadcast).
	//
	// The pattern is `^[a-z0-9_-]+([\\.]{1}[a-z0-9_-]+)*$`.
	Name string
	// `true` for the receiver and `false` for the sender.
	IsRecv bool
	// Reliable by selecting the confirm channel (for publish).
	Reliable bool
	// `true` for broadcast and `false` for unicast.
	Broadcast bool
	// Time in milliseconds from disconnection to reconnection.
	//
	// Default or zero value is `1000`.
	ReconnectMS uint64
	// The QoS of the receiver queue.
	//
	// `Note`: this value MUST be a positive value.
	Prefetch uint16
}

type amqpMessage struct {
	// Hold the consumer callback channel to operate ack/nack.
	channel *amqp.Channel
	// Hold the consumer callback deliver to operate ack/nack.
	delivery uint64
	// Hold the consumer callback content.
	content []byte
}

var _ GmqQueue = (*AmqpQueue)(nil)
var _ Message = (*amqpMessage)(nil)

// Create a queue instance.
func NewAmqpQueue(opts AmqpQueueOptions, conn *AmqpConnection) (*AmqpQueue, error) {
	if conn == nil {
		return nil, errors.New("give nil AmqpConnection")
	} else if opts.Name == "" {
		return nil, errors.New("queue name cannot be empty")
	} else if !nameValidate(opts.Name) {
		return nil, fmt.Errorf("queue name %s is not match %s", opts.Name, queueNamePattern)
	} else if opts.IsRecv && opts.Prefetch == 0 {
		return nil, fmt.Errorf("prefetch cannot be zero for a receiver")
	}
	if opts.ReconnectMS == 0 {
		opts.ReconnectMS = 1000
	}

	return &AmqpQueue{
		opts:   opts,
		conn:   conn,
		status: Closed,
	}, nil
}

func (q *AmqpQueue) Name() string {
	return q.opts.Name
}

func (q *AmqpQueue) IsRecv() bool {
	return q.opts.IsRecv
}

func (q *AmqpQueue) Status() Status {
	return q.status
}

func (q *AmqpQueue) SetHandler(handler QueueHandler) {
	q.handler = handler
}

func (q *AmqpQueue) ClearHandler() {
	q.handler = nil
}

func (q *AmqpQueue) Connect() error {
	q.statusMutex.Lock()
	if q.evChannel != nil {
		q.statusMutex.Unlock()
		return nil
	}
	q.status = Connecting
	q.evChannel = createAmqpQueueEventLoop(q)
	q.statusMutex.Unlock()

	q.evChannel <- Connecting
	return nil
}

func (q *AmqpQueue) Close() error {
	var channel *amqp.Channel
	var err error

	q.statusMutex.Lock()
	if q.evChannel == nil {
		q.statusMutex.Unlock()
		return nil
	}
	q.status = Closing
	q.evChannel <- Closing
	q.evChannel = nil
	channel = q.channel
	q.channel = nil
	q.statusMutex.Unlock()

	if channel != nil {
		err = channel.Close()
	}

	q.statusMutex.Lock()
	q.status = Closed
	q.statusMutex.Unlock()

	handler := q.handler
	if handler != nil {
		handler.OnStatus(q, Closed)
	}

	return err
}

func (q *AmqpQueue) SendMsg(payload []byte) error {
	if q.opts.IsRecv {
		return fmt.Errorf("%s", QueueIsReceiver)
	}

	channel := q.channel
	if channel == nil {
		return fmt.Errorf("%s", NotConnected)
	}

	var routingKey string
	var exchange string
	if q.opts.Broadcast {
		exchange = q.opts.Name
	} else {
		routingKey = q.opts.Name
	}
	msg := amqp.Publishing{Body: payload}
	if q.opts.Reliable {
		msg.DeliveryMode = 2
	}
	ctx := context.Background()
	return channel.PublishWithContext(ctx, exchange, routingKey, q.opts.Reliable, false, msg)
}

// To get the associated connection status.
func (q *AmqpQueue) connStatus() Status {
	return q.conn.status
}

// The error handling.
func (q *AmqpQueue) onError(err error) {
	handler := q.handler
	if handler != nil {
		go func() {
			handler.OnError(q, err)
		}()
	}
}

// Set message consumer.
func (q *AmqpQueue) setConsumer(deliveryChannel <-chan amqp.Delivery) {
	go func() {
		for delivery := range deliveryChannel {
			channel := q.channel
			if channel == nil {
				return
			}

			handler := q.handler
			if handler != nil {
				msg := &amqpMessage{
					channel:  channel,
					delivery: delivery.DeliveryTag,
					content:  delivery.Body,
				}
				go func() {
					handler.OnMessage(q, msg)
				}()
			}
		}
	}()
}

func (m *amqpMessage) Payload() []byte {
	return m.content
}

func (m *amqpMessage) Ack() error {
	return m.channel.Ack(m.delivery, false)
}

func (m *amqpMessage) Nack() error {
	return m.channel.Nack(m.delivery, false, true)
}

func createAmqpQueueEventLoop(q *AmqpQueue) chan Status {
	ch := make(chan Status, 1)

	go func() {
		var queueChannnel chan *amqp.Error
		for {
			switch <-ch {
			case Closed, Closing:
				return
			case Connecting:
				if q.connStatus() != Connected {
					time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
					ch <- Connecting
					continue
				}

				rawConn := q.conn.getRawConnection()
				if rawConn == nil {
					time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
					ch <- Connecting
					continue
				}
				channel, err := rawConn.Channel()
				if err != nil {
					q.onError(err)
					time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
					ch <- Connecting
					continue
				}
				if q.opts.Reliable {
					if err := channel.Confirm(false); err != nil {
						q.onError(err)
						time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
						ch <- Connecting
						continue
					}
				}

				name := q.opts.Name
				if q.opts.Broadcast {
					err := channel.ExchangeDeclare(name, amqp.ExchangeFanout, false, false, false,
						false, nil)
					if err != nil {
						q.onError(err)
						time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
						ch <- Connecting
						continue
					}

					if q.opts.IsRecv {
						queue, err := channel.QueueDeclare("", false, false, true, false, nil)
						if err != nil {
							q.onError(err)
							time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
							ch <- Connecting
							continue
						}
						err = channel.QueueBind(queue.Name, "", name, false, nil)
						if err != nil {
							q.onError(err)
							time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
							ch <- Connecting
							continue
						}
						err = channel.Qos(int(q.opts.Prefetch), 0, false)
						if err != nil {
							q.onError(err)
							time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
							ch <- Connecting
							continue
						}
						deliveryChannel, err :=
							channel.Consume(queue.Name, "", false, false, false, false, nil)
						if err != nil {
							q.onError(err)
							time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
							ch <- Connecting
							continue
						}
						q.setConsumer(deliveryChannel)
					}
				} else {
					_, err = channel.QueueDeclare(name, true, false, false, false, nil)
					if err != nil {
						q.onError(err)
						time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
						ch <- Connecting
						continue
					}

					if q.opts.IsRecv {
						err = channel.Qos(int(q.opts.Prefetch), 0, false)
						if err != nil {
							q.onError(err)
							time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
							ch <- Connecting
							continue
						}
						deliveryChannel, err :=
							channel.Consume(name, "", false, false, false, false, nil)
						if err != nil {
							q.onError(err)
							time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
							ch <- Connecting
							continue
						}
						q.setConsumer(deliveryChannel)
					}
				}

				q.statusMutex.Lock()
				if q.status == Closed || q.status == Closing {
					q.statusMutex.Unlock()
					return
				}

				q.channel = channel
				q.status = Connected
				q.statusMutex.Unlock()

				queueChannnel = channel.NotifyClose(make(chan *amqp.Error, 1))

				handler := q.handler
				if handler != nil {
					handler.OnStatus(q, Connected)
				}
				ch <- Connected
			case Connected:
				<-queueChannnel
				var channel *amqp.Channel
				q.statusMutex.Lock()
				q.status = Connecting
				channel = q.channel
				q.channel = nil
				q.statusMutex.Unlock()

				handler := q.handler
				if handler != nil {
					handler.OnStatus(q, Connecting)
				}
				if channel != nil {
					_ = channel.Close()
				}
				ch <- Connecting
			case Disconnected:
				q.statusMutex.Lock()
				q.status = Connecting
				q.statusMutex.Unlock()
				ch <- Connecting
			}
		}
	}()

	return ch
}
