package gmq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Manages a MQTT queue.
type MqttQueue struct {
	// Options of the queue.
	opts MqttQueueOptions
	// The associated `MqttConnection`. Cannot be nil.
	conn *MqttConnection
	// Queue status.
	status Status
	// Queue status mutex for changing `status` and `evChannel`.
	statusMutex sync.RWMutex
	// The event handler.
	handler QueueEventHandler
	// Handler mutex.
	handlerMutex sync.RWMutex
	// The message handler.
	msgHandler QueueMessageHandler
	// Message handler mutex.
	msgHandlerMutex sync.RWMutex
	// The event loop channel.
	evChannel chan Status
}

// The queue options.
type MqttQueueOptions struct {
	// The queue name that is used to map a MQTT topic.
	//
	// The pattern is `^[a-z0-9_-]+([\\.]{1}[a-z0-9_-]+)*$`.
	Name string
	// `true` for the receiver and `false` for the sender.
	IsRecv bool
	// Reliable by selecting the confirm channel (for publish).
	Reliable bool
	// `true` for broadcast and `false` for unicast.
	//
	// `Note`: the unicast queue relies on `shared queue`. See the `SharedPrefix` option.
	Broadcast bool
	// Time in milliseconds from disconnection to reconnection.
	//
	// Default or zero value is `1000`.
	ReconnectMS uint64
	// Used for `broadcast=false`.
	SharedPrefix string
}

type mqttQueuePacketHandler struct {
	queue *MqttQueue
}

type mqttMessage struct {
	rawMessage mqtt.Message
}

var _ GmqQueue = (*MqttQueue)(nil)
var _ Message = (*mqttMessage)(nil)
var _ mqttPacketHandler = (*mqttQueuePacketHandler)(nil)

// Create a queue instance.
func NewMqttQueue(opts MqttQueueOptions, conn *MqttConnection) (*MqttQueue, error) {
	if conn == nil {
		return nil, errors.New("give nil MqttConnection")
	} else if opts.Name == "" {
		return nil, errors.New("queue name cannot be empty")
	} else if !nameValidate(opts.Name) {
		return nil, fmt.Errorf("queue name %s is not match %s", opts.Name, queueNamePattern)
	}
	if opts.ReconnectMS == 0 {
		opts.ReconnectMS = 1000
	}

	return &MqttQueue{
		opts:   opts,
		conn:   conn,
		status: Closed,
	}, nil
}

func (q *MqttQueue) Name() string {
	return q.opts.Name
}

func (q *MqttQueue) IsRecv() bool {
	return q.opts.IsRecv
}

func (q *MqttQueue) Status() Status {
	q.statusMutex.RLock()
	defer q.statusMutex.RUnlock()
	return q.status
}

func (q *MqttQueue) SetHandler(handler QueueEventHandler) {
	q.handlerMutex.Lock()
	q.handler = handler
	q.handlerMutex.Unlock()
}

func (q *MqttQueue) SetMsgHandler(handler QueueMessageHandler) error {
	if handler == nil {
		return errors.New("cannot use nil message handler")
	}
	q.msgHandlerMutex.Lock()
	q.msgHandler = handler
	q.msgHandlerMutex.Unlock()
	return nil
}

func (q *MqttQueue) Connect() error {
	if q.opts.IsRecv && q.getMsgHandler() == nil {
		return fmt.Errorf("%s", NoMsgHandler)
	}

	q.statusMutex.Lock()
	if q.evChannel != nil {
		q.statusMutex.Unlock()
		return nil
	}
	q.status = Connecting
	q.evChannel = createMqttQueueEventLoop(q)
	q.statusMutex.Unlock()

	q.evChannel <- Connecting
	return nil
}

func (q *MqttQueue) Close() error {
	var err error

	q.statusMutex.Lock()
	if q.evChannel == nil {
		q.statusMutex.Unlock()
		return nil
	}
	q.status = Closing
	q.evChannel <- Closing
	q.evChannel = nil
	q.statusMutex.Unlock()

	q.conn.removePacketHandler(q.opts.Name)

	q.setStatus(Closed)

	handler := q.getHandler()
	if handler != nil {
		go handler.OnStatus(q, Closed)
	}

	return err
}

func (q *MqttQueue) SendMsg(payload []byte) error {
	if q.opts.IsRecv {
		return fmt.Errorf("%s", QueueIsReceiver)
	} else if q.Status() != Connected {
		return fmt.Errorf("%s", NotConnected)
	}

	conn := q.conn.getRawConnection()
	if conn == nil {
		return fmt.Errorf("%s", NotConnected)
	}
	token := conn.Publish(q.opts.Name, q.qos(), false, payload)
	if !token.WaitTimeout(time.Duration(q.opts.ReconnectMS) * time.Millisecond) {
		return errors.New("publish queue timeout")
	}
	return token.Error()
}

// To get the associated connection status.
func (q *MqttQueue) connStatus() Status {
	return q.conn.Status()
}

// Set status with mutex.
func (q *MqttQueue) setStatus(status Status) {
	q.statusMutex.Lock()
	q.status = status
	q.statusMutex.Unlock()
}

// Get handler with mutex.
func (q *MqttQueue) getHandler() QueueEventHandler {
	q.handlerMutex.RLock()
	defer q.handlerMutex.RUnlock()
	return q.handler
}

// Get message handler with mutex.
func (q *MqttQueue) getMsgHandler() QueueMessageHandler {
	q.msgHandlerMutex.RLock()
	defer q.msgHandlerMutex.RUnlock()
	return q.msgHandler
}

// To get the associated topic.
func (q *MqttQueue) topic() string {
	if !q.opts.Broadcast {
		return q.opts.SharedPrefix + q.opts.Name
	}
	return q.opts.Name
}

// To get the associated QoS.
func (q *MqttQueue) qos() byte {
	if q.opts.Reliable {
		return 1
	}
	return 0
}

func (m *mqttMessage) Payload() []byte {
	return m.rawMessage.Payload()
}

func (m *mqttMessage) Ack() error {
	m.rawMessage.Ack()
	return nil
}

func (m *mqttMessage) Nack() error {
	m.rawMessage.Ack()
	return nil
}

func (m *mqttQueuePacketHandler) OnPublish(msg mqtt.Message) {
	handler := m.queue.getMsgHandler()
	if handler != nil {
		go handler.OnMessage(m.queue, &mqttMessage{rawMessage: msg})
	}
}

func createMqttQueueEventLoop(q *MqttQueue) chan Status {
	ch := make(chan Status, 2)

	go func() {
		for {
			switch <-ch {
			case Closed, Closing:
				return
			case Connecting:
				q.setStatus(Connecting)

				if q.connStatus() != Connected {
					time.Sleep(time.Duration(q.opts.ReconnectMS) * time.Millisecond)
					ch <- Connecting
					continue
				}

				handler := q.getHandler()
				if handler != nil {
					go handler.OnStatus(q, Connecting)
				}

				if q.opts.IsRecv {
					packetHandler := &mqttQueuePacketHandler{queue: q}
					q.conn.addPacketHandler(q.opts.Name, q.topic(), q.opts.Reliable, packetHandler)
				}

				ch <- Connected
			case Connected:
				q.setStatus(Connected)

				handler := q.getHandler()
				if handler != nil {
					go handler.OnStatus(q, Connected)
				}
			case Disconnected:
				q.statusMutex.Lock()
				if q.status == Closed || q.status == Closing {
					q.statusMutex.Unlock()
					return
				}
				q.status = Disconnected
				q.statusMutex.Unlock()
				ch <- Connecting
			}
		}
	}()

	return ch
}
