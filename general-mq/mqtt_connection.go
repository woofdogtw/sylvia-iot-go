package gmq

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	randomString "github.com/delphinus/random-string"
	"github.com/dlclark/regexp2"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Manages an MQTT connection.
type MqttConnection struct {
	// Options of the connection.
	opts mqttInnerOptions
	// Connection status.
	status Status
	// Connection status mutex for changing `status`, `conn` and `evChannel`.
	statusMutex sync.Mutex
	// Hold the connection instance.
	conn mqtt.Client
	// Event handlers.
	handlers map[string]ConnectionHandler
	// Handler mutex.
	handlersMutex sync.Mutex
	// Publish packet handlers. The key is the queue name.
	//
	// Because MQTT is connection-driven, the receiver `MqttQueue` queues must register a handler to
	// receive `mqtt.Message` packets.
	packetHandlers map[string]mqttPacketHandlerItem
	// Packet handler mutex.
	packetHandlersMutex sync.Mutex
	// The unique MQTT message handler and publish to the associated queue by the topic.
	mqttMessageHandler mqtt.MessageHandler
	// The event loop channel.
	evChannel chan Status
}

// The connection options.
type MqttConnectionOptions struct {
	// Connection URI. Use `mqtt|mqtts://username:password@host:port` format.
	//
	// Default is `mqtt://localhost`.
	URI string
	// Connection timeout in milliseconds.
	//
	// Default or zero value is `3000`.
	ConnectTimeoutMS uint64
	// Time in milliseconds from disconnection to reconnection.
	//
	// Default or zero value is `1000`.
	ReconnectMS uint64
	// Client identifier. Empty to generate a random client identifier.
	ClientID string
	// Do not clean session flag.
	//
	// `Note`: this is not stable.
	NotCleanSession bool
}

// The validated options for management.
type mqttInnerOptions struct {
	// The host URI.
	uri mqttURI
	// Connection timeout in milliseconds.
	connectTimeout time.Duration
	// Time in milliseconds from disconnection to reconnection.
	reconnect time.Duration
	// Client ID.
	clientID string
	// The clean session flag.
	cleanSession bool
}

// Parsed URI information for paho-mqtt.
type mqttURI struct {
	scheme   string
	host     string
	port     int
	username string
	password string
}

// Packet handler definitions.
type mqttPacketHandler interface {
	// For `Publish` packets.
	OnPublish(msg mqtt.Message)
}

type mqttPacketHandlerItem struct {
	handler mqttPacketHandler
	topic   string
	qos     byte
}

// Constants.
const (
	// The accepted pattern of the client identifier.
	mqttClientIDPattern = "^[0-9A-Za-z-]{1,23}$"
)

// Scheme mappings.
var (
	// Scheme to port.
	mqttSchemePorts = map[string]int{
		"mqtt":  1883,
		"mqtts": 8883,
	}

	// general-mq scheme to paho scheme.
	mqttSchemeMaps = map[string]string{
		"mqtt":  "tcp",
		"mqtts": "ssl",
	}
)

var _ GmqConnection = (*MqttConnection)(nil)

// Create a MQTT connection instance.
func NewMqttConnection(opts MqttConnectionOptions) (*MqttConnection, error) {
	if opts.URI == "" {
		opts.URI = "mqtt://localhost"
	}
	uri, err := mqttParseURI(opts.URI)
	if err != nil {
		return nil, err
	}
	if opts.ConnectTimeoutMS == 0 {
		opts.ConnectTimeoutMS = 3000
	}
	if opts.ReconnectMS == 0 {
		opts.ReconnectMS = 1000
	}
	if opts.ClientID != "" {
		if !mqttClientIDValidate(opts.ClientID) {
			return nil,
				fmt.Errorf("client ID %s is not match %s", opts.ClientID, mqttClientIDPattern)
		}
	} else {
		opts.ClientID = fmt.Sprintf("general-mq-%s", randomString.Generate(12))
	}

	conn := &MqttConnection{
		opts: mqttInnerOptions{
			uri:            uri,
			connectTimeout: (time.Duration(opts.ConnectTimeoutMS) * time.Millisecond),
			reconnect:      (time.Duration(opts.ReconnectMS) * time.Millisecond),
			clientID:       opts.ClientID,
			cleanSession:   !opts.NotCleanSession,
		},
		status:         Closed,
		handlers:       map[string]ConnectionHandler{},
		packetHandlers: map[string]mqttPacketHandlerItem{},
	}
	conn.mqttMessageHandler = genMqttMessageHandler(conn)
	return conn, nil
}

func (c *MqttConnection) Status() Status {
	return c.status
}

func (c *MqttConnection) AddHandler(handler ConnectionHandler) string {
	id := randomString.Generate(idSize)
	c.handlersMutex.Lock()
	c.handlers[id] = handler
	c.handlersMutex.Unlock()
	return id
}

func (c *MqttConnection) RemoveHandler(id string) {
	c.handlersMutex.Lock()
	delete(c.handlers, id)
	c.handlersMutex.Unlock()
}

func (c *MqttConnection) Connect() error {
	c.statusMutex.Lock()
	if c.evChannel != nil {
		c.statusMutex.Unlock()
		return nil
	}
	c.status = Disconnected
	c.evChannel = createMqttConnectionEventLoop(c)
	c.statusMutex.Unlock()

	c.evChannel <- Connecting
	return nil
}

func (c *MqttConnection) Close() error {
	var conn mqtt.Client

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
		conn.Disconnect(0)
	}

	c.statusMutex.Lock()
	c.status = Closed
	c.statusMutex.Unlock()

	for id, handler := range c.handlers {
		handler.OnStatus(id, c, Closed)
	}

	return nil
}

// To get the raw MQTT connection instance for topic operations such as subscribe or publish.
func (c *MqttConnection) getRawConnection() mqtt.Client {
	return c.conn
}

// To add a packet handler for `MqttQueue`. The `name` is the queue name.
func (c *MqttConnection) addPacketHandler(name string, topic string, reliable bool,
	handler mqttPacketHandler) {
	var qos byte
	if reliable {
		qos = 1
	}
	c.packetHandlersMutex.Lock()
	c.packetHandlers[name] = mqttPacketHandlerItem{
		handler: handler,
		topic:   topic,
		qos:     qos,
	}
	c.packetHandlersMutex.Unlock()

	token := c.conn.Subscribe(topic, qos, c.mqttMessageHandler)
	_ = token.WaitTimeout(c.opts.reconnect)
}

// To remove a packet handler. The `name` is the queue name.
func (c *MqttConnection) removePacketHandler(name string) {
	c.packetHandlersMutex.Lock()
	item := c.packetHandlers[name]
	delete(c.packetHandlers, name)
	c.packetHandlersMutex.Unlock()

	if item.handler != nil {
		token := c.conn.Unsubscribe(item.topic)
		_ = token.WaitTimeout(c.opts.reconnect)
	}
}

func createMqttConnectionEventLoop(c *MqttConnection) chan Status {
	ch := make(chan Status, 1)

	go func() {
		for {
			switch <-ch {
			case Closed, Closing:
				return
			case Connecting:
				c.statusMutex.Lock()
				c.status = Connecting
				c.statusMutex.Unlock()

				for id, handler := range c.handlers {
					handler.OnStatus(id, c, Connecting)
				}

				if c.conn != nil {
					// We use paho auto reconnect mechanism.
					continue
				}

				opts := mqtt.NewClientOptions().
					AddBroker(fmt.Sprintf("%s://%s:%d",
						c.opts.uri.scheme, c.opts.uri.host, c.opts.uri.port)).
					SetCleanSession(c.opts.cleanSession).
					SetClientID(c.opts.clientID).
					SetConnectionLostHandler(genMqttDisconnectedHandler(c)).
					SetConnectRetryInterval(c.opts.reconnect).
					SetConnectTimeout(c.opts.connectTimeout).
					SetOnConnectHandler(genMqttConnectedHandler(c)).
					SetPassword(c.opts.uri.password).
					SetUsername(c.opts.uri.username)
				c.conn = mqtt.NewClient(opts)
				c.conn.Connect()
			case Connected:
				c.statusMutex.Lock()
				c.status = Connected
				c.statusMutex.Unlock()

				for id, handler := range c.handlers {
					handler.OnStatus(id, c, Connected)
				}
			case Disconnected:
				c.statusMutex.Lock()
				c.status = Disconnected
				c.statusMutex.Unlock()

				ch <- Connecting
			}
		}
	}()

	return ch
}

// To parse general-mq URI and transform information for paho-mqtt.
func mqttParseURI(uri string) (mqttURI, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return mqttURI{}, err
	}

	port := mqttSchemePorts[u.Scheme]
	scheme := mqttSchemeMaps[u.Scheme]
	if port == 0 || scheme == "" {
		return mqttURI{}, errors.New("wrong scheme")
	}
	if uPort, _ := strconv.Atoi(u.Port()); uPort != 0 {
		port = uPort
	}
	password, _ := u.User.Password()

	return mqttURI{
		scheme:   scheme,
		host:     u.Host,
		port:     port,
		username: u.User.Username(),
		password: password,
	}, nil
}

// To validate the MQTT client name.
func mqttClientIDValidate(name string) bool {
	regexp, err := regexp2.Compile(mqttClientIDPattern, regexp2.None)
	if err != nil || regexp == nil {
		return false
	}
	match, _ := regexp.MatchString(name)
	return match
}

func genMqttConnectedHandler(c *MqttConnection) mqtt.OnConnectHandler {
	return func(_c mqtt.Client) {
		c.statusMutex.Lock()
		if c.status == Connected {
			c.statusMutex.Unlock()
			return
		}
		c.status = Connected
		c.statusMutex.Unlock()

		// Subscribe all topics again.
		topics := map[string]byte{}
		c.packetHandlersMutex.Lock()
		for _, item := range c.packetHandlers {
			topics[item.topic] = item.qos
		}
		c.packetHandlersMutex.Unlock()
		conn := c.conn
		if conn != nil {
			conn.SubscribeMultiple(topics, c.mqttMessageHandler)
		}

		c.evChannel <- Connected
	}
}

func genMqttDisconnectedHandler(c *MqttConnection) mqtt.ConnectionLostHandler {
	return func(_c mqtt.Client, err error) {
		c.statusMutex.Lock()
		c.status = Connecting
		c.statusMutex.Unlock()

		for id, handler := range c.handlers {
			handler.OnStatus(id, c, Connecting)
		}
		c.evChannel <- Connecting
	}
}

func genMqttMessageHandler(c *MqttConnection) mqtt.MessageHandler {
	return func(_c mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()

		var item mqttPacketHandlerItem
		c.packetHandlersMutex.Lock()
		item = c.packetHandlers[topic]
		c.packetHandlersMutex.Unlock()

		if item.handler != nil {
			item.handler.OnPublish(msg)
		}
	}
}
