/*
Package mq manages queues for applications and networks.

For applications, the `ApplicationMgr` manages the following kind of queues:
  - uldata: uplink data from the broker to the application.
  - dldata: downlink data from the application to the broker.
  - dldata-resp: the response of downlink data.
  - dldata-result: the data process result from the network.

For networks, the `NetworkMgr` manages the following kind of queues:
  - uldata: device uplink data from the network to the broker.
  - dldata: downlink data from the broker to the network.
  - dldata-result: the data process result from the network.
  - ctrl: the control messages from the broker to the network
*/
package mq

import (
	"errors"
	"fmt"
	"net/url"
	"sync"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
)

// Connection pool. The key is `host` of the message broker.
type ConnectionPool struct {
	connections map[string]*counterConnection
	mutex       sync.Mutex
}

// Detail queue connection status.
type DataMqStatus struct {
	// For `uldata`.
	UlData gmq.Status
	// For `dldata`.
	DlData gmq.Status
	// For `dldata-resp`.
	DlDataResp gmq.Status
	// For `dldata-result`.
	DlDataResult gmq.Status
	// For `ctrl`.
	Ctrl gmq.Status
}

// The options of the application/network manager.
type Options struct {
	// The associated unit ID of the application/network. Empty for public network.
	UnitID string `json:"unitId"`
	// The associated unit code of the application/network. Empty for public network.
	UnitCode string `json:"unitCode"`
	// The associated application/network ID.
	ID string `json:"id"`
	// The associated application/network code.
	Name string `json:"name"`
	// AMQP prefetch option.
	Prefetch uint16 `json:"prefetch"`
	// AMQP persistent option.
	Persistent bool `json:"persistent"`
	// MQTT shared queue prefix option.
	SharedPrefix string `json:"sharedPrefix"`
}

// The connection with queue counter.
type counterConnection struct {
	connection gmq.GmqConnection
	counter    int
	mutex      sync.Mutex
	connType   connectionType
}

// The created data queues.
type dataQueues struct {
	// `[prefix].[unit].[code].uldata`.
	uldata gmq.GmqQueue
	// `[prefix].[unit].[code].dldata`.
	dldata gmq.GmqQueue
	// `[prefix].[unit].[code].dldata-resp`: for applications only. `nil` for networks.
	dldataResp gmq.GmqQueue
	// `[prefix].[unit].[code].dldata-result`.
	dldataResult gmq.GmqQueue
	// `[prefix].[unit].[code].ctrl`.
	ctrl gmq.GmqQueue
}

// Application/Network Manager status.
type MgrStatus int

// To identify shared connection type.
type connectionType int

// Manager status.
const (
	// One or more queues are not connected.
	NotReady MgrStatus = iota
	// All queues are connected.
	Ready
)

// Constants.
const (
	// Default AMQP prefetch.
	defPrefetch = 100
)

// Connection type.
const (
	connTypeAmqp connectionType = iota
	connTypeMqtt
)

// Error response.
const (
	errParamCorrID = "the `CorrelationID` must be a non-empty string"
	errParamData   = "the `Data` must be a hex string"
	errParamDataID = "the `DataID` must be a non-empty string"
	errParamAppDev = "one of `DeviceID` or [`NetworkCode`, `NetworkAddr`] pair must be provided with non-empty string"
	errParamNetDev = "`NetworkAddr` must be a non-empty string"
)

// Constants.
var (
	// Support application/network host schemes.
	SupportSchemes = []string{"amqp", "amqps", "mqtt", "mqtts"}
)

func (s MgrStatus) String() string {
	switch s {
	case NotReady:
		return "not ready"
	case Ready:
		return "ready"
	}
	return "unknown"
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		connections: map[string]*counterConnection{},
	}
}

func (p *ConnectionPool) ForceClear() {
	p.mutex.Lock()
	for _, conn := range p.connections {
		c := conn
		go func() { _ = c.connection.Close() }()
	}
	p.connections = map[string]*counterConnection{}
	p.mutex.Unlock()
}

func (c *counterConnection) add(count int) {
	c.mutex.Lock()
	c.counter += count
	c.mutex.Unlock()
}

// Utility function to get the message queue connection instance. A new connection will be created
// if the host does not exist.
func getConnection(pool *ConnectionPool, hostUri url.URL) (*counterConnection, error) {
	if pool == nil {
		return nil, errors.New("connection pool is nil")
	}
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	conn := pool.connections[hostUri.String()]
	if conn != nil {
		return conn, nil
	}

	switch hostUri.Scheme {
	case "amqp", "amqps":
		opts := gmq.AmqpConnectionOptions{
			URI: hostUri.String(),
		}
		conn, err := gmq.NewAmqpConnection(opts)
		if err != nil {
			return nil, err
		}
		_ = conn.Connect()
		retConn := &counterConnection{
			connection: conn,
			counter:    0,
			connType:   connTypeAmqp,
		}
		pool.connections[hostUri.String()] = retConn
		return retConn, nil
	case "mqtt", "mqtts":
		opts := gmq.MqttConnectionOptions{
			URI: hostUri.String(),
		}
		conn, err := gmq.NewMqttConnection(opts)
		if err != nil {
			return nil, err
		}
		_ = conn.Connect()
		retConn := &counterConnection{
			connection: conn,
			counter:    0,
			connType:   connTypeMqtt,
		}
		pool.connections[hostUri.String()] = retConn
		return retConn, nil
	default:
		return nil, errors.New("unsupport scheme" + hostUri.Scheme)
	}
}

// Utility function to remove connection from the pool if the reference count meet zero.
func removeConnection(pool *ConnectionPool, hostUri string, count int) error {
	pool.mutex.Lock()

	conn, ok := pool.connections[hostUri]
	if !ok {
		pool.mutex.Unlock()
		return nil
	}
	conn.counter -= count
	if conn.counter > 0 {
		pool.mutex.Unlock()
		return nil
	}
	delete(pool.connections, hostUri)
	pool.mutex.Unlock()

	return conn.connection.Close()
}

// The utility function for creating application/network queue.
func newDataQueues(conn *counterConnection, opts Options, prefix string,
	isNetwork bool) (*dataQueues, error) {
	var uldata gmq.GmqQueue
	var dldata gmq.GmqQueue
	var dldataResp gmq.GmqQueue
	var dldataResult gmq.GmqQueue
	var ctrl gmq.GmqQueue

	if opts.UnitID == "" {
		if opts.UnitCode != "" {
			return nil, errors.New("UnitID and UnitCode must both empty or non-empty")
		}
	} else {
		if opts.UnitCode == "" {
			return nil, errors.New("UnitID and UnitCode must both empty or non-empty")
		}
	}
	if opts.ID == "" {
		return nil, errors.New("`ID` cannot be empty")
	}
	if opts.Name == "" {
		return nil, errors.New("`Name` cannot be empty")
	}

	unit := opts.UnitCode
	if unit == "" {
		unit = "_"
	}

	var err error
	if conn.connType == connTypeAmqp {
		prefetch := opts.Prefetch
		if prefetch == 0 {
			prefetch = defPrefetch
		}

		uldataOpts := gmq.AmqpQueueOptions{
			Name:       fmt.Sprintf("%s.%s.%s.uldata", prefix, unit, opts.Name),
			IsRecv:     !isNetwork,
			Reliable:   true,
			Persistent: opts.Persistent,
			Prefetch:   prefetch,
		}
		dldataOpts := gmq.AmqpQueueOptions{
			Name:       fmt.Sprintf("%s.%s.%s.dldata", prefix, unit, opts.Name),
			IsRecv:     isNetwork,
			Reliable:   true,
			Persistent: opts.Persistent,
			Prefetch:   prefetch,
		}
		dldataRespOpts := gmq.AmqpQueueOptions{
			Name:       fmt.Sprintf("%s.%s.%s.dldata-resp", prefix, unit, opts.Name),
			IsRecv:     !isNetwork,
			Reliable:   true,
			Persistent: opts.Persistent,
			Prefetch:   prefetch,
		}
		dldataResultOpts := gmq.AmqpQueueOptions{
			Name:       fmt.Sprintf("%s.%s.%s.dldata-result", prefix, unit, opts.Name),
			IsRecv:     !isNetwork,
			Reliable:   true,
			Persistent: opts.Persistent,
			Prefetch:   prefetch,
		}
		ctrlOpts := gmq.AmqpQueueOptions{
			Name:     fmt.Sprintf("%s.%s.%s.ctrl", prefix, unit, opts.Name),
			IsRecv:   true,
			Reliable: true,
			Prefetch: prefetch,
		}
		_conn := conn.connection.(*gmq.AmqpConnection)
		if uldata, err = gmq.NewAmqpQueue(uldataOpts, _conn); err != nil {
			return nil, err
		} else if dldata, err = gmq.NewAmqpQueue(dldataOpts, _conn); err != nil {
			return nil, err
		} else if dldataResp, err = gmq.NewAmqpQueue(dldataRespOpts, _conn); err != nil {
			return nil, err
		} else if dldataResult, err = gmq.NewAmqpQueue(dldataResultOpts, _conn); err != nil {
			return nil, err
		} else if ctrl, err = gmq.NewAmqpQueue(ctrlOpts, _conn); err != nil {
			return nil, err
		}
	} else if conn.connType == connTypeMqtt {
		uldataOpts := gmq.MqttQueueOptions{
			Name:         fmt.Sprintf("%s.%s.%s.uldata", prefix, unit, opts.Name),
			IsRecv:       !isNetwork,
			Reliable:     true,
			SharedPrefix: opts.SharedPrefix,
		}
		dldataOpts := gmq.MqttQueueOptions{
			Name:         fmt.Sprintf("%s.%s.%s.dldata", prefix, unit, opts.Name),
			IsRecv:       isNetwork,
			Reliable:     true,
			SharedPrefix: opts.SharedPrefix,
		}
		dldataRespOpts := gmq.MqttQueueOptions{
			Name:         fmt.Sprintf("%s.%s.%s.dldata-resp", prefix, unit, opts.Name),
			IsRecv:       !isNetwork,
			Reliable:     true,
			SharedPrefix: opts.SharedPrefix,
		}
		dldataResultOpts := gmq.MqttQueueOptions{
			Name:         fmt.Sprintf("%s.%s.%s.dldata-result", prefix, unit, opts.Name),
			IsRecv:       !isNetwork,
			Reliable:     true,
			SharedPrefix: opts.SharedPrefix,
		}
		ctrlOpts := gmq.MqttQueueOptions{
			Name:         fmt.Sprintf("%s.%s.%s.ctrl", prefix, unit, opts.Name),
			IsRecv:       true,
			Reliable:     true,
			SharedPrefix: opts.SharedPrefix,
		}
		_conn := conn.connection.(*gmq.MqttConnection)
		if uldata, err = gmq.NewMqttQueue(uldataOpts, _conn); err != nil {
			return nil, err
		} else if dldata, err = gmq.NewMqttQueue(dldataOpts, _conn); err != nil {
			return nil, err
		} else if dldataResp, err = gmq.NewMqttQueue(dldataRespOpts, _conn); err != nil {
			return nil, err
		} else if dldataResult, err = gmq.NewMqttQueue(dldataResultOpts, _conn); err != nil {
			return nil, err
		} else if ctrl, err = gmq.NewMqttQueue(ctrlOpts, _conn); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("unknown shared connection type")
	}

	return &dataQueues{
		uldata:       uldata,
		dldata:       dldata,
		dldataResp:   dldataResp,
		dldataResult: dldataResult,
		ctrl:         ctrl,
	}, nil
}
