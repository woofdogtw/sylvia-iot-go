package mq

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
	"github.com/woofdogtw/sylvia-iot-go/sdk/constants"
)

// Uplink data from network to broker.
type NetUlData struct {
	Time        time.Time
	NetworkAddr string
	Data        []byte
	Extension   map[string]interface{}
}

// Downlink data from broker to network.
type NetDlData struct {
	DataID      string
	Pub         time.Time
	ExpiresIn   int64
	NetworkAddr string
	Data        []byte
	Extension   map[string]interface{}
}

// Downlink data result when processing or completing data transfer to the device.
type NetDlDataResult struct {
	DataID  string `json:"dataId"`
	Status  int    `json:"status"`
	Message string `json:"message,omitempty"`
}

// `add-device` control data.
type NetCtrlAddDevice struct {
	NetworkAddr string `json:"networkAddr"`
}

// `add-device-bulk` control data.
type NetCtrlAddDeviceBulk struct {
	NetworkAddrs []string `json:"networkAddrs"`
}

// `add-device-range` control data.
type NetCtrlAddDeviceRange struct {
	StartAddr string `json:"startAddr"`
	EndAddr   string `json:"endAddr"`
}

// `del-device` control data.
type NetCtrlDelDevice struct {
	NetworkAddr string `json:"networkAddr"`
}

// `del-device-bulk` control data.
type NetCtrlDelDeviceBulk struct {
	NetworkAddrs []string `json:"networkAddrs"`
}

// `del-device-range` control data.
type NetCtrlDelDeviceRange struct {
	StartAddr string `json:"startAddr"`
	EndAddr   string `json:"endAddr"`
}

// The manager for network queues.
type NetworkMgr struct {
	opts Options

	// Information for delete connection automatically.
	connPool *ConnectionPool
	hostUri  string

	uldata       gmq.GmqQueue
	dldata       gmq.GmqQueue
	dldataResult gmq.GmqQueue
	ctrl         gmq.GmqQueue

	status      MgrStatus
	statusMutex sync.Mutex
	handler     NetMgrEventHandler
}

// Event handler interface for the `NetworkMgr`.
type NetMgrEventHandler interface {
	// Fired when one of the manager's queues encounters a state change.
	OnStatusChange(mgr *NetworkMgr, status MgrStatus)

	// Fired when a `DlData` data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnDlData(mgr *NetworkMgr, data *NetDlData) error

	// Fired when a `add-device` control data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnCtrlAddDevice(mgr *NetworkMgr, time time.Time, new *NetCtrlAddDevice) error

	// Fired when a `add-device-bulk` control data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnCtrlAddDeviceBulk(mgr *NetworkMgr, time time.Time, new *NetCtrlAddDeviceBulk) error

	// Fired when a `add-device-range` control data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnCtrlAddDeviceRange(mgr *NetworkMgr, time time.Time, new *NetCtrlAddDeviceRange) error

	// Fired when a `del-device` control data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnCtrlDelDevice(mgr *NetworkMgr, time time.Time, new *NetCtrlDelDevice) error

	// Fired when a `del-device-bulk` control data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnCtrlDelDeviceBulk(mgr *NetworkMgr, time time.Time, new *NetCtrlDelDeviceBulk) error

	// Fired when a `del-device-range` control data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnCtrlDelDeviceRange(mgr *NetworkMgr, time time.Time, new *NetCtrlDelDeviceRange) error
}

// The event handler for `gmq.GmqQueue`.
type netMgrMqEventHandler struct {
	mgr *NetworkMgr
}

type netUlDataInner struct {
	Time        string                 `json:"time"`
	NetworkAddr string                 `json:"networkAddr"`
	Data        string                 `json:"data"`
	Extension   map[string]interface{} `json:"extension,omitempty"`
}

type netDlDataInner struct {
	DataID      string                 `json:"dataId"`
	Pub         string                 `json:"pub"`
	ExpiresIn   int64                  `json:"expiresIn"`
	NetworkAddr string                 `json:"networkAddr"`
	Data        string                 `json:"data"`
	Extension   map[string]interface{} `json:"extension,omitempty"`
}

// Constants.
const (
	netQueuePrefix = "broker.network"
)

var _ gmq.QueueEventHandler = (*netMgrMqEventHandler)(nil)
var _ gmq.QueueMessageHandler = (*netMgrMqEventHandler)(nil)

func NewNetworkMgr(
	connPool *ConnectionPool,
	hostUri url.URL,
	opts Options,
	handler NetMgrEventHandler,
) (*NetworkMgr, error) {
	conn, err := getConnection(connPool, hostUri)
	if err != nil {
		return nil, err
	}

	queues, err := newDataQueues(conn, opts, netQueuePrefix, true)
	if err != nil {
		return nil, err
	}

	mgr := &NetworkMgr{
		opts:         opts,
		connPool:     connPool,
		hostUri:      hostUri.String(),
		uldata:       queues.uldata,
		dldata:       queues.dldata,
		dldataResult: queues.dldataResult,
		ctrl:         queues.ctrl,
		status:       NotReady,
		handler:      handler,
	}
	mqHandler := &netMgrMqEventHandler{mgr: mgr}
	mgr.uldata.SetHandler(mqHandler)
	if err := mgr.uldata.Connect(); err != nil {
		return nil, err
	}
	mgr.dldata.SetHandler(mqHandler)
	mgr.dldata.SetMsgHandler(mqHandler)
	if err := mgr.dldata.Connect(); err != nil {
		return nil, err
	}
	mgr.dldataResult.SetHandler(mqHandler)
	if err := mgr.dldataResult.Connect(); err != nil {
		return nil, err
	}
	mgr.ctrl.SetHandler(mqHandler)
	mgr.ctrl.SetMsgHandler(mqHandler)
	if err := mgr.ctrl.Connect(); err != nil {
		return nil, err
	}
	conn.add(4)
	return mgr, nil
}

// The associated unit ID of the network.
func (mgr *NetworkMgr) UnitID() string {
	return mgr.opts.UnitID
}

// The associated unit code of the network.
func (mgr *NetworkMgr) UnitCode() string {
	return mgr.opts.UnitCode
}

// The network ID.
func (mgr *NetworkMgr) ID() string {
	return mgr.opts.ID
}

// The network code.
func (mgr *NetworkMgr) Name() string {
	return mgr.opts.Name
}

// Manager status.
func (mgr *NetworkMgr) Status() MgrStatus {
	return mgr.status
}

// Detail status of each message queue. Please ignore `DlDataResp`.
func (mgr *NetworkMgr) MqStatus() DataMqStatus {
	return DataMqStatus{
		UlData:       mgr.uldata.Status(),
		DlData:       mgr.dldata.Status(),
		DlDataResp:   gmq.Closed,
		DlDataResult: mgr.dldataResult.Status(),
		Ctrl:         mgr.ctrl.Status(),
	}
}

// To close the manager queues.
// The underlying connection will be closed when there are no queues use it.
func (mgr *NetworkMgr) Close() error {
	if err := mgr.uldata.Close(); err != nil {
		return err
	} else if err = mgr.dldata.Close(); err != nil {
		return err
	} else if err = mgr.dldataResult.Close(); err != nil {
		return err
	} else if err = mgr.ctrl.Close(); err != nil {
		return err
	}

	return removeConnection(mgr.connPool, mgr.hostUri, 4)
}

// Send uplink data `UlData` to the broker.
func (mgr *NetworkMgr) SendUlData(data NetUlData) error {
	if data.NetworkAddr == "" {
		return errors.New(errParamNetDev)
	}

	msg := netUlDataInner{
		Time:        data.Time.Format(constants.TimeFormat),
		NetworkAddr: data.NetworkAddr,
		Data:        hex.EncodeToString(data.Data),
		Extension:   data.Extension,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	go func() {
		_ = mgr.uldata.SendMsg(payload)
	}()
	return nil
}

// Send uplink data `DlDataResult` to the broker.
func (mgr *NetworkMgr) SendDlDataResult(data NetDlDataResult) error {
	if data.DataID == "" {
		return errors.New(errParamDataID)
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	go func() {
		_ = mgr.dldataResult.SendMsg(payload)
	}()
	return nil
}

func (h *netMgrMqEventHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	h.onEvent(queue)
}

func (h *netMgrMqEventHandler) OnError(queue gmq.GmqQueue, err error) {
	h.onEvent(queue)
}

func (h *netMgrMqEventHandler) OnMessage(queue gmq.GmqQueue, message gmq.Message) {
	queueName := queue.Name()
	if queueName == h.mgr.dldata.Name() {
		var data netDlDataInner
		if err := json.Unmarshal(message.Payload(), &data); err != nil {
			_ = message.Ack()
			return
		}
		dataBytes, err := hex.DecodeString(data.Data)
		if err != nil {
			_ = message.Ack()
			return
		}
		dPub, err := time.Parse(constants.TimeFormat, data.Pub)
		if err != nil {
			_ = message.Ack()
			return
		}
		dldata := &NetDlData{
			DataID:      data.DataID,
			Pub:         dPub.UTC(),
			ExpiresIn:   data.ExpiresIn,
			NetworkAddr: data.NetworkAddr,
			Data:        dataBytes,
			Extension:   data.Extension,
		}
		handler := h.mgr.handler
		if handler != nil {
			if err := handler.OnDlData(h.mgr, dldata); err != nil {
				_ = message.Nack()
			} else {
				_ = message.Ack()
			}
			return
		}
	} else if queueName == h.mgr.ctrl.Name() {
		payload := message.Payload()
		dTime, err := time.Parse(constants.TimeFormat, gjson.GetBytes(payload, "time").String())
		if err != nil {
			_ = message.Ack()
			return
		}
		new := gjson.GetBytes(payload, "new").String()

		switch gjson.GetBytes(payload, "operation").String() {
		case "add-device":
			var data NetCtrlAddDevice
			err := json.Unmarshal([]byte(new), &data)
			if err != nil {
				_ = message.Ack()
				return
			}
			handler := h.mgr.handler
			if handler != nil {
				if err := handler.OnCtrlAddDevice(h.mgr, dTime.UTC(), &data); err != nil {
					_ = message.Nack()
				} else {
					_ = message.Ack()
				}
				return
			}
		case "add-device-bulk":
			var data NetCtrlAddDeviceBulk
			err := json.Unmarshal([]byte(new), &data)
			if err != nil {
				_ = message.Ack()
				return
			}
			handler := h.mgr.handler
			if handler != nil {
				if err := handler.OnCtrlAddDeviceBulk(h.mgr, dTime.UTC(), &data); err != nil {
					_ = message.Nack()
				} else {
					_ = message.Ack()
				}
				return
			}
		case "add-device-range":
			var data NetCtrlAddDeviceRange
			err := json.Unmarshal([]byte(new), &data)
			if err != nil {
				_ = message.Ack()
				return
			}
			handler := h.mgr.handler
			if handler != nil {
				if err := handler.OnCtrlAddDeviceRange(h.mgr, dTime.UTC(), &data); err != nil {
					_ = message.Nack()
				} else {
					_ = message.Ack()
				}
				return
			}
		case "del-device":
			var data NetCtrlDelDevice
			err := json.Unmarshal([]byte(new), &data)
			if err != nil {
				_ = message.Ack()
				return
			}
			handler := h.mgr.handler
			if handler != nil {
				if err := handler.OnCtrlDelDevice(h.mgr, dTime.UTC(), &data); err != nil {
					_ = message.Nack()
				} else {
					_ = message.Ack()
				}
				return
			}
		case "del-device-bulk":
			var data NetCtrlDelDeviceBulk
			err := json.Unmarshal([]byte(new), &data)
			if err != nil {
				_ = message.Ack()
				return
			}
			handler := h.mgr.handler
			if handler != nil {
				if err := handler.OnCtrlDelDeviceBulk(h.mgr, dTime.UTC(), &data); err != nil {
					_ = message.Nack()
				} else {
					_ = message.Ack()
				}
				return
			}
		case "del-device-range":
			var data NetCtrlDelDeviceRange
			err := json.Unmarshal([]byte(new), &data)
			if err != nil {
				_ = message.Ack()
				return
			}
			handler := h.mgr.handler
			if handler != nil {
				if err := handler.OnCtrlDelDeviceRange(h.mgr, dTime.UTC(), &data); err != nil {
					_ = message.Nack()
				} else {
					_ = message.Ack()
				}
				return
			}
		default:
			_ = message.Ack()
			return
		}
	}
	_ = message.Ack()
}

func (h *netMgrMqEventHandler) onEvent(queue gmq.GmqQueue) {
	h.mgr.statusMutex.Lock()
	var mgrStatus MgrStatus
	if h.mgr.uldata.Status() == gmq.Connected &&
		h.mgr.dldata.Status() == gmq.Connected &&
		h.mgr.dldataResult.Status() == gmq.Connected &&
		h.mgr.ctrl.Status() == gmq.Connected {
		mgrStatus = Ready
	} else {
		mgrStatus = NotReady
	}

	changed := false
	if h.mgr.status != mgrStatus {
		h.mgr.status = mgrStatus
		changed = true
	}
	h.mgr.statusMutex.Unlock()

	if changed {
		handler := h.mgr.handler
		if handler != nil {
			handler.OnStatusChange(h.mgr, mgrStatus)
		}
	}
}
