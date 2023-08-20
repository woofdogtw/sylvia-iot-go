package mq

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"sync"
	"time"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
	"github.com/woofdogtw/sylvia-iot-go/sdk/constants"
)

// Uplink data from broker to application.
type AppUlData struct {
	DataID      string
	Time        time.Time
	Pub         time.Time
	DeviceID    string
	NetworkID   string
	NetworkCode string
	NetworkAddr string
	IsPublic    bool
	Data        []byte
	Extension   map[string]interface{}
}

// Downlink data from application to broker.
type AppDlData struct {
	CorrelationID string
	DeviceID      string
	NetworkCode   string
	NetworkAddr   string
	Data          []byte
	Extension     map[string]interface{}
}

// Downlink data response for `DlData`.
type AppDlDataResp struct {
	CorrelationID string `json:"correlationId"`
	DataID        string `json:"dataId"`
	Error         string `json:"error"`
	Message       string `json:"message"`
}

// Downlink data result when processing or completing data transfer to the device.
type AppDlDataResult struct {
	DataID  string `json:"dataId"`
	Status  int    `json:"status"`
	Message string `json:"message"`
}

// The manager for application queues.
type ApplicationMgr struct {
	opts Options

	// Information for delete connection automatically.
	connPool *ConnectionPool
	hostUri  string

	uldata       gmq.GmqQueue
	dldata       gmq.GmqQueue
	dldataResp   gmq.GmqQueue
	dldataResult gmq.GmqQueue

	status      MgrStatus
	statusMutex sync.Mutex
	handler     AppMgrEventHandler
}

// Event handler interface for the `ApplicationMgr`.
type AppMgrEventHandler interface {
	// Fired when one of the manager's queues encounters a state change.
	OnStatusChange(mgr *ApplicationMgr, status MgrStatus)

	// Fired when a `UlData` data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnUlData(mgr *ApplicationMgr, data *AppUlData) error

	// Fired when a `DlDataResult` data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnDlDataResp(mgr *ApplicationMgr, data *AppDlDataResp) error

	// Fired when a `DlDataResp` data is received.
	//
	// Return error will NACK the data.
	// The data may will be received again depending on the protocol (such as AMQP).
	OnDlDataResult(mgr *ApplicationMgr, data *AppDlDataResult) error
}

// The event handler for `gmq.GmqQueue`.
type appMgrMqEventHandler struct {
	mgr *ApplicationMgr
}

type appUlDataInner struct {
	DataID      string                 `json:"dataId"`
	Time        string                 `json:"time"`
	Pub         string                 `json:"pub"`
	DeviceID    string                 `json:"deviceId"`
	NetworkID   string                 `json:"networkId"`
	NetworkCode string                 `json:"networkCode"`
	NetworkAddr string                 `json:"networkAddr"`
	IsPublic    bool                   `json:"isPublic"`
	Data        string                 `json:"data"`
	Extension   map[string]interface{} `json:"extension"`
}

type appDlDataInner struct {
	CorrelationID string                 `json:"correlationId"`
	DeviceID      string                 `json:"deviceId,omitempty"`
	NetworkCode   string                 `json:"networkCode,omitempty"`
	NetworkAddr   string                 `json:"networkAddr,omitempty"`
	Data          string                 `json:"data"`
	Extension     map[string]interface{} `json:"extension,omitempty"`
}

// Constants.
const (
	appQueuePrefix = "broker.application"
)

var _ gmq.QueueEventHandler = (*appMgrMqEventHandler)(nil)
var _ gmq.QueueMessageHandler = (*appMgrMqEventHandler)(nil)

func NewApplicationMgr(connPool *ConnectionPool, hostUri url.URL, opts Options,
	handler AppMgrEventHandler) (*ApplicationMgr, error) {
	if opts.UnitID == "" {
		return nil, errors.New("`UnitID` cannot be empty for application")
	}

	conn, err := getConnection(connPool, hostUri)
	if err != nil {
		return nil, err
	}

	queues, err := newDataQueues(conn, opts, appQueuePrefix, false)
	if err != nil {
		return nil, err
	}

	mgr := &ApplicationMgr{
		opts:         opts,
		connPool:     connPool,
		hostUri:      hostUri.String(),
		uldata:       queues.uldata,
		dldata:       queues.dldata,
		dldataResp:   queues.dldataResp,
		dldataResult: queues.dldataResult,
		status:       NotReady,
		handler:      handler,
	}
	mqHandler := &appMgrMqEventHandler{mgr: mgr}
	mgr.uldata.SetHandler(mqHandler)
	mgr.uldata.SetMsgHandler(mqHandler)
	if err := mgr.uldata.Connect(); err != nil {
		return nil, err
	}
	mgr.dldata.SetHandler(mqHandler)
	if err := mgr.dldata.Connect(); err != nil {
		return nil, err
	}
	mgr.dldataResp.SetHandler(mqHandler)
	mgr.dldataResp.SetMsgHandler(mqHandler)
	if err := mgr.dldataResp.Connect(); err != nil {
		return nil, err
	}
	mgr.dldataResult.SetHandler(mqHandler)
	mgr.dldataResult.SetMsgHandler(mqHandler)
	if err := mgr.dldataResult.Connect(); err != nil {
		return nil, err
	}
	conn.add(4)
	return mgr, nil
}

// The associated unit ID of the application.
func (mgr *ApplicationMgr) UnitID() string {
	return mgr.opts.UnitID
}

// The associated unit code of the application.
func (mgr *ApplicationMgr) UnitCode() string {
	return mgr.opts.UnitCode
}

// The application ID.
func (mgr *ApplicationMgr) ID() string {
	return mgr.opts.ID
}

// The application code.
func (mgr *ApplicationMgr) Name() string {
	return mgr.opts.Name
}

// Manager status.
func (mgr *ApplicationMgr) Status() MgrStatus {
	return mgr.status
}

// Detail status of each message queue. Please ignore `Ctrl`.
func (mgr *ApplicationMgr) MqStatus() DataMqStatus {
	return DataMqStatus{
		UlData:       mgr.uldata.Status(),
		DlData:       mgr.dldata.Status(),
		DlDataResp:   mgr.dldataResp.Status(),
		DlDataResult: mgr.dldataResult.Status(),
		Ctrl:         gmq.Closed,
	}
}

// To close the manager queues.
// The underlying connection will be closed when there are no queues use it.
func (mgr *ApplicationMgr) Close() error {
	if err := mgr.uldata.Close(); err != nil {
		return err
	} else if err = mgr.dldata.Close(); err != nil {
		return err
	} else if err = mgr.dldataResp.Close(); err != nil {
		return err
	} else if err = mgr.dldataResult.Close(); err != nil {
		return err
	}

	return removeConnection(mgr.connPool, mgr.hostUri, 4)
}

// Send downlink data `DlData` to the broker.
func (mgr *ApplicationMgr) SendDlData(data AppDlData) error {
	if data.CorrelationID == "" {
		return errors.New(errParamCorrID)
	}
	if data.DeviceID == "" && (data.NetworkCode == "" || data.NetworkAddr == "") {
		return errors.New(errParamAppDev)
	}

	msg := appDlDataInner{
		CorrelationID: data.CorrelationID,
		DeviceID:      data.DeviceID,
		NetworkCode:   data.NetworkCode,
		NetworkAddr:   data.NetworkAddr,
		Data:          hex.EncodeToString(data.Data),
		Extension:     data.Extension,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	go func() {
		_ = mgr.dldata.SendMsg(payload)
	}()
	return nil
}

func (h *appMgrMqEventHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	h.onEvent(queue)
}

func (h *appMgrMqEventHandler) OnError(queue gmq.GmqQueue, err error) {
	h.onEvent(queue)
}

func (h *appMgrMqEventHandler) OnMessage(queue gmq.GmqQueue, message gmq.Message) {
	queueName := queue.Name()
	if queueName == h.mgr.uldata.Name() {
		var data appUlDataInner
		if err := json.Unmarshal(message.Payload(), &data); err != nil {
			_ = message.Ack()
			return
		}
		dataBytes, err := hex.DecodeString(data.Data)
		if err != nil {
			_ = message.Ack()
			return
		}
		dTime, err := time.Parse(constants.TimeFormat, data.Time)
		if err != nil {
			_ = message.Ack()
			return
		}
		dPub, err := time.Parse(constants.TimeFormat, data.Pub)
		if err != nil {
			_ = message.Ack()
			return
		}
		uldata := &AppUlData{
			DataID:      data.DataID,
			Time:        dTime.UTC(),
			Pub:         dPub.UTC(),
			DeviceID:    data.DeviceID,
			NetworkID:   data.NetworkID,
			NetworkCode: data.NetworkCode,
			NetworkAddr: data.NetworkAddr,
			IsPublic:    data.IsPublic,
			Data:        dataBytes,
			Extension:   data.Extension,
		}
		handler := h.mgr.handler
		if handler != nil {
			if err := handler.OnUlData(h.mgr, uldata); err != nil {
				_ = message.Nack()
			} else {
				_ = message.Ack()
			}
			return
		}
	} else if queueName == h.mgr.dldataResp.Name() {
		var data AppDlDataResp
		if err := json.Unmarshal(message.Payload(), &data); err != nil {
			_ = message.Ack()
			return
		}
		handler := h.mgr.handler
		if handler != nil {
			if err := handler.OnDlDataResp(h.mgr, &data); err != nil {
				_ = message.Nack()
			} else {
				_ = message.Ack()
			}
			return
		}
	} else if queueName == h.mgr.dldataResult.Name() {
		var data AppDlDataResult
		if err := json.Unmarshal(message.Payload(), &data); err != nil {
			_ = message.Ack()
			return
		}
		handler := h.mgr.handler
		if handler != nil {
			if err := handler.OnDlDataResult(h.mgr, &data); err != nil {
				_ = message.Nack()
			} else {
				_ = message.Ack()
			}
			return
		}
	}
	_ = message.Ack()
}

func (h *appMgrMqEventHandler) onEvent(queue gmq.GmqQueue) {
	h.mgr.statusMutex.Lock()
	var mgrStatus MgrStatus
	if h.mgr.uldata.Status() == gmq.Connected &&
		h.mgr.dldata.Status() == gmq.Connected &&
		h.mgr.dldataResp.Status() == gmq.Connected &&
		h.mgr.dldataResult.Status() == gmq.Connected {
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
