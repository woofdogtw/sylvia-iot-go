package mq

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/gomega"
	"github.com/tidwall/gjson"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
	"github.com/woofdogtw/sylvia-iot-go/sdk/mq"
)

type appTestHandler struct {
	mutex              sync.Mutex
	statusChanged      bool
	recvUlData         []mq.AppUlData
	recvDlDataResp     []mq.AppDlDataResp
	recvDlDataResult   []mq.AppDlDataResult
	isUlDataRecv       bool
	isDlDataRespRecv   bool
	isDlDataResultRecv bool
}

type appTestDlDataHandler struct {
	mutex           sync.Mutex
	statusConnected bool
	recvDlData      [][]byte
}

var _ mq.AppMgrEventHandler = (*appTestHandler)(nil)
var _ gmq.QueueEventHandler = (*appTestDlDataHandler)(nil)
var _ gmq.QueueMessageHandler = (*appTestDlDataHandler)(nil)

func (h *appTestHandler) OnStatusChange(mgr *mq.ApplicationMgr, status mq.MgrStatus) {
	h.statusChanged = true
}

func (h *appTestHandler) OnUlData(mgr *mq.ApplicationMgr, data *mq.AppUlData) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isUlDataRecv {
		h.isUlDataRecv = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvUlData = append(h.recvUlData, *data)
	return nil
}

func (h *appTestHandler) OnDlDataResp(mgr *mq.ApplicationMgr, data *mq.AppDlDataResp) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isDlDataRespRecv {
		h.isDlDataRespRecv = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvDlDataResp = append(h.recvDlDataResp, *data)
	return nil
}

func (h *appTestHandler) OnDlDataResult(mgr *mq.ApplicationMgr, data *mq.AppDlDataResult) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isDlDataResultRecv {
		h.isDlDataResultRecv = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvDlDataResult = append(h.recvDlDataResult, *data)
	return nil
}

func (h *appTestDlDataHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	if status == gmq.Connected {
		h.statusConnected = true
	}
}

func (h *appTestDlDataHandler) OnError(queue gmq.GmqQueue, err error) {}

func (h *appTestDlDataHandler) OnMessage(queue gmq.GmqQueue, message gmq.Message) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.recvDlData = append(h.recvDlData, message.Payload())
	_ = message.Ack()
}

// Test new managers with default options.
func appNewDefault() {
	connPool := mq.NewConnectionPool()
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(connPool, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	Expect(mgr.UnitID()).Should(Equal("unit_id"))
	Expect(mgr.UnitCode()).Should(Equal("unit_code"))
	Expect(mgr.ID()).Should(Equal("id_application"))
	Expect(mgr.Name()).Should(Equal("code_application"))

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if handler.statusChanged {
			break
		}
	}
	status := mgr.Status()
	mqStatus := mgr.MqStatus()
	Expect(status).Should(Equal(mq.Ready))
	Expect(mqStatus.UlData).Should(Equal(gmq.Connected))
	Expect(mqStatus.DlData).Should(Equal(gmq.Connected))
	Expect(mqStatus.DlDataResp).Should(Equal(gmq.Connected))
	Expect(mqStatus.DlDataResult).Should(Equal(gmq.Connected))
	Expect(mqStatus.Ctrl).Should(Equal(gmq.Closed))
}

// Test new managers with manual options.
func appNewManual() {
	connPool := mq.NewConnectionPool()
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		Prefetch:     1,
		Persistent:   true,
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(connPool, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)
}

// Test new managers with wrong options.
func appNewWrongOpts() {
	connPool := mq.NewConnectionPool()
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{}
	_, err = mq.NewApplicationMgr(nil, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{}
	_, err = mq.NewApplicationMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{
		UnitCode: "unit_code",
	}
	_, err = mq.NewApplicationMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{
		UnitID: "unit_id",
	}
	_, err = mq.NewApplicationMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{
		UnitID:   "unit_id",
		UnitCode: "unit_code",
	}
	_, err = mq.NewApplicationMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{
		UnitID:   "unit_id",
		UnitCode: "unit_code",
		ID:       "id_application",
	}
	_, err = mq.NewApplicationMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())
}

// Test `Close()`.
func appClose() {
	connPool := mq.NewConnectionPool()
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(connPool, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	err = mgr.Close()
	Expect(err).ShouldNot(HaveOccurred())
}

// Test receiving uldata.
func appUlData() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.application.unit_code.code_application.uldata",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.application.unit_code.code_application.uldata",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if queue.Status() == gmq.Connected {
			break
		}
	}
	Expect(queue.Status()).Should(Equal(gmq.Connected))
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	now := time.Now().UTC()
	data1 := map[string]interface{}{
		"dataId":      "1",
		"time":        now.Format(timeFormat),
		"pub":         now.Add(1 * time.Millisecond).Format(timeFormat),
		"deviceId":    "device_id1",
		"networkId":   "network_id1",
		"networkCode": "network_code1",
		"networkAddr": "network_addr1",
		"isPublic":    true,
		"data":        "01",
	}
	payload, err := json.Marshal(data1)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data2 := map[string]interface{}{
		"dataId":      "2",
		"time":        now.Add(1 * time.Millisecond).Format(timeFormat),
		"pub":         now.Add(2 * time.Millisecond).Format(timeFormat),
		"deviceId":    "device_id2",
		"networkId":   "network_id2",
		"networkCode": "network_code2",
		"networkAddr": "network_addr2",
		"isPublic":    false,
		"data":        "02",
		"extension": map[string]interface{}{
			"key": "value",
		},
	}
	payload, err = json.Marshal(data2)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data3 := map[string]interface{}{
		"dataId":      "3",
		"time":        now.Add(2 * time.Millisecond).Format(timeFormat),
		"pub":         now.Add(3 * time.Millisecond).Format(timeFormat),
		"deviceId":    "device_id3",
		"networkId":   "network_id3",
		"networkCode": "network_code3",
		"networkAddr": "network_addr3",
		"isPublic":    true,
		"data":        "",
	}
	payload, err = json.Marshal(data3)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())

	var expectCount int
	if testMqEngine == EngineRabbitMQ {
		expectCount = 3
	} else {
		expectCount = 2
	}
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if len(handler.recvUlData) >= expectCount {
			break
		}
	}
	Expect(len(handler.recvUlData) >= expectCount).Should(BeTrue())

	for i := range handler.recvUlData {
		data := &handler.recvUlData[i]
		if data.DataID == "1" {
			Expect(testMqEngine).Should(Equal(EngineRabbitMQ))
			Expect(data.Time.UnixMilli()).Should(Equal(now.UnixMilli()))
			Expect(data.Pub.UnixMilli()).Should(Equal(now.UnixMilli() + 1))
			Expect(data.DeviceID).Should(Equal(data1["deviceId"]))
			Expect(data.NetworkID).Should(Equal(data1["networkId"]))
			Expect(data.NetworkCode).Should(Equal(data1["networkCode"]))
			Expect(data.NetworkAddr).Should(Equal(data1["networkAddr"]))
			Expect(data.IsPublic).Should(Equal(data1["isPublic"]))
			Expect(hex.EncodeToString(data.Data)).Should(Equal(data1["data"]))
			Expect(data.Extension).Should(BeNil())
		} else if data.DataID == "2" {
			Expect(data.Time.UnixMilli()).Should(Equal(now.UnixMilli() + 1))
			Expect(data.Pub.UnixMilli()).Should(Equal(now.UnixMilli() + 2))
			Expect(data.DeviceID).Should(Equal(data2["deviceId"]))
			Expect(data.NetworkID).Should(Equal(data2["networkId"]))
			Expect(data.NetworkCode).Should(Equal(data2["networkCode"]))
			Expect(data.NetworkAddr).Should(Equal(data2["networkAddr"]))
			Expect(data.IsPublic).Should(Equal(data2["isPublic"]))
			Expect(hex.EncodeToString(data.Data)).Should(Equal(data2["data"]))
			Expect(data.Extension).Should(Equal(data2["extension"]))
		} else if data.DataID == "3" {
			Expect(data.Time.UnixMilli()).Should(Equal(now.UnixMilli() + 2))
			Expect(data.Pub.UnixMilli()).Should(Equal(now.UnixMilli() + 3))
			Expect(data.DeviceID).Should(Equal(data3["deviceId"]))
			Expect(data.NetworkID).Should(Equal(data3["networkId"]))
			Expect(data.NetworkCode).Should(Equal(data3["networkCode"]))
			Expect(data.NetworkAddr).Should(Equal(data3["networkAddr"]))
			Expect(data.IsPublic).Should(Equal(data3["isPublic"]))
			Expect(hex.EncodeToString(data.Data)).Should(Equal(data3["data"]))
			Expect(data.Extension).Should(BeNil())
		} else {
			panic("receive wrong data " + data.DataID)
		}
	}
}

// Test receiving uldata with wrong content.
func appUlDataWrong() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.application.unit_code.code_application.uldata",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.application.unit_code.code_application.uldata",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if queue.Status() == gmq.Connected {
			break
		}
	}
	Expect(queue.Status()).Should(Equal(gmq.Connected))
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	err = queue.SendMsg([]byte("{"))
	Expect(err).ShouldNot(HaveOccurred())
	now := time.Now().UTC()
	data := map[string]interface{}{
		"dataId":      "1",
		"time":        "2022-20-29T11:45:00.000Z",
		"pub":         now.Format(timeFormat),
		"deviceId":    "device_id1",
		"networkId":   "network_id1",
		"networkCode": "network_code1",
		"networkAddr": "network_addr1",
		"isPublic":    true,
		"data":        "01",
	}
	payload, err := json.Marshal(data)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data["time"] = now.Format(timeFormat)
	data["pub"] = "2022-20-29T11:45:00.000Z"
	payload, err = json.Marshal(data)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data["pub"] = now.Format(timeFormat)
	data["data"] = "gg"
	payload, err = json.Marshal(data)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())

	time.Sleep(1 * time.Second)
	count := len(handler.recvUlData)
	Expect(count).Should(BeZero())
}

// Test generating dldata.
func appDlData() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	dlHandler := &appTestDlDataHandler{}
	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata",
			IsRecv:    true,
			Reliable:  true,
			Broadcast: false,
			Prefetch:  1,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata",
			IsRecv:    true,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	queue.SetHandler(dlHandler)
	queue.SetMsgHandler(dlHandler)
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if queue.Status() == gmq.Connected {
			break
		}
	}
	Expect(queue.Status()).Should(Equal(gmq.Connected))
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	data1 := mq.AppDlData{
		CorrelationID: "1",
		DeviceID:      "device1",
		Data:          []byte{1},
		Extension:     map[string]interface{}{"key": "value"},
	}
	err = mgr.SendDlData(data1)
	Expect(err).ShouldNot(HaveOccurred())
	data2 := mq.AppDlData{
		CorrelationID: "2",
		NetworkCode:   "code",
		NetworkAddr:   "addr2",
		Data:          []byte{2},
	}
	err = mgr.SendDlData(data2)
	Expect(err).ShouldNot(HaveOccurred())

	expectCount := 2
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if len(dlHandler.recvDlData) >= expectCount {
			break
		}
	}
	Expect(len(dlHandler.recvDlData) >= expectCount).Should(BeTrue())

	for i := range dlHandler.recvDlData {
		data := dlHandler.recvDlData[i]
		if gjson.GetBytes(data, "correlationId").String() == "1" {
			Expect(gjson.GetBytes(data, "deviceId").String()).Should(Equal(data1.DeviceID))
			Expect(gjson.GetBytes(data, "networkCode").Exists()).ShouldNot(BeTrue())
			Expect(gjson.GetBytes(data, "networkAddr").Exists()).ShouldNot(BeTrue())
			Expect(gjson.GetBytes(data, "data").String()).Should(Equal(hex.EncodeToString(data1.Data)))
			Expect(gjson.GetBytes(data, "extension.key").String()).Should(Equal("value"))
		} else if gjson.GetBytes(data, "correlationId").String() == "2" {
			Expect(gjson.GetBytes(data, "deviceId").Exists()).ShouldNot(BeTrue())
			Expect(gjson.GetBytes(data, "networkCode").String()).Should(Equal(data2.NetworkCode))
			Expect(gjson.GetBytes(data, "networkAddr").String()).Should(Equal(data2.NetworkAddr))
			Expect(gjson.GetBytes(data, "data").String()).Should(Equal(hex.EncodeToString(data2.Data)))
			Expect(gjson.GetBytes(data, "extension").Exists()).ShouldNot(BeTrue())
		} else {
			panic("receive wrong data " + gjson.GetBytes(data, "correlationId").String())
		}
	}
}

// Test sending dldata with wrong content.
func appDlDataWrong() {
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	data := mq.AppDlData{
		DeviceID: "device",
		Data:     []byte{0},
	}
	Expect(mgr.SendDlData(data)).Should(HaveOccurred())
	data.CorrelationID = "1"
	data.DeviceID = ""
	Expect(mgr.SendDlData(data)).Should(HaveOccurred())
	data.NetworkCode = "code"
	Expect(mgr.SendDlData(data)).Should(HaveOccurred())
}

// Test receiving dldata-resp.
func appDlDataResp() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata-resp",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata-resp",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if queue.Status() == gmq.Connected {
			break
		}
	}
	Expect(queue.Status()).Should(Equal(gmq.Connected))
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	data1 := map[string]interface{}{
		"correlationId": "1",
		"dataId":        "data_id1",
	}
	payload, err := json.Marshal(data1)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data2 := map[string]interface{}{
		"correlationId": "2",
		"dataId":        "data_id2",
	}
	payload, err = json.Marshal(data2)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data3 := map[string]interface{}{
		"correlationId": "3",
		"error":         "error3",
		"message":       "message3",
	}
	payload, err = json.Marshal(data3)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())

	var expectCount int
	if testMqEngine == EngineRabbitMQ {
		expectCount = 3
	} else {
		expectCount = 2
	}
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if len(handler.recvDlDataResp) >= expectCount {
			break
		}
	}
	Expect(len(handler.recvDlDataResp) >= expectCount).Should(BeTrue())

	for i := range handler.recvDlDataResp {
		data := &handler.recvDlDataResp[i]
		if data.CorrelationID == "1" {
			Expect(testMqEngine).Should(Equal(EngineRabbitMQ))
			Expect(data.DataID).Should(Equal(data1["dataId"]))
			Expect(data.Error).Should(BeEmpty())
			Expect(data.Message).Should(BeEmpty())
		} else if data.CorrelationID == "2" {
			Expect(data.DataID).Should(Equal(data2["dataId"]))
			Expect(data.Error).Should(BeEmpty())
			Expect(data.Message).Should(BeEmpty())
		} else if data.CorrelationID == "3" {
			Expect(data.DataID).Should(BeEmpty())
			Expect(data.Error).Should(Equal(data3["error"]))
			Expect(data.Message).Should(Equal(data3["message"]))
		} else {
			panic("receive wrong data " + data.CorrelationID)
		}
	}
}

// Test receiving dldata-resp with wrong content.
func appDlDataRespWrong() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata-resp",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata-resp",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if queue.Status() == gmq.Connected {
			break
		}
	}
	Expect(queue.Status()).Should(Equal(gmq.Connected))
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	err = queue.SendMsg([]byte("{"))
	Expect(err).ShouldNot(HaveOccurred())

	time.Sleep(1 * time.Second)
	count := len(handler.recvDlDataResp)
	Expect(count).Should(BeZero())
}

// Test receiving dldata-result.
func appDlDataResult() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata-result",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata-result",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if queue.Status() == gmq.Connected {
			break
		}
	}
	Expect(queue.Status()).Should(Equal(gmq.Connected))
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	data1 := map[string]interface{}{
		"dataId": "1",
		"status": -1,
	}
	payload, err := json.Marshal(data1)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data2 := map[string]interface{}{
		"dataId": "2",
		"status": 0,
	}
	payload, err = json.Marshal(data2)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data3 := map[string]interface{}{
		"dataId":  "3",
		"status":  1,
		"message": "error",
	}
	payload, err = json.Marshal(data3)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())

	var expectCount int
	if testMqEngine == EngineRabbitMQ {
		expectCount = 3
	} else {
		expectCount = 2
	}
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if len(handler.recvDlDataResult) >= expectCount {
			break
		}
	}
	Expect(len(handler.recvDlDataResult) >= expectCount).Should(BeTrue())

	for i := range handler.recvDlDataResult {
		data := &handler.recvDlDataResult[i]
		if data.DataID == "1" {
			Expect(testMqEngine).Should(Equal(EngineRabbitMQ))
			Expect(data.Status).Should(Equal(data1["status"]))
			Expect(data.Message).Should(BeEmpty())
		} else if data.DataID == "2" {
			Expect(data.Status).Should(Equal(data2["status"]))
			Expect(data.Message).Should(BeEmpty())
		} else if data.DataID == "3" {
			Expect(data.Status).Should(Equal(data3["status"]))
			Expect(data.Message).Should(Equal(data3["message"]))
		} else {
			panic("receive wrong data " + data.DataID)
		}
	}
}

// Test receiving dldata-result with wrong content.
func appDlDataResultWrong() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &appTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_application",
		Name:         "code_application",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewApplicationMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testAppMgrs = append(testAppMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata-result",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.application.unit_code.code_application.dldata-result",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if queue.Status() == gmq.Connected {
			break
		}
	}
	Expect(queue.Status()).Should(Equal(gmq.Connected))
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	err = queue.SendMsg([]byte("{"))
	Expect(err).ShouldNot(HaveOccurred())

	time.Sleep(1 * time.Second)
	count := len(handler.recvDlDataResult)
	Expect(count).Should(BeZero())
}
