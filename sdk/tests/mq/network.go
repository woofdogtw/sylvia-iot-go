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

type netTestHandler struct {
	mutex                      sync.Mutex
	statusChanged              bool
	recvDlData                 []mq.NetDlData
	recvCtrlAddDeviceTime      []time.Time
	recvCtrlAddDevice          []mq.NetCtrlAddDevice
	recvCtrlAddDeviceBulkTime  []time.Time
	recvCtrlAddDeviceBulk      []mq.NetCtrlAddDeviceBulk
	recvCtrlAddDeviceRangeTime []time.Time
	recvCtrlAddDeviceRange     []mq.NetCtrlAddDeviceRange
	recvCtrlDelDeviceTime      []time.Time
	recvCtrlDelDevice          []mq.NetCtrlDelDevice
	recvCtrlDelDeviceBulkTime  []time.Time
	recvCtrlDelDeviceBulk      []mq.NetCtrlDelDeviceBulk
	recvCtrlDelDeviceRangeTime []time.Time
	recvCtrlDelDeviceRange     []mq.NetCtrlDelDeviceRange
	isDlDataRecv               bool
	isCtrlAddDeviceRecv        bool
	isCtrlAddDeviceRecvBulk    bool
	isCtrlAddDeviceRecvRange   bool
	isCtrlDelDeviceRecv        bool
	isCtrlDelDeviceRecvBulk    bool
	isCtrlDelDeviceRecvRange   bool
}

type netTestUlDataHandler struct {
	mutex           sync.Mutex
	statusConnected bool
	recvUlData      [][]byte
}

type netTestDlDataResultHandler struct {
	mutex            sync.Mutex
	statusConnected  bool
	recvDlDataResult [][]byte
}

var _ mq.NetMgrEventHandler = (*netTestHandler)(nil)
var _ gmq.QueueEventHandler = (*netTestUlDataHandler)(nil)
var _ gmq.QueueMessageHandler = (*netTestUlDataHandler)(nil)
var _ gmq.QueueEventHandler = (*netTestDlDataResultHandler)(nil)
var _ gmq.QueueMessageHandler = (*netTestDlDataResultHandler)(nil)

func (h *netTestHandler) OnStatusChange(mgr *mq.NetworkMgr, status mq.MgrStatus) {
	h.statusChanged = true
}

func (h *netTestHandler) OnDlData(mgr *mq.NetworkMgr, data *mq.NetDlData) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isDlDataRecv {
		h.isDlDataRecv = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvDlData = append(h.recvDlData, *data)
	return nil
}

func (h *netTestHandler) OnCtrlAddDevice(
	mgr *mq.NetworkMgr,
	time time.Time,
	new *mq.NetCtrlAddDevice,
) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isCtrlAddDeviceRecv {
		h.isCtrlAddDeviceRecv = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvCtrlAddDeviceTime = append(h.recvCtrlAddDeviceTime, time)
	h.recvCtrlAddDevice = append(h.recvCtrlAddDevice, *new)
	return nil
}

func (h *netTestHandler) OnCtrlAddDeviceBulk(
	mgr *mq.NetworkMgr,
	time time.Time,
	new *mq.NetCtrlAddDeviceBulk,
) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isCtrlAddDeviceRecvBulk {
		h.isCtrlAddDeviceRecvBulk = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvCtrlAddDeviceBulkTime = append(h.recvCtrlAddDeviceBulkTime, time)
	h.recvCtrlAddDeviceBulk = append(h.recvCtrlAddDeviceBulk, *new)
	return nil
}

func (h *netTestHandler) OnCtrlAddDeviceRange(
	mgr *mq.NetworkMgr,
	time time.Time,
	new *mq.NetCtrlAddDeviceRange,
) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isCtrlAddDeviceRecvRange {
		h.isCtrlAddDeviceRecvRange = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvCtrlAddDeviceRangeTime = append(h.recvCtrlAddDeviceRangeTime, time)
	h.recvCtrlAddDeviceRange = append(h.recvCtrlAddDeviceRange, *new)
	return nil
}

func (h *netTestHandler) OnCtrlDelDevice(
	mgr *mq.NetworkMgr,
	time time.Time,
	new *mq.NetCtrlDelDevice,
) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isCtrlDelDeviceRecv {
		h.isCtrlDelDeviceRecv = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvCtrlDelDeviceTime = append(h.recvCtrlDelDeviceTime, time)
	h.recvCtrlDelDevice = append(h.recvCtrlDelDevice, *new)
	return nil
}

func (h *netTestHandler) OnCtrlDelDeviceBulk(
	mgr *mq.NetworkMgr,
	time time.Time,
	new *mq.NetCtrlDelDeviceBulk,
) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isCtrlDelDeviceRecvBulk {
		h.isCtrlDelDeviceRecvBulk = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvCtrlDelDeviceBulkTime = append(h.recvCtrlDelDeviceBulkTime, time)
	h.recvCtrlDelDeviceBulk = append(h.recvCtrlDelDeviceBulk, *new)
	return nil
}

func (h *netTestHandler) OnCtrlDelDeviceRange(
	mgr *mq.NetworkMgr,
	time time.Time,
	new *mq.NetCtrlDelDeviceRange,
) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.isCtrlDelDeviceRecvRange {
		h.isCtrlDelDeviceRecvRange = true
		return errors.New("") // test AMQP NACK.
	}

	h.recvCtrlDelDeviceRangeTime = append(h.recvCtrlDelDeviceRangeTime, time)
	h.recvCtrlDelDeviceRange = append(h.recvCtrlDelDeviceRange, *new)
	return nil
}

func (h *netTestUlDataHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	if status == gmq.Connected {
		h.statusConnected = true
	}
}

func (h *netTestUlDataHandler) OnError(queue gmq.GmqQueue, err error) {}

func (h *netTestUlDataHandler) OnMessage(queue gmq.GmqQueue, message gmq.Message) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.recvUlData = append(h.recvUlData, message.Payload())
	_ = message.Ack()
}

func (h *netTestDlDataResultHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	if status == gmq.Connected {
		h.statusConnected = true
	}
}

func (h *netTestDlDataResultHandler) OnError(queue gmq.GmqQueue, err error) {}

func (h *netTestDlDataResultHandler) OnMessage(queue gmq.GmqQueue, message gmq.Message) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.recvDlDataResult = append(h.recvDlDataResult, message.Payload())
	_ = message.Ack()
}

// Test new managers with default options.
func netNewDefault() {
	connPool := mq.NewConnectionPool()
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	Expect(mgr.UnitID()).Should(Equal("unit_id"))
	Expect(mgr.UnitCode()).Should(Equal("unit_code"))
	Expect(mgr.ID()).Should(Equal("id_network"))
	Expect(mgr.Name()).Should(Equal("code_network"))

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
	Expect(mqStatus.DlDataResp).Should(Equal(gmq.Closed))
	Expect(mqStatus.DlDataResult).Should(Equal(gmq.Connected))
	Expect(mqStatus.Ctrl).Should(Equal(gmq.Connected))
}

// Test new managers with manual options.
func netNewManual() {
	connPool := mq.NewConnectionPool()
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		ID:           "id_network",
		Name:         "code_network",
		Prefetch:     1,
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	opts = mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		Prefetch:     1,
		Persistent:   true,
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err = mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)
}

// Test new managers with wrong options.
func netNewWrongOpts() {
	connPool := mq.NewConnectionPool()
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{}
	_, err = mq.NewNetworkMgr(nil, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{}
	_, err = mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{
		UnitCode: "unit_code",
	}
	_, err = mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{
		UnitID: "unit_id",
	}
	_, err = mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{
		UnitID:   "unit_id",
		UnitCode: "unit_code",
	}
	_, err = mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())

	opts = mq.Options{
		UnitID:   "unit_id",
		UnitCode: "unit_code",
		ID:       "id",
	}
	_, err = mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).Should(HaveOccurred())
}

// Test `Close()`.
func netClose() {
	connPool := mq.NewConnectionPool()
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(connPool, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	err = mgr.Close()
	Expect(err).ShouldNot(HaveOccurred())
}

// Test generating uldata.
func netUlData() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	ulHandler := &netTestUlDataHandler{}
	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.network.unit_code.code_network.uldata",
			IsRecv:    true,
			Reliable:  true,
			Broadcast: false,
			Prefetch:  1,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.network.unit_code.code_network.uldata",
			IsRecv:    true,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	queue.SetHandler(ulHandler)
	queue.SetMsgHandler(ulHandler)
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

	now := time.Now()
	data1 := mq.NetUlData{
		Time:        now,
		NetworkAddr: "addr1",
		Data:        []byte{1},
		Extension:   map[string]interface{}{"key": "value"},
	}
	err = mgr.SendUlData(data1)
	Expect(err).ShouldNot(HaveOccurred())
	data2 := mq.NetUlData{
		Time:        now.Add(1 * time.Millisecond),
		NetworkAddr: "addr2",
		Data:        []byte{2},
	}
	err = mgr.SendUlData(data2)
	Expect(err).ShouldNot(HaveOccurred())

	expectCount := 2
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if len(ulHandler.recvUlData) >= expectCount {
			break
		}
	}
	Expect(len(ulHandler.recvUlData) >= expectCount).Should(BeTrue())

	for i := range ulHandler.recvUlData {
		data := ulHandler.recvUlData[i]
		if gjson.GetBytes(data, "networkAddr").String() == "addr1" {
			Expect(gjson.GetBytes(data, "time").String()).Should(Equal(now.Format(timeFormat)))
			Expect(gjson.GetBytes(data, "data").String()).Should(Equal(hex.EncodeToString(data1.Data)))
			Expect(gjson.GetBytes(data, "extension.key").String()).Should(Equal("value"))
		} else if gjson.GetBytes(data, "networkAddr").String() == "addr2" {
			Expect(gjson.GetBytes(data, "time").String()).Should(Equal(now.Add(1 * time.Millisecond).Format(timeFormat)))
			Expect(gjson.GetBytes(data, "data").String()).Should(Equal(hex.EncodeToString(data2.Data)))
			Expect(gjson.GetBytes(data, "extension").Exists()).ShouldNot(BeTrue())
		} else {
			panic("receive wrong data " + gjson.GetBytes(data, "networkAddr").String())
		}
	}
}

// Test sending uldata with wrong content.
func netUlDataWrong() {
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	data := mq.NetUlData{
		Time: time.Now().UTC(),
		Data: []byte{0},
	}
	Expect(mgr.SendUlData(data)).Should(HaveOccurred())
}

// Test receiving dldata.
func netDlData() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.network.unit_code.code_network.dldata",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.network.unit_code.code_network.dldata",
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
		"pub":         now.Format(timeFormat),
		"expiresIn":   int64(1000),
		"networkAddr": "addr1",
		"data":        "01",
	}
	payload, err := json.Marshal(data1)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data2 := map[string]interface{}{
		"dataId":      "2",
		"pub":         now.Add(1 * time.Millisecond).Format(timeFormat),
		"expiresIn":   int64(2000),
		"networkAddr": "addr2",
		"data":        "02",
		"extension":   map[string]interface{}{"key": "value"},
	}
	payload, err = json.Marshal(data2)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data3 := map[string]interface{}{
		"dataId":      "3",
		"pub":         now.Add(2 * time.Millisecond).Format(timeFormat),
		"expiresIn":   int64(3000),
		"networkAddr": "addr3",
		"data":        "03",
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
		if len(handler.recvDlData) >= expectCount {
			break
		}
	}
	Expect(len(handler.recvDlData) >= expectCount).Should(BeTrue())

	for i := range handler.recvDlData {
		data := &handler.recvDlData[i]
		if data.DataID == "1" {
			Expect(testMqEngine).Should(Equal(EngineRabbitMQ))
			Expect(data.Pub.UnixMilli()).Should(Equal(now.UnixMilli()))
			Expect(data.ExpiresIn).Should(Equal(data1["expiresIn"]))
			Expect(data.NetworkAddr).Should(Equal(data1["networkAddr"]))
			Expect(hex.EncodeToString(data.Data)).Should(Equal(data1["data"]))
			Expect(data.Extension).Should(BeNil())
		} else if data.DataID == "2" {
			Expect(data.Pub.UnixMilli()).Should(Equal(now.UnixMilli() + 1))
			Expect(data.ExpiresIn).Should(Equal(data2["expiresIn"]))
			Expect(data.NetworkAddr).Should(Equal(data2["networkAddr"]))
			Expect(hex.EncodeToString(data.Data)).Should(Equal(data2["data"]))
			Expect(data.Extension).Should(Equal(data2["extension"]))
		} else if data.DataID == "3" {
			Expect(data.Pub.UnixMilli()).Should(Equal(now.UnixMilli() + 2))
			Expect(data.ExpiresIn).Should(Equal(data3["expiresIn"]))
			Expect(data.NetworkAddr).Should(Equal(data3["networkAddr"]))
			Expect(hex.EncodeToString(data.Data)).Should(Equal(data3["data"]))
			Expect(data.Extension).Should(BeNil())
		} else {
			panic("receive wrong data " + data.DataID)
		}
	}
}

// Test receiving dldata with wrong content.
func netDlDataWrong() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.network.unit_code.code_network.dldata",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.network.unit_code.code_network.dldata",
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
	data := map[string]interface{}{
		"dataId":      "1",
		"pub":         "2022-20-29T11:45:00.000Z",
		"expiresIn":   1000,
		"networkAddr": "addr1",
		"data":        "00",
	}
	payload, err := json.Marshal(data)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())

	time.Sleep(1 * time.Second)
	count := len(handler.recvDlData)
	Expect(count).Should(BeZero())
}

// Test generating dldata-result.
func netDlDataResult() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	dlResultHandler := &netTestDlDataResultHandler{}
	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.network.unit_code.code_network.dldata-result",
			IsRecv:    true,
			Reliable:  true,
			Broadcast: false,
			Prefetch:  1,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.network.unit_code.code_network.dldata-result",
			IsRecv:    true,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewMqttQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		panic("connection is not AMQP/MQTT")
	}
	queue.SetHandler(dlResultHandler)
	queue.SetMsgHandler(dlResultHandler)
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

	data1 := mq.NetDlDataResult{
		DataID: "1",
		Status: -1,
	}
	err = mgr.SendDlDataResult(data1)
	Expect(err).ShouldNot(HaveOccurred())
	data2 := mq.NetDlDataResult{
		DataID:  "2",
		Status:  1,
		Message: "error",
	}
	err = mgr.SendDlDataResult(data2)
	Expect(err).ShouldNot(HaveOccurred())

	expectCount := 2
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if len(dlResultHandler.recvDlDataResult) >= expectCount {
			break
		}
	}
	Expect(len(dlResultHandler.recvDlDataResult) >= expectCount).Should(BeTrue())

	for i := range dlResultHandler.recvDlDataResult {
		data := dlResultHandler.recvDlDataResult[i]
		if gjson.GetBytes(data, "dataId").String() == "1" {
			Expect(int(gjson.GetBytes(data, "status").Int())).Should(Equal(data1.Status))
			Expect(gjson.GetBytes(data, "message").Bool()).ShouldNot(BeTrue())
		} else if gjson.GetBytes(data, "dataId").String() == "2" {
			Expect(int(gjson.GetBytes(data, "status").Int())).Should(Equal(data2.Status))
			Expect(gjson.GetBytes(data, "message").String()).Should(Equal(data2.Message))
		} else {
			panic("receive wrong data " + gjson.GetBytes(data, "dataId").String())
		}
	}
}

// Test sending dldata-result with wrong content.
func netDlDataResultWrong() {
	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if mgr.Status() == mq.Ready {
			break
		}
	}
	Expect(mgr.Status()).Should(Equal(mq.Ready))

	data := mq.NetDlDataResult{}
	Expect(mgr.SendDlDataResult(data)).Should(HaveOccurred())
}

// Test receiving ctrl.
func netCtrl() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.network.unit_code.code_network.ctrl",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.network.unit_code.code_network.ctrl",
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

	now := time.Now().UTC().Truncate(1 * time.Millisecond)
	data1 := map[string]interface{}{
		"time":      now.Format(timeFormat),
		"operation": "add-device",
		"new": map[string]interface{}{
			"networkAddr": "addr1",
		},
	}
	payload, err := json.Marshal(data1)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data2 := map[string]interface{}{
		"time":      now.Format(timeFormat),
		"operation": "add-device-bulk",
		"new": map[string]interface{}{
			"networkAddrs": []string{"addr2"},
		},
	}
	payload, err = json.Marshal(data2)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data3 := map[string]interface{}{
		"time":      now.Format(timeFormat),
		"operation": "add-device-range",
		"new": map[string]interface{}{
			"startAddr": "0001",
			"endAddr":   "0002",
		},
	}
	payload, err = json.Marshal(data3)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data4 := map[string]interface{}{
		"time":      now.Format(timeFormat),
		"operation": "del-device",
		"new": map[string]interface{}{
			"networkAddr": "addr4",
		},
	}
	payload, err = json.Marshal(data4)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data5 := map[string]interface{}{
		"time":      now.Format(timeFormat),
		"operation": "del-device-bulk",
		"new": map[string]interface{}{
			"networkAddrs": []string{"addr5"},
		},
	}
	payload, err = json.Marshal(data5)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	data6 := map[string]interface{}{
		"time":      now.Format(timeFormat),
		"operation": "del-device-range",
		"new": map[string]interface{}{
			"startAddr": "0003",
			"endAddr":   "0004",
		},
	}
	payload, err = json.Marshal(data6)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg(payload)
	Expect(err).ShouldNot(HaveOccurred())

	var expectCount int
	if testMqEngine == EngineRabbitMQ {
		expectCount = 12
	} else {
		expectCount = 6
	}
	var recvLen int
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		recvLen = len(handler.recvCtrlAddDevice) + len(handler.recvCtrlAddDeviceBulk) +
			len(handler.recvCtrlAddDeviceRange) + len(handler.recvCtrlDelDevice) +
			len(handler.recvCtrlDelDeviceBulk) + len(handler.recvCtrlDelDeviceRange)
		if recvLen >= expectCount {
			break
		}
	}
	Expect(recvLen >= expectCount).Should(BeTrue())

	{
		Expect(len(handler.recvCtrlAddDeviceTime)).ShouldNot(BeZero())
		Expect(handler.recvCtrlAddDeviceTime[0]).Should(Equal(now))
		Expect(len(handler.recvCtrlAddDevice)).ShouldNot(BeZero())
		data := handler.recvCtrlAddDevice[0]
		src := data1["new"].(map[string]interface{})
		Expect(data.NetworkAddr).Should(Equal(src["networkAddr"]))
	}
	{
		Expect(len(handler.recvCtrlAddDeviceBulkTime)).ShouldNot(BeZero())
		Expect(handler.recvCtrlAddDeviceBulkTime[0]).Should(Equal(now))
		Expect(len(handler.recvCtrlAddDeviceBulk)).ShouldNot(BeZero())
		data := handler.recvCtrlAddDeviceBulk[0]
		src := data2["new"].(map[string]interface{})
		Expect(data.NetworkAddrs).Should(Equal(src["networkAddrs"]))
	}
	{
		Expect(len(handler.recvCtrlAddDeviceRangeTime)).ShouldNot(BeZero())
		Expect(handler.recvCtrlAddDeviceRangeTime[0]).Should(Equal(now))
		Expect(len(handler.recvCtrlAddDeviceRange)).ShouldNot(BeZero())
		data := handler.recvCtrlAddDeviceRange[0]
		src := data3["new"].(map[string]interface{})
		Expect(data.StartAddr).Should(Equal(src["startAddr"]))
		Expect(data.EndAddr).Should(Equal(src["endAddr"]))
	}
	{
		Expect(len(handler.recvCtrlDelDeviceTime)).ShouldNot(BeZero())
		Expect(handler.recvCtrlDelDeviceTime[0]).Should(Equal(now))
		Expect(len(handler.recvCtrlDelDevice)).ShouldNot(BeZero())
		data := handler.recvCtrlDelDevice[0]
		src := data4["new"].(map[string]interface{})
		Expect(data.NetworkAddr).Should(Equal(src["networkAddr"]))
	}
	{
		Expect(len(handler.recvCtrlDelDeviceBulkTime)).ShouldNot(BeZero())
		Expect(handler.recvCtrlDelDeviceBulkTime[0]).Should(Equal(now))
		Expect(len(handler.recvCtrlDelDeviceBulk)).ShouldNot(BeZero())
		data := handler.recvCtrlDelDeviceBulk[0]
		src := data5["new"].(map[string]interface{})
		Expect(data.NetworkAddrs).Should(Equal(src["networkAddrs"]))
	}
	{
		Expect(len(handler.recvCtrlDelDeviceRangeTime)).ShouldNot(BeZero())
		Expect(handler.recvCtrlDelDeviceRangeTime[0]).Should(Equal(now))
		Expect(len(handler.recvCtrlDelDeviceRange)).ShouldNot(BeZero())
		data := handler.recvCtrlDelDeviceRange[0]
		src := data6["new"].(map[string]interface{})
		Expect(data.StartAddr).Should(Equal(src["startAddr"]))
		Expect(data.EndAddr).Should(Equal(src["endAddr"]))
	}
}

// Test receiving ctrl with wrong content.
func netCtrlWrong() {
	testAppNetConn := newConnection(testMqEngine)

	hostUri, err := connHostUri(testMqEngine)
	Expect(err).ShouldNot(HaveOccurred())
	handler := &netTestHandler{}

	opts := mq.Options{
		UnitID:       "unit_id",
		UnitCode:     "unit_code",
		ID:           "id_network",
		Name:         "code_network",
		SharedPrefix: mqttSharedPrefix,
	}
	mgr, err := mq.NewNetworkMgr(testMgrConns, *hostUri, opts, handler)
	Expect(err).ShouldNot(HaveOccurred())
	testNetMgrs = append(testNetMgrs, mgr)

	var queue gmq.GmqQueue
	if conn, ok := testAppNetConn.(*gmq.AmqpConnection); ok {
		opts := gmq.AmqpQueueOptions{
			Name:      "broker.network.unit_code.code_network.ctrl",
			IsRecv:    false,
			Reliable:  true,
			Broadcast: false,
		}
		queue, err = gmq.NewAmqpQueue(opts, conn)
		Expect(err).ShouldNot(HaveOccurred())
	} else if conn, ok := testAppNetConn.(*gmq.MqttConnection); ok {
		opts := gmq.MqttQueueOptions{
			Name:      "broker.network.unit_code.code_network.ctrl",
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

	data := `{"time":"2022-20-29T11:45:00.000Z"}`
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg([]byte(data))
	Expect(err).ShouldNot(HaveOccurred())
	data = `{"time":"2022-10-29T11:45:00.000Z","operation":"add-device","new":1}`
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg([]byte(data))
	Expect(err).ShouldNot(HaveOccurred())
	data = `{"time":"2022-10-29T11:45:00.000Z","operation":"add-device-bulk","new":1}`
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg([]byte(data))
	Expect(err).ShouldNot(HaveOccurred())
	data = `{"time":"2022-10-29T11:45:00.000Z","operation":"add-device-range","new":1}`
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg([]byte(data))
	Expect(err).ShouldNot(HaveOccurred())
	data = `{"time":"2022-10-29T11:45:00.000Z","operation":"del-device","new":1}`
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg([]byte(data))
	Expect(err).ShouldNot(HaveOccurred())
	data = `{"time":"2022-10-29T11:45:00.000Z","operation":"del-device-bulk","new":1}`
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg([]byte(data))
	Expect(err).ShouldNot(HaveOccurred())
	data = `{"time":"2022-10-29T11:45:00.000Z","operation":"del-device-range","new":1}`
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg([]byte(data))
	Expect(err).ShouldNot(HaveOccurred())
	data = `{"time":"2022-10-29T11:45:00.000Z","operation":"not-exist,"new":{}}`
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.SendMsg([]byte(data))
	Expect(err).ShouldNot(HaveOccurred())

	time.Sleep(1 * time.Second)
	count := len(handler.recvCtrlAddDevice) + len(handler.recvCtrlAddDeviceBulk) +
		len(handler.recvCtrlAddDeviceRange) + len(handler.recvCtrlDelDevice) +
		len(handler.recvCtrlDelDeviceBulk) + len(handler.recvCtrlDelDeviceRange)
	Expect(count).Should(BeZero())
}
