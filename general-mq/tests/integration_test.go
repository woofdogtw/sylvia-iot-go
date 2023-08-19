package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/novln/macchiato"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
)

type testConnConnectHandler struct {
	recvConnected bool
}

type testConnRemoveHandler struct {
	connectedCount int
	countMutex     sync.Mutex
}

type testConnCloseHandler struct {
	recvClosed bool
}

type testQueueConnectHandler struct {
	recvConnected bool
	recvQueueName string
}

type testQueueRemoveHandler struct {
	connectedCount int
	countMutex     sync.Mutex
}

type testQueueCloseHandler struct {
	recvClosed    bool
	recvQueueName string
}

type testQueueReconnectHandler struct {
	connectedCount int
	countMutex     sync.Mutex
	recvConneting  bool
}

type testQueueRecvMsgHandler struct {
	recvMessages      [][]byte
	recvMessagesMutex sync.Mutex
	ackErrors         []string
	ackErrorsMutex    sync.Mutex
	useNack           bool
	useNackMutex      sync.Mutex
	nackMessages      [][]byte
	nackMessagesMutex sync.Mutex
	nackErrors        []string
	nackErrorsMutex   sync.Mutex
}

var _ gmq.ConnectionHandler = (*testConnConnectHandler)(nil)
var _ gmq.ConnectionHandler = (*testConnRemoveHandler)(nil)
var _ gmq.ConnectionHandler = (*testConnCloseHandler)(nil)
var _ gmq.QueueEventHandler = (*testQueueConnectHandler)(nil)
var _ gmq.QueueEventHandler = (*testQueueRemoveHandler)(nil)
var _ gmq.QueueEventHandler = (*testQueueCloseHandler)(nil)
var _ gmq.QueueEventHandler = (*testQueueReconnectHandler)(nil)
var _ gmq.QueueEventHandler = (*testQueueRecvMsgHandler)(nil)
var _ gmq.QueueMessageHandler = (*testQueueConnectHandler)(nil)
var _ gmq.QueueMessageHandler = (*testQueueRemoveHandler)(nil)
var _ gmq.QueueMessageHandler = (*testQueueCloseHandler)(nil)
var _ gmq.QueueMessageHandler = (*testQueueReconnectHandler)(nil)
var _ gmq.QueueMessageHandler = (*testQueueRecvMsgHandler)(nil)

// Constants.
const (
	retry10Ms = 100
)

// Runtime variables to be cleared in After functions.
var (
	testConns  []gmq.GmqConnection
	testQueues []gmq.GmqQueue
)

func TestIntegration(t *testing.T) {
	Describe("Integration test", func() {
		It("strings", testStrings)
		Describe("amqp", amqpSuite)
		Describe("mqtt", mqttSuite)
	})

	RegisterFailHandler(Fail)

	macchiato.RunSpecs(t, "")
}

func testStrings() {
	var err gmq.GmqError = -1
	Expect(err.String()).Should(Equal("unknown error"))
	Expect(gmq.NotConnected.String()).Should(Equal("not connected"))
	Expect(gmq.QueueIsReceiver.String()).Should(Equal("this queue is a receiver"))

	var status gmq.Status = -1
	Expect(status.String()).Should(Equal("unknown"))
	Expect(gmq.Closing.String()).Should(Equal("closing"))
	Expect(gmq.Closed.String()).Should(Equal("closed"))
	Expect(gmq.Connecting.String()).Should(Equal("connecting"))
	Expect(gmq.Connected.String()).Should(Equal("connected"))
	Expect(gmq.Disconnected.String()).Should(Equal("disconnected"))
}

func clearState() {
	for _, q := range testQueues {
		_ = q.Close()
	}
	testQueues = []gmq.GmqQueue{}
	for _, c := range testConns {
		_ = c.Close()
	}
	testConns = []gmq.GmqConnection{}
}

func waitConnConnected(c gmq.GmqConnection) {
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if c.Status() == gmq.Connected {
			break
		}
	}
	Expect(c.Status()).Should(Equal(gmq.Connected))
}

func waitQueueConnected(q gmq.GmqQueue) {
	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if q.Status() == gmq.Connected {
			break
		}
	}
	Expect(q.Status()).Should(Equal(gmq.Connected))
}

func getMessage(messages [][]byte, index int) (string, error) {
	if index >= len(messages) {
		return "", fmt.Errorf("messages[%d] get none", index)
	}
	msg := string(messages[index])
	if len(msg) != len(messages[index]) {
		return "", fmt.Errorf("messages[%d] to string error", index)
	}
	return msg, nil
}

func (h *testConnConnectHandler) OnStatus(
	handlerID string, conn gmq.GmqConnection, status gmq.Status) {
	if status == gmq.Connected {
		h.recvConnected = true
	}
}

func (h *testConnConnectHandler) OnError(handlerID string, conn gmq.GmqConnection, err error) {}

func (h *testConnRemoveHandler) OnStatus(
	handlerID string, conn gmq.GmqConnection, status gmq.Status) {
	if status == gmq.Connected {
		h.countMutex.Lock()
		h.connectedCount++
		h.countMutex.Unlock()
	}
}

func (h *testConnRemoveHandler) OnError(handlerID string, conn gmq.GmqConnection, err error) {}

func (h *testConnCloseHandler) OnStatus(
	handlerID string, conn gmq.GmqConnection, status gmq.Status) {
	if status == gmq.Closed {
		h.recvClosed = true
	}
}

func (h *testConnCloseHandler) OnError(handlerID string, conn gmq.GmqConnection, err error) {}

func (h *testQueueConnectHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	if status == gmq.Connected {
		h.recvConnected = true
		h.recvQueueName = queue.Name()
	}
}

func (h *testQueueConnectHandler) OnError(queue gmq.GmqQueue, err error) {}

func (h *testQueueConnectHandler) OnMessage(queue gmq.GmqQueue, msg gmq.Message) {}

func (h *testQueueRemoveHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	if status == gmq.Connected {
		h.countMutex.Lock()
		h.connectedCount++
		h.countMutex.Unlock()
	}
}

func (h *testQueueRemoveHandler) OnError(queue gmq.GmqQueue, err error) {}

func (h *testQueueRemoveHandler) OnMessage(queue gmq.GmqQueue, msg gmq.Message) {}

func (h *testQueueCloseHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	if status == gmq.Closed {
		h.recvClosed = true
		h.recvQueueName = queue.Name()
	}
}

func (h *testQueueCloseHandler) OnError(queue gmq.GmqQueue, err error) {}

func (h *testQueueCloseHandler) OnMessage(queue gmq.GmqQueue, msg gmq.Message) {}

func (h *testQueueReconnectHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {
	if status == gmq.Connected {
		h.countMutex.Lock()
		h.connectedCount++
		h.countMutex.Unlock()
	} else if status == gmq.Connecting {
		h.recvConneting = true
	}
}

func (h *testQueueReconnectHandler) OnError(queue gmq.GmqQueue, err error) {}

func (h *testQueueReconnectHandler) OnMessage(queue gmq.GmqQueue, msg gmq.Message) {}

func (h *testQueueRecvMsgHandler) OnStatus(queue gmq.GmqQueue, status gmq.Status) {}

func (h *testQueueRecvMsgHandler) OnError(queue gmq.GmqQueue, err error) {}

func (h *testQueueRecvMsgHandler) OnMessage(queue gmq.GmqQueue, msg gmq.Message) {
	var useNack bool
	h.useNackMutex.Lock()
	useNack = h.useNack
	h.useNackMutex.Unlock()

	if useNack {
		if err := msg.Nack(); err != nil {
			h.nackErrorsMutex.Lock()
			h.nackErrors = append(h.nackErrors, err.Error())
			h.nackErrorsMutex.Unlock()
		} else {
			data := msg.Payload()
			h.nackMessagesMutex.Lock()
			h.nackMessages = append(h.nackMessages, data)
			h.nackMessagesMutex.Unlock()
		}
	} else {
		if err := msg.Ack(); err != nil {
			h.ackErrorsMutex.Lock()
			h.ackErrors = append(h.ackErrors, err.Error())
			h.ackErrorsMutex.Unlock()
		} else {
			data := msg.Payload()
			h.recvMessagesMutex.Lock()
			h.recvMessages = append(h.recvMessages, data)
			h.recvMessagesMutex.Unlock()
		}
	}
}
