package tests

import (
	"fmt"
	"time"

	randomString "github.com/delphinus/random-string"
	. "github.com/onsi/gomega"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
)

type mqttQueueResources struct {
	conn   []*gmq.MqttConnection
	queues []*gmq.MqttQueue
}

// Test default options.
func mqttNewQueueDefault() {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())

	opts := gmq.MqttQueueOptions{
		Name: "name",
	}
	queue, err := gmq.NewMqttQueue(opts, conn)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue).ShouldNot(BeNil())
}

// Test options with wrong values.
func mqttNewQueueWrongOpts() {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())

	opts := gmq.MqttQueueOptions{}
	_, err = gmq.NewMqttQueue(opts, nil)
	Expect(err).Should(HaveOccurred())

	opts = gmq.MqttQueueOptions{}
	_, err = gmq.NewMqttQueue(opts, conn)
	Expect(err).Should(HaveOccurred())

	opts = gmq.MqttQueueOptions{
		Name: "A@",
	}
	_, err = gmq.NewMqttQueue(opts, conn)
	Expect(err).Should(HaveOccurred())
}

// Test queue properties after `NewMqttQueue()`.
func mqttQueueProperties() {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())

	opts := gmq.MqttQueueOptions{
		Name: "name-send",
	}
	queue, err := gmq.NewMqttQueue(opts, conn)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue).ShouldNot(BeNil())
	Expect(queue.Name()).Should(Equal(opts.Name))
	Expect(queue.IsRecv()).Should(BeFalse())
	Expect(queue.Status()).Should(Equal(gmq.Closed))

	opts = gmq.MqttQueueOptions{
		Name:   "name-recv",
		IsRecv: true,
	}
	queue, err = gmq.NewMqttQueue(opts, conn)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue).ShouldNot(BeNil())
	Expect(queue.Name()).Should(Equal(opts.Name))
	Expect(queue.IsRecv()).Should(BeTrue())
	Expect(queue.Status()).Should(Equal(gmq.Closed))
}

// Test `Connect()` without handlers.
func mqttQueueConnectNoHandler() {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	testConns = append(testConns, conn)
	opts := gmq.MqttQueueOptions{
		Name:   "name",
		IsRecv: true,
	}
	queue, err := gmq.NewMqttQueue(opts, conn)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue).ShouldNot(BeNil())
	testQueues = append(testQueues, queue)
	queue.SetMsgHandler(&testQueueConnectHandler{})

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	waitQueueConnected(queue)
}

// Test `Connect()` with a handler.
func mqttQueueConnectWithHandler() {
	resources := mqttQueueResources{}

	handler := testQueueConnectHandler{}
	err := createMqttConnRsc(&resources, &handler, &handler, true)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}

	for retry := retry10Ms; retry > 0; retry-- {
		if handler.recvConnected && handler.recvQueueName == "name" {
			return
		}
	}
	panic("not connected")
}

// Test `Connect()` for a conneted queue.
func mqttQueueConnectAfterConnect() {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	testConns = append(testConns, conn)
	opts := gmq.MqttQueueOptions{
		Name:   "name",
		IsRecv: true,
	}
	queue, err := gmq.NewMqttQueue(opts, conn)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue).ShouldNot(BeNil())
	testQueues = append(testQueues, queue)
	queue.SetMsgHandler(&testQueueConnectHandler{})

	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())
}

// Test remove the handler.
func mqttQueueClearHandler() {
	resources := mqttQueueResources{}

	handler := testQueueRemoveHandler{}
	err := createMqttConnRsc(&resources, &handler, &handler, true)
	Expect(err).ShouldNot(HaveOccurred())

	Expect(len(resources.conn) > 0).Should(BeTrue())
	conn := resources.conn[0]
	Expect(len(resources.queues) > 0).Should(BeTrue())
	queue := resources.queues[0]
	queue.SetHandler(nil)

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	time.Sleep(1 * time.Second)
	Expect(handler.connectedCount).Should(Equal(0))
}

// Test `Close()`.
func mqttQueueClose() {
	resources := mqttQueueResources{}

	handler := testQueueCloseHandler{}
	err := createMqttConnRsc(&resources, &handler, &handler, true)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}

	Expect(len(resources.queues) > 0).Should(BeTrue())
	queue := resources.queues[0]

	err = queue.Close()
	Expect(err).ShouldNot(HaveOccurred())
	for retry := retry10Ms; retry > 0; retry-- {
		if handler.recvClosed && handler.recvQueueName == "name" {
			return
		}
	}
	panic("not connected")
}

// Test `close()` for a closed queue.
func mqttQueueCloseAfterClose() {
	resources := mqttQueueResources{}

	handler := testQueueCloseHandler{}
	err := createMqttConnRsc(&resources, &handler, &handler, true)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}

	Expect(len(resources.queues) > 0).Should(BeTrue())
	queue := resources.queues[0]

	err = queue.Close()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue.Status()).Should(Equal(gmq.Closed))
	err = queue.Close()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue.Status()).Should(Equal(gmq.Closed))
}

// Test send with an invalid queue.
func mqttQueueSendError() {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())

	opts := gmq.MqttQueueOptions{
		Name:   "name",
		IsRecv: true,
	}
	queue, err := gmq.NewMqttQueue(opts, conn)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue).ShouldNot(BeNil())

	err = queue.Connect()
	Expect(err).Should(HaveOccurred())

	err = queue.SetMsgHandler(nil)
	Expect(err).Should(HaveOccurred())

	err = queue.SendMsg([]byte(""))
	Expect(err).Should(HaveOccurred())

	opts = gmq.MqttQueueOptions{
		Name:   "name",
		IsRecv: false,
	}
	queue, err = gmq.NewMqttQueue(opts, conn)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(queue).ShouldNot(BeNil())

	err = queue.SendMsg([]byte(""))
	Expect(err).Should(HaveOccurred())
}

// Test reconnect by closing/connecting the associated connection.
func mqttScenarioReconnect() {
	resources := mqttQueueResources{}

	handler := testQueueReconnectHandler{}
	err := createMqttConnRsc(&resources, &handler, &handler, true)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}

	Expect(len(resources.conn) > 0).Should(BeTrue())
	conn := resources.conn[0]
	Expect(len(resources.queues) > 0).Should(BeTrue())
	queue := resources.queues[0]

	err = conn.Close()
	Expect(err).ShouldNot(HaveOccurred())
	recvConnecting := false
	for retry := 200; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if handler.recvConneting {
			recvConnecting = true
			break
		}
	}
	Expect(recvConnecting).Should(BeTrue())

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	waitQueueConnected(queue)
}

// Send unicast data to one receiver.
func mqttScenarioDataUnicast1to1() {
	resources := mqttQueueResources{}

	opts := gmq.MqttQueueOptions{
		Name:         "name",
		SharedPrefix: "$share/general-mq/",
	}
	handlers, err := createMqttMsgRsc(&resources, opts, 1)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}

	Expect(len(resources.queues) > 0).Should(BeTrue())
	sendQueue := resources.queues[0]
	Expect(len(handlers) > 0).Should(BeTrue())
	handler := handlers[0]

	dataset := []string{"1", "2"}
	for _, data := range dataset {
		dataBytes := []byte(data)
		go func() {
			err := sendQueue.SendMsg(dataBytes)
			if err != nil {
				fmt.Printf("send error: %s\n", err)
			}
		}()
	}

	recvLen := 0
	retry := 150
	for ; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		recvLen := len(handler.recvMessages)
		if recvLen == 2 {
			break
		}
	}
	Expect(retry == 0).Should(BeFalse(), fmt.Sprintf("received %d/2 messages", recvLen))
	var msg1 string
	var msg2 string
	msg1, err = getMessage(handler.recvMessages, 0)
	Expect(err).ShouldNot(HaveOccurred())
	msg2, err = getMessage(handler.recvMessages, 1)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(msg1 == msg2).ShouldNot(BeTrue(), "duplicate message")
}

// Send unicast data to 3 receivers.
func mqttScenarioDataUnicast1to3() {
	resources := mqttQueueResources{}

	opts := gmq.MqttQueueOptions{
		Name:         "name",
		SharedPrefix: "$share/general-mq/",
	}
	handlers, err := createMqttMsgRsc(&resources, opts, 3)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}
	Expect(len(resources.queues) > 0).Should(BeTrue())
	sendQueue := resources.queues[0]
	Expect(len(handlers) >= 3).Should(BeTrue())
	handler1 := handlers[0]
	handler2 := handlers[1]
	handler3 := handlers[2]

	dataset := []string{"1", "2", "3", "4", "5", "6"}
	for _, data := range dataset {
		dataBytes := []byte(data)
		go func() {
			err := sendQueue.SendMsg(dataBytes)
			if err != nil {
				fmt.Printf("send error: %s\n", err)
			}
		}()
	}

	recvLen := 0
	retry := 150
	for ; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		recvLen := len(handler1.recvMessages) + len(handler2.recvMessages) +
			len(handler3.recvMessages)
		if recvLen == 6 {
			break
		}
	}
	Expect(retry == 0).Should(BeFalse(), fmt.Sprintf("received %d/6 messages", recvLen))
	allMsg := []string{}
	for i := range handler1.recvMessages {
		str, err := getMessage(handler1.recvMessages, i)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(allMsg).ShouldNot(ContainElement(str))
		allMsg = append(allMsg, str)
	}
	for i := range handler2.recvMessages {
		str, err := getMessage(handler2.recvMessages, i)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(allMsg).ShouldNot(ContainElement(str))
		allMsg = append(allMsg, str)
	}
	for i := range handler3.recvMessages {
		str, err := getMessage(handler3.recvMessages, i)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(allMsg).ShouldNot(ContainElement(str))
		allMsg = append(allMsg, str)
	}
}

// Send broadcast data to one receiver.
func mqttScenarioDataBroadcast1to1() {
	resources := mqttQueueResources{}

	opts := gmq.MqttQueueOptions{
		Name:      "name",
		Broadcast: true,
	}
	handlers, err := createMqttMsgRsc(&resources, opts, 1)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}

	Expect(len(resources.queues) > 0).Should(BeTrue())
	sendQueue := resources.queues[0]
	Expect(len(handlers) > 0).Should(BeTrue())
	handler := handlers[0]

	dataset := []string{"1", "2"}
	for _, data := range dataset {
		dataBytes := []byte(data)
		go func() {
			err := sendQueue.SendMsg(dataBytes)
			if err != nil {
				fmt.Printf("send error: %s\n", err)
			}
		}()
	}

	recvLen := 0
	retry := 150
	for ; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		recvLen := len(handler.recvMessages)
		if recvLen == 2 {
			break
		}
	}
	Expect(retry == 0).Should(BeFalse(), fmt.Sprintf("received %d/2 messages", recvLen))
	var msg1 string
	var msg2 string
	msg1, err = getMessage(handler.recvMessages, 0)
	Expect(err).ShouldNot(HaveOccurred())
	msg2, err = getMessage(handler.recvMessages, 1)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(msg1 == msg2).ShouldNot(BeTrue(), "duplicate message")
}

// Send broadcast data to 3 receivers.
func mqttScenarioDataBroadcast1to3() {
	resources := mqttQueueResources{}

	opts := gmq.MqttQueueOptions{
		Name:      "name",
		Broadcast: true,
	}
	handlers, err := createMqttMsgRsc(&resources, opts, 3)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}
	Expect(len(resources.queues) > 0).Should(BeTrue())
	sendQueue := resources.queues[0]
	Expect(len(handlers) >= 3).Should(BeTrue())
	handler1 := handlers[0]
	handler2 := handlers[1]
	handler3 := handlers[2]

	dataset := []string{"1", "2"}
	for _, data := range dataset {
		dataBytes := []byte(data)
		go func() {
			err := sendQueue.SendMsg(dataBytes)
			if err != nil {
				fmt.Printf("send error: %s\n", err)
			}
		}()
	}

	recvLen := 0
	retry := 150
	len1 := 0
	len2 := 0
	len3 := 0
	for ; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		len1 = len(handler1.recvMessages)
		len2 = len(handler2.recvMessages)
		len3 = len(handler3.recvMessages)
		recvLen := len1 + len2 + len3
		if recvLen == 6 {
			break
		}
	}
	Expect(retry == 0).Should(BeFalse(), fmt.Sprintf("received %d/6 messages", recvLen))
	Expect(len1 != len2 || len2 != len3).ShouldNot(BeTrue(), "receive count not all 2")
	msg1, err := getMessage(handler1.recvMessages, 0)
	Expect(err).ShouldNot(HaveOccurred())
	msg2, err := getMessage(handler1.recvMessages, 1)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(msg1 == msg2).Should(BeFalse(), "duplicate message handler 1")
	msg1, err = getMessage(handler2.recvMessages, 0)
	Expect(err).ShouldNot(HaveOccurred())
	msg2, err = getMessage(handler2.recvMessages, 1)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(msg1 == msg2).Should(BeFalse(), "duplicate message handler 2")
	msg1, err = getMessage(handler3.recvMessages, 0)
	Expect(err).ShouldNot(HaveOccurred())
	msg2, err = getMessage(handler3.recvMessages, 1)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(msg1 == msg2).Should(BeFalse(), "duplicate message handler 3")
}

// Send reliable data by sending data to a closed queue then it will receive after connecting.
func mqttScenarioDataReliable() {
	resources := mqttQueueResources{}

	opts := gmq.MqttQueueOptions{
		Name:     "name",
		Reliable: true,
	}
	handlers, err := createMqttMsgRsc(&resources, opts, 1)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}

	Expect(len(handlers) > 0).Should(BeTrue())
	handler := handlers[0]

	Expect(len(resources.queues) > 0).Should(BeTrue())
	queue := resources.queues[0]
	err = queue.SendMsg([]byte("1"))
	Expect(err).ShouldNot(HaveOccurred())
	retry := 150
	for ; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		recvLen := len(handler.recvMessages)
		if recvLen == 1 {
			msg, err := getMessage(handler.recvMessages, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(msg).Should(Equal("1"))
			break
		}
	}
	Expect(retry == 0).ShouldNot(BeTrue())

	Expect(len(resources.queues) >= 2).Should(BeTrue())
	queue = resources.queues[1]
	err = queue.Close()
	Expect(err).ShouldNot(HaveOccurred())
	queue = resources.queues[0]
	err = queue.SendMsg([]byte("2"))
	Expect(err).ShouldNot(HaveOccurred())
	queue = resources.queues[1]
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	retry = 150
	for ; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		recvLen := len(handler.recvMessages)
		if recvLen == 2 {
			msg, err := getMessage(handler.recvMessages, 1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(msg).Should(Equal("2"))
			break
		}
	}
	Expect(retry == 0).ShouldNot(BeTrue())
}

// Send unreliable data by sending data to a closed queue then it MAY receive after connecting.
func mqttScenarioDataBestEffort() {
	resources := mqttQueueResources{}

	opts := gmq.MqttQueueOptions{
		Name:     "name",
		Reliable: false,
	}
	handlers, err := createMqttMsgRsc(&resources, opts, 1)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range resources.queues {
		waitQueueConnected(queue)
	}

	Expect(len(handlers) > 0).Should(BeTrue())
	handler := handlers[0]

	Expect(len(resources.queues) > 0).Should(BeTrue())
	queue := resources.queues[0]
	err = queue.SendMsg([]byte("1"))
	Expect(err).ShouldNot(HaveOccurred())
	retry := 150
	for ; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		recvLen := len(handler.recvMessages)
		if recvLen == 1 {
			msg, err := getMessage(handler.recvMessages, 0)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(msg).Should(Equal("1"))
			break
		}
	}
	Expect(retry == 0).ShouldNot(BeTrue())

	Expect(len(resources.queues) >= 2).Should(BeTrue())
	queue = resources.queues[1]
	err = queue.Close()
	Expect(err).ShouldNot(HaveOccurred())
	queue = resources.queues[0]
	err = queue.SendMsg([]byte("2"))
	Expect(err).ShouldNot(HaveOccurred())
	queue = resources.queues[1]
	err = queue.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	retry = 150
	for ; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		recvLen := len(handler.recvMessages)
		if recvLen == 2 {
			msg, err := getMessage(handler.recvMessages, 1)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(msg).Should(Equal("2"))
			break
		}
	}
}

// Create connected (optional) connections/queues for testing connections.
func createMqttConnRsc(
	resources *mqttQueueResources,
	handler gmq.QueueEventHandler,
	msgHandler gmq.QueueMessageHandler,
	connect bool,
) error {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	if err != nil {
		return err
	}
	testConns = append(testConns, conn)
	resources.conn = append(resources.conn, conn)
	opts := gmq.MqttQueueOptions{
		Name:   "name",
		IsRecv: true,
	}
	queue, err := gmq.NewMqttQueue(opts, conn)
	if err != nil {
		return err
	}
	testQueues = append(testQueues, queue)
	resources.queues = append(resources.queues, queue)

	if handler != nil {
		queue.SetHandler(handler)
	}
	if err := queue.SetMsgHandler(msgHandler); err != nil {
		return err
	}

	if !connect {
		return nil
	}
	if err := conn.Connect(); err != nil {
		return err
	}
	if err := queue.Connect(); err != nil {
		return err
	}
	return nil
}

// Create connected (optional) connections/queues for testing messages.
func createMqttMsgRsc(
	resources *mqttQueueResources,
	opts gmq.MqttQueueOptions,
	receiverCount int,
) ([]*testQueueRecvMsgHandler, error) {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{
		ClientID: fmt.Sprintf("sender-%s", randomString.Generate(8)),
	})
	if err != nil {
		return nil, err
	}
	testConns = append(testConns, conn)
	resources.conn = append(resources.conn, conn)
	sendOpts := opts
	sendOpts.IsRecv = false
	queue, err := gmq.NewMqttQueue(sendOpts, conn)
	if err != nil {
		return nil, err
	}
	testQueues = append(testQueues, queue)
	resources.queues = append(resources.queues, queue)

	retHandlers := []*testQueueRecvMsgHandler{}
	for i := 0; i < receiverCount; i++ {
		var clientID string
		if opts.Reliable {
			clientID = fmt.Sprintf("receiver-%d", i)
		} else {
			clientID = fmt.Sprintf("receiver-%d-%s", i, randomString.Generate(8))
		}
		conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{
			ClientID:        clientID,
			NotCleanSession: opts.Reliable,
		})
		if err != nil {
			return nil, err
		}
		testConns = append(testConns, conn)
		resources.conn = append(resources.conn, conn)
		recvOpts := opts
		recvOpts.IsRecv = true
		queue, err := gmq.NewMqttQueue(recvOpts, conn)
		if err != nil {
			return nil, err
		}
		testQueues = append(testQueues, queue)
		resources.queues = append(resources.queues, queue)

		handler := &testQueueRecvMsgHandler{
			recvMessages: [][]byte{},
			ackErrors:    []string{},
			nackMessages: [][]byte{},
			nackErrors:   []string{},
		}
		queue.SetHandler(handler)
		queue.SetMsgHandler(handler)
		retHandlers = append(retHandlers, handler)
	}

	for _, conn := range resources.conn {
		if err := conn.Connect(); err != nil {
			return nil, err
		}
	}
	for _, queue := range resources.queues {
		if err := queue.Connect(); err != nil {
			return nil, err
		}
	}
	return retHandlers, nil
}
