package main

import (
	"fmt"
	"os"
	"time"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
)

type connHandler struct{}
type queueHandler struct {
	name string
}

const (
	testBroadcast = true
	testReliable  = true
)

var _ gmq.ConnectionHandler = (*connHandler)(nil)
var _ gmq.QueueHandler = (*queueHandler)(nil)

func (c *connHandler) OnStatus(handlerID string, conn gmq.GmqConnection, status gmq.Status) {
	fmt.Printf("handlerID: %s, status: %s\n", handlerID, status)
}

func (c *connHandler) OnError(handlerID string, conn gmq.GmqConnection, err error) {
	fmt.Printf("handlerID: %s, error: %s\n", handlerID, err)
}

func (h *queueHandler) OnStatus(q gmq.GmqQueue, status gmq.Status) {
	fmt.Printf("name %s queue: %s, status: %s\n", h.name, q.Name(), status)
}

func (h *queueHandler) OnError(q gmq.GmqQueue, err error) {
	fmt.Printf("name %s queue: %s, error: %s\n", h.name, q.Name(), err)
}

func (h *queueHandler) OnMessage(q gmq.GmqQueue, message gmq.Message) {
	str := string(message.Payload())
	fmt.Printf("name %s queue %s received %s\n", h.name, q.Name(), str)
	err := message.Ack()
	if err != nil {
		fmt.Printf("name %s queue %s ack %s error: %s\n", h.name, q.Name(), str, err)
	} else {
		fmt.Printf("name %s queue %s ack %s ok\n", h.name, q.Name(), str)
	}
}

func main() {
	_, runMqtt := os.LookupEnv("RUN_MQTT")
	if runMqtt {
		fmt.Println("Run MQTT")
		testMqtt()
	} else {
		fmt.Println("Run AMQP")
		testAmqp()
	}
}

func testAmqp() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	if err != nil {
		fmt.Println("new AmqpConnection error: " + err.Error())
		return
	}
	conn.AddHandler(&connHandler{})
	conn.AddHandler(&connHandler{})

	opts := gmq.AmqpQueueOptions{
		Name:        "test",
		IsRecv:      false,
		Reliable:    testReliable,
		Broadcast:   testBroadcast,
		ReconnectMS: 1000,
		Prefetch:    10,
	}
	sendQueue, err := gmq.NewAmqpQueue(opts, conn)
	if err != nil {
		fmt.Println("new AmqpQueue error: " + err.Error())
		return
	}
	sendQueue.SetHandler(&queueHandler{name: "send"})
	if err := sendQueue.Connect(); err != nil {
		fmt.Println("connect send queue error: " + err.Error())
		return
	}

	opts = gmq.AmqpQueueOptions{
		Name:        "test",
		IsRecv:      true,
		Reliable:    testReliable,
		Broadcast:   testBroadcast,
		ReconnectMS: 1000,
		Prefetch:    10,
	}
	recvQueue1, err := gmq.NewAmqpQueue(opts, conn)
	if err != nil {
		fmt.Println("new AmqpQueue error: " + err.Error())
		return
	}
	recvQueue1.SetHandler(&queueHandler{name: "recv1"})
	if err := recvQueue1.Connect(); err != nil {
		fmt.Println("connect recv1 queue error: " + err.Error())
		return
	}
	recvQueue2, err := gmq.NewAmqpQueue(opts, conn)
	if err != nil {
		fmt.Println("new AmqpQueue error: " + err.Error())
		return
	}
	recvQueue2.SetHandler(&queueHandler{name: "recv2"})
	if err := recvQueue2.Connect(); err != nil {
		fmt.Println("connect recv2 queue error: " + err.Error())
		return
	}

	for {
		if err := conn.Connect(); err != nil {
			fmt.Println("connect error: " + err.Error())
			return
		}
		for count := 2; count > 0; count-- {
			time.Sleep(2 * time.Second)
			str := fmt.Sprintf("count %d", count)
			if err := sendQueue.SendMsg([]byte(str)); err != nil {
				fmt.Printf("send %s error: %s\n", str, err)
			} else {
				fmt.Printf("send %s ok\n", str)
			}
		}
		time.Sleep(2 * time.Second)
		if err := conn.Close(); err != nil {
			fmt.Println("close error: " + err.Error())
			return
		}
		time.Sleep(5 * time.Second)
	}
}

func testMqtt() {
	conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	if err != nil {
		fmt.Println("new MqttConnection error: " + err.Error())
		return
	}
	conn.AddHandler(&connHandler{})
	conn.AddHandler(&connHandler{})
	conn2, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
	if err != nil {
		fmt.Println("new MqttConnection error: " + err.Error())
		return
	}
	conn2.AddHandler(&connHandler{})

	opts := gmq.MqttQueueOptions{
		Name:         "test",
		IsRecv:       false,
		Reliable:     testReliable,
		Broadcast:    testBroadcast,
		ReconnectMS:  1000,
		SharedPrefix: "$share/general-mq/",
	}
	sendQueue, err := gmq.NewMqttQueue(opts, conn)
	if err != nil {
		fmt.Println("new MqttQueue error: " + err.Error())
		return
	}
	sendQueue.SetHandler(&queueHandler{name: "send"})
	if err := sendQueue.Connect(); err != nil {
		fmt.Println("connect send queue error: " + err.Error())
		return
	}

	opts = gmq.MqttQueueOptions{
		Name:         "test",
		IsRecv:       true,
		Reliable:     testReliable,
		Broadcast:    testBroadcast,
		ReconnectMS:  1000,
		SharedPrefix: "$share/general-mq/",
	}
	recvQueue1, err := gmq.NewMqttQueue(opts, conn)
	if err != nil {
		fmt.Println("new MqttQueue error: " + err.Error())
		return
	}
	recvQueue1.SetHandler(&queueHandler{name: "recv1"})
	if err := recvQueue1.Connect(); err != nil {
		fmt.Println("connect recv1 queue error: " + err.Error())
		return
	}
	recvQueue2, err := gmq.NewMqttQueue(opts, conn2)
	if err != nil {
		fmt.Println("new MqttQueue error: " + err.Error())
		return
	}
	recvQueue2.SetHandler(&queueHandler{name: "recv2"})
	if err := recvQueue2.Connect(); err != nil {
		fmt.Println("connect recv2 queue error: " + err.Error())
		return
	}

	for {
		if err := conn.Connect(); err != nil {
			fmt.Println("connect error: " + err.Error())
			return
		}
		if err := conn2.Connect(); err != nil {
			fmt.Println("connect 2 error: " + err.Error())
			return
		}
		for count := 2; count > 0; count-- {
			time.Sleep(2 * time.Second)
			str := fmt.Sprintf("count %d", count)
			if err := sendQueue.SendMsg([]byte(str)); err != nil {
				fmt.Printf("send %s error: %s\n", str, err)
			} else {
				fmt.Printf("send %s ok\n", str)
			}
		}
		time.Sleep(2 * time.Second)
		if err := conn.Close(); err != nil {
			fmt.Println("close error: " + err.Error())
			return
		}
		if err := conn2.Close(); err != nil {
			fmt.Println("close 2 error: " + err.Error())
			return
		}
		time.Sleep(5 * time.Second)
	}
}
