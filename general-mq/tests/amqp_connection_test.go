package tests

import (
	"time"

	. "github.com/onsi/gomega"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
)

// Test default options.
func amqpNewConnDefault() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
}

// Test options with wrong values.
func amqpNewConnWrongOpts() {
	_, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{
		URI: "amq://localhost",
	})
	Expect(err).Should(HaveOccurred())
}

// Test connection properties after `NewAmqpConnection()`.
func amqpConnProperties() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	Expect(conn.Status()).Should(Equal(gmq.Closed))
}

// Test `Connect()` without handlers.
func amqpConnConnectNoHandler() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	testConns = append(testConns, conn)

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	waitConnConnected(conn)
}

// Test `Connect()` with a handler.
func amqpConnConnectWithHandler() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	testConns = append(testConns, conn)

	handler := testConnConnectHandler{}
	_ = conn.AddHandler(&handler)

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if handler.recvConnected {
			break
		}
	}
	Expect(handler.recvConnected).Should(BeTrue())
}

// Test `Connect()` for a conneted connection.
func amqpConnConnectAfterConnect() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	testConns = append(testConns, conn)

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())
}

// Test remove handlers.
func amqpConnRemoveHandler() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	testConns = append(testConns, conn)

	handler := testConnRemoveHandler{}
	_ = conn.AddHandler(&handler)
	id := conn.AddHandler(&handler)
	conn.RemoveHandler(id)

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())

	time.Sleep(1 * time.Second)
	Expect(handler.connectedCount).Should(Equal(1))
}

// Test `Close()`.
func amqpConnClose() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	testConns = append(testConns, conn)

	handler := testConnCloseHandler{}
	_ = conn.AddHandler(&handler)

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	waitConnConnected(conn)

	err = conn.Close()
	Expect(err).ShouldNot(HaveOccurred())

	for retry := retry10Ms; retry > 0; retry-- {
		time.Sleep(10 * time.Millisecond)
		if handler.recvClosed {
			break
		}
	}
	Expect(handler.recvClosed).Should(BeTrue())
}

// Test `Close()` for a closed connection.
func amqpConnCloseAfterClose() {
	conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn).ShouldNot(BeNil())
	testConns = append(testConns, conn)

	err = conn.Connect()
	Expect(err).ShouldNot(HaveOccurred())
	waitConnConnected(conn)

	err = conn.Close()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn.Status()).Should(Equal(gmq.Closed))
	err = conn.Close()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(conn.Status()).Should(Equal(gmq.Closed))
}
