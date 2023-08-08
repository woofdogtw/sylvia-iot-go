package mq

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	gmq "github.com/woofdogtw/sylvia-iot-go/general-mq"
	"github.com/woofdogtw/sylvia-iot-go/sdk/mq"
)

type MqEngine int

const (
	EngineRabbitMQ MqEngine = iota
	EngineEMQX

	retry10Ms        = 100
	mqttSharedPrefix = "$share/sylvia-iot-sdk/"
	timeFormat       = "2006-01-02T15:04:05.999Z"
)

var (
	testMqEngine     MqEngine
	testMgrConns     = mq.NewConnectionPool()
	testAppMgrs      []*mq.ApplicationMgr
	testNetMgrs      []*mq.NetworkMgr
	testAppNetConn   gmq.GmqConnection // application/network side connection.
	testAppNetQueues []gmq.GmqQueue    // application/network side queues.
)

func (e MqEngine) String() string {
	switch e {
	case EngineEMQX:
		return "emqx"
	case EngineRabbitMQ:
		return "rabbitmq"
	}
	return "unknown"
}

func Suite(mqEngine MqEngine) func() {
	return func() {
		Describe(fmt.Sprintf("ApplicationMgr - %s", mqEngine), func() {
			BeforeEach(func() {
				testMqEngine = mqEngine
			})

			It("NewApplicationMgr() with default options", appNewDefault)
			It("NewApplicationMgr() with manual options", appNewManual)
			It("NewApplicationMgr() with wrong options", appNewWrongOpts)
			It("Close()", appClose)

			It("uldata", appUlData)
			It("uldata with wrong content", appUlDataWrong)
			It("dldata", appDlData)
			It("dldata with wrong content", appDlDataWrong)
			It("dldata-resp", appDlDataResp)
			It("dldata-resp with wrong content", appDlDataRespWrong)
			It("dldata-result", appDlDataResult)
			It("dldata-result with wrong content", appDlDataResultWrong)

			AfterEach(afterEachFn)
		})

		Describe(fmt.Sprintf("NetworkMgr - %s", mqEngine), func() {
			BeforeEach(func() {
				testMqEngine = mqEngine
			})

			It("NewNetworkMgr() with default options", netNewDefault)
			It("NewNetworkMgr() with manual options", netNewManual)
			It("NewNetworkMgr() with wrong options", netNewWrongOpts)
			It("Close()", netClose)

			It("uldata", netUlData)
			It("uldata with wrong content", netUlDataWrong)
			It("dldata", netDlData)
			It("dldata with wrong content", netDlDataWrong)
			It("dldata-result", netDlDataResult)
			It("dldata-result with wrong content", netDlDataResultWrong)
			It("ctrl", netCtrl)
			It("ctrl with wrong content", netCtrlWrong)

			AfterEach(afterEachFn)
		})
	}
}

func afterEachFn() {
	for i := range testAppMgrs {
		_ = testAppMgrs[i].Close()
	}
	testAppMgrs = nil

	for i := range testNetMgrs {
		_ = testNetMgrs[i].Close()
	}
	testNetMgrs = nil

	if testMgrConns != nil {
		testMgrConns.ForceClear()
	}
	testMgrConns = mq.NewConnectionPool()

	for i := range testAppNetQueues {
		_ = testAppNetQueues[i].Close()
	}
	testAppNetQueues = nil

	if testAppNetConn != nil {
		_ = testAppNetConn.Close()
	}
	testAppNetConn = nil

	if testMqEngine == EngineRabbitMQ {
		removeRabbitmqQueues()
	}
}

func newConnection(mqEngine MqEngine) gmq.GmqConnection {
	switch mqEngine {
	case EngineRabbitMQ:
		conn, err := gmq.NewAmqpConnection(gmq.AmqpConnectionOptions{})
		Expect(err).ShouldNot(HaveOccurred())
		conn.Connect()
		waitConnConnected(conn)
		return conn
	case EngineEMQX:
		conn, err := gmq.NewMqttConnection(gmq.MqttConnectionOptions{})
		Expect(err).ShouldNot(HaveOccurred())
		conn.Connect()
		waitConnConnected(conn)
		return conn
	default:
		panic("unsupport mq_engine")
	}
}

func connHostUri(mqEngine MqEngine) (*url.URL, error) {
	switch mqEngine {
	case EngineRabbitMQ:
		return url.Parse("amqp://localhost")
	case EngineEMQX:
		return url.Parse("mqtt://localhost")
	default:
		return nil, errors.New("unsupport mq_engine")
	}
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

func removeRabbitmqQueues() {
	client := http.Client{}
	baseUrl := "http://guest:guest@localhost:15672/api/queues/%2f"

	req, err := http.NewRequest(http.MethodGet, baseUrl, nil)
	Expect(err).ShouldNot(HaveOccurred())
	res, err := client.Do(req)
	Expect(err).ShouldNot(HaveOccurred())
	defer res.Body.Close()
	Expect(res.StatusCode).Should(Equal(http.StatusOK))
	bodyBytes, err := io.ReadAll(res.Body)
	Expect(err).ShouldNot(HaveOccurred())
	var queues []struct {
		Name string `json:"name"`
	}
	err = json.Unmarshal(bodyBytes, &queues)
	Expect(err).ShouldNot(HaveOccurred())

	for _, queue := range queues {
		req, err = http.NewRequest(http.MethodDelete, baseUrl+"/"+queue.Name, nil)
		Expect(err).ShouldNot(HaveOccurred())
		res, err := client.Do(req)
		Expect(err).ShouldNot(HaveOccurred())
		defer res.Body.Close()
	}
}
