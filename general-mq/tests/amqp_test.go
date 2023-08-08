package tests

import (
	"encoding/json"
	"io"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func amqpSuite() {
	Describe("AmqpConnection", func() {
		It("NewAmqpConnection() with default", amqpNewConnDefault)
		It("NewAmqpConnection() with wrong options", amqpNewConnWrongOpts)

		It("Status()", amqpConnProperties)

		It("Connect() without handler", amqpConnConnectNoHandler)
		It("Connect() with handler", amqpConnConnectWithHandler)
		It("Connect() after Connect()", amqpConnConnectAfterConnect)

		It("RemoveHandler()", amqpConnRemoveHandler)

		It("Close()", amqpConnClose)
		It("Close() after Close()", amqpConnCloseAfterClose)

		caseCount := 9
		clearDone := false
		AfterEach(func() {
			clearState()

			caseCount--
			if caseCount <= 0 && !clearDone {
				clearDone = true
				removeRabbitmqQueues()
			}
		})
	})

	Describe("AmqpQueue", func() {
		It("NewAmqpQueue() with default", amqpNewQueueDefault)
		It("NewAmqpQueue() with wrong options", amqpNewQueueWrongOpts)

		It("Name(), IsRecv(), Status()", amqpQueueProperties)

		It("Connect() without handler", amqpQueueConnectNoHandler)
		It("Connect() with handler", amqpQueueConnectWithHandler)
		It("Connect() after Connect()", amqpQueueConnectAfterConnect)

		It("ClearHandler()", amqpQueueClearHandler)

		It("Close()", amqpQueueClose)
		It("Close() after Close()", amqpQueueCloseAfterClose)

		It("SendMsg() with error conditions", amqpQueueSendError)

		caseCount := 10
		clearDone := false
		AfterEach(func() {
			clearState()

			caseCount--
			if caseCount <= 0 && !clearDone {
				clearDone = true
				removeRabbitmqQueues()
			}
		})
	})

	Describe("Scenarios", func() {
		It("reconnect", amqpScenarioReconnect)

		It("unicast 1 to 1", amqpScenarioDataUnicast1to1)
		It("unicast 1 to 3", amqpScenarioDataUnicast1to3)

		It("broadcast 1 to 1", amqpScenarioDataBroadcast1to1)
		It("broadcast 1 to 3", amqpScenarioDataBroadcast1to3)

		It("reliable", amqpScenarioDataReliable)
		It("best effort", amqpScenarioDataBestEffort)

		It("nack", amqpScenarioDataNack)

		caseCount := 8
		clearDone := false
		AfterEach(func() {
			clearState()

			caseCount--
			if caseCount <= 0 && !clearDone {
				clearDone = true
				removeRabbitmqQueues()
			}
		})
	})
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
