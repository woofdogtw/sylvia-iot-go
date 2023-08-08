package tests

import (
	. "github.com/onsi/ginkgo"
)

func mqttSuite() {
	Describe("MqttConnection", func() {
		It("NewMqttConnection() with default", mqttNewConnDefault)
		It("NewMqttConnection() with wrong options", mqttNewConnWrongOpts)

		It("Status()", mqttConnProperties)

		It("Connect() without handler", mqttConnConnectNoHandler)
		It("Connect() with handler", mqttConnConnectWithHandler)
		It("Connect() after Connect()", mqttConnConnectAfterConnect)

		It("RemoveHandler()", mqttConnRemoveHandler)

		It("Close()", mqttConnClose)
		It("Close() after Close()", mqttConnCloseAfterClose)

		AfterEach(clearState)
	})

	Describe("MqttQueue", func() {
		It("NewMqttQueue() with default", mqttNewQueueDefault)
		It("NewMqttQueue() with wrong options", mqttNewQueueWrongOpts)

		It("Name(), IsRecv(), Status()", mqttQueueProperties)

		It("Connect() without handler", mqttQueueConnectNoHandler)
		It("Connect() with handler", mqttQueueConnectWithHandler)
		It("Connect() after Connect()", mqttQueueConnectAfterConnect)

		It("ClearHandler()", mqttQueueClearHandler)

		It("Close()", mqttQueueClose)
		It("Close() after Close()", mqttQueueCloseAfterClose)

		It("SendMsg() with error conditions", mqttQueueSendError)

		AfterEach(clearState)
	})

	Describe("Scenarios", func() {
		It("reconnect", mqttScenarioReconnect)

		It("unicast 1 to 1", mqttScenarioDataUnicast1to1)
		It("unicast 1 to 3", mqttScenarioDataUnicast1to3)

		It("broadcast 1 to 1", mqttScenarioDataBroadcast1to1)
		It("broadcast 1 to 3", mqttScenarioDataBroadcast1to3)

		XIt("reliable", mqttScenarioDataReliable)
		It("best effort", mqttScenarioDataBestEffort)

		AfterEach(clearState)
	})
}
