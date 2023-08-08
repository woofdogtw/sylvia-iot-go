package tests

import (
	"testing"

	"github.com/novln/macchiato"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/woofdogtw/sylvia-iot-go/sdk/tests/api"
	"github.com/woofdogtw/sylvia-iot-go/sdk/tests/constants"
	"github.com/woofdogtw/sylvia-iot-go/sdk/tests/middlewares"
	"github.com/woofdogtw/sylvia-iot-go/sdk/tests/mq"
)

func TestIntegration(t *testing.T) {
	Describe("Integration test", func() {
		Describe("api", api.Suite())
		Describe("constants", constants.Suite())
		Describe("middlewares", middlewares.Suite())
		Describe("mq", mq.Suite(mq.EngineRabbitMQ))
		Describe("mq", mq.Suite(mq.EngineEMQX))
	})

	RegisterFailHandler(Fail)

	macchiato.RunSpecs(t, "")
}
