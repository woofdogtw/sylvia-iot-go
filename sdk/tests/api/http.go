package api

import (
	"net/http"

	. "github.com/onsi/gomega"

	"github.com/woofdogtw/sylvia-iot-go/sdk/api"
)

const (
	clientAuthBase     = "http://localhost:1080/auth"
	clientCoremgrBase  = "http://localhost:1080/coremgr"
	clientClientID     = "private"
	clientClientSecret = "secret"
)

func testHttpNew() {
	opts := api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  clientCoremgrBase,
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err := api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())
}

func testHttpReq() {
	opts := api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  clientCoremgrBase,
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err := api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())

	statusCode, _, err := client.Request(http.MethodGet, "/api/v1/user", nil)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(statusCode).Should(Equal(http.StatusOK))

	// Request twice to use in memory token.
	statusCode, _, err = client.Request(http.MethodGet, "/api/v1/user", nil)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(statusCode).Should(Equal(http.StatusOK))
}

func testHttpReqErr() {
	opts := api.ClientOptions{
		AuthBase:     "",
		CoremgrBase:  clientCoremgrBase,
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err := api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())
	_, _, err = client.Request(http.MethodGet, "/api/v1/user", nil)
	Expect(err).Should(HaveOccurred())

	opts = api.ClientOptions{
		AuthBase:     "http://localhost:1234",
		CoremgrBase:  clientCoremgrBase,
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err = api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())
	_, _, err = client.Request(http.MethodGet, "/api/v1/user", nil)
	Expect(err).Should(HaveOccurred())

	opts = api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  "",
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err = api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())
	_, _, err = client.Request(http.MethodGet, "/api/v1/user", nil)
	Expect(err).Should(HaveOccurred())

	opts = api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  "http://localhost:1234",
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err = api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())
	_, _, err = client.Request(http.MethodGet, "/api/v1/user", nil)
	Expect(err).Should(HaveOccurred())

	opts = api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  clientCoremgrBase,
		ClientID:     "error",
		ClientSecret: clientClientSecret,
	}
	client, err = api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())
	statusCode, _, err := client.Request(http.MethodGet, "/api/v1/user", nil)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(statusCode).ShouldNot(Equal(http.StatusOK))
}
