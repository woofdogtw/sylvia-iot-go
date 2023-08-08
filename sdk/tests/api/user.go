package api

import (
	. "github.com/onsi/gomega"

	"github.com/woofdogtw/sylvia-iot-go/sdk/api"
)

func testUserGet() {
	opts := api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  clientCoremgrBase,
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err := api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())

	user, err := api.GetUser(client)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(user).ShouldNot(BeNil())
}

func testUserGetErr() {
	opts := api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  clientCoremgrBase,
		ClientID:     "error",
		ClientSecret: clientClientSecret,
	}
	_, err := api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
}

func testUserUpdate() {
	opts := api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  clientCoremgrBase,
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err := api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())

	name := "new name"
	data := api.PatchUserReqData{
		Name: &name,
	}

	err = api.UpdateUser(client, data)
	Expect(err).ShouldNot(HaveOccurred())
}

func testUserUpdateErr() {
	opts := api.ClientOptions{
		AuthBase:     clientAuthBase,
		CoremgrBase:  clientCoremgrBase,
		ClientID:     clientClientID,
		ClientSecret: clientClientSecret,
	}
	client, err := api.NewClient(opts)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(client).ShouldNot(BeNil())

	data := api.PatchUserReqData{}

	err = api.UpdateUser(client, data)
	Expect(err).Should(HaveOccurred())
}
