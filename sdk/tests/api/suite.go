package api

import (
	. "github.com/onsi/ginkgo"
)

func Suite() func() {
	return func() {
		Describe("http", func() {
			It("NewClient()", testHttpNew)
			It("Request()", testHttpReq)
			It("Request() with error", testHttpReqErr)
		})

		Describe("user", func() {
			It("GetUser()", testUserGet)
			It("GetUser() with error", testUserGetErr)
			It("UpdateUser()", testUserUpdate)
			It("UpdateUser() with error", testUserUpdateErr)
		})
	}
}
