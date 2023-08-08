package middlewares

import (
	. "github.com/onsi/ginkgo"
)

func Suite() func() {
	return func() {
		Describe("Auth", func() {
			init := false
			BeforeEach(func() {
				if init {
					return
				}
				init = true
				authBeforeAll()
			})

			It("200", authTest200)
			It("400", authTest400)
			It("401", authTest401)
			It("503", authTest503)
		})
	}
}
