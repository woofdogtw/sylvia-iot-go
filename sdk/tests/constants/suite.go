package constants

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/woofdogtw/sylvia-iot-go/sdk/constants"
)

func Suite() func() {
	return func() {
		Describe("errors", func() {
			It("ErrAuth", func() {
				Expect(constants.ErrAuth.String()).Should(Equal("err_auth"))
			})
			It("ErrDB", func() {
				Expect(constants.ErrDB.String()).Should(Equal("err_db"))
			})
			It("ErrIntMsg", func() {
				Expect(constants.ErrIntMsg.String()).Should(Equal("err_int_msg"))
			})
			It("ErrNotFound", func() {
				Expect(constants.ErrNotFound.String()).Should(Equal("err_not_found"))
			})
			It("ErrParam", func() {
				Expect(constants.ErrParam.String()).Should(Equal("err_param"))
			})
			It("ErrPerm", func() {
				Expect(constants.ErrPerm.String()).Should(Equal("err_perm"))
			})
			It("ErrRsc", func() {
				Expect(constants.ErrRsc.String()).Should(Equal("err_rsc"))
			})
			It("ErrUnknown", func() {
				Expect(constants.ErrUnknown.String()).Should(Equal("err_unknown"))
			})
			It("undefined", func() {
				Expect(constants.ErrResp(-1).String()).Should(Equal("err_unknown"))
			})
		})
	}
}
