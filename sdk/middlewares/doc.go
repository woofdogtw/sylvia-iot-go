/*
Package middlewares provides the following Gin middlewares:
  - auth

The authentication middleware by sending the Bearer token to `sylvia-iot-auth`.

Here is an example to wrap the auth middleware and how to get token information:

	import (
		"github.com/gin-gonic/gin"
		"github.com/woofdogtw/sylvia-iot-go/sdk/middlewares"
	)

	func main() {
		r := gin.Default()
		r.Use(middlewares.AuthMiddleware("http://localhost:1080/auth/api/v1/auth/tokeninfo"))
		r.GET("/", func(c *gin.Context) {
			c.AbortWithStatus(204)
		})
		r.Run()
	}
*/
package middlewares
