/*
Package api provides HTTP API functions to access Sylvia-IoT coremgr APIs.

Client is a wrapped HTTP client that is used for Sylvia-IoT `coremgr` APIs with the following
features:
  - Use `client_credentials` grant type to get access token.
  - It is REQUIRED to register `private` clients (with secret).
  - It is RECOMMENDED to use the `service` role for clients, not to use `admin`, `manager` or
    `user` roles.
  - Refresh token automatically to integrate network servers and application servers (or adapters)
    conviniently because they do not need to do multiple operations for one API request.

Here is an example to create a client to access an API:

	import (
		"github.com/woofdogtw/sylvia-iot-go/sdk/api"
	)

	func main() {
		opts := api.ClientOptions{
			AuthBase:     "http://localhost:1080/auth",
			CoremgrBase:  "http://localhost:1080/coremgr",
			ClientID:     "ADAPTER_CLIENT_ID",
			ClientSecret: "ADAPTER_CLIENT_SECRET",
		}
		client, _ := api.NewClient(opts)
		client.Request("GET", "/api/v1/user", nil)
	}
*/
package api
