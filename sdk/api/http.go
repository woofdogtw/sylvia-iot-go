package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
)

// The HTTP client to request Sylvia-IoT APIs. With this client, you do not need to handle 401
// refresh token flow.
type Client struct {
	// The underlying HTTP client instance.
	client http.Client
	// `sylvia-iot-auth` base path.
	authBase string
	// `sylvia-iot-coremgr` base path.
	coremgrBase string
	// Client ID.
	clientID string
	// Client secret.
	clientSecret string
	// The access token.
	accessToken string
	// Mutex for access token.
	mutex sync.Mutex
}

// Options of the HTTP client [`Client`] that contains OAuth2 information.
type ClientOptions struct {
	// `sylvia-iot-auth` base path with scheme. For example `http://localhost:1080/auth`
	AuthBase string
	// `sylvia-iot-coremgr` base path with scheme. For example `http://localhost:1080/coremgr`
	CoremgrBase string
	// Client ID.
	ClientID string
	// Client secret.
	ClientSecret string
}

// The OAuth2 error response.
type Oauth2Error struct {
	// Error code.
	ErrorCode string `json:"error"`
	// Detail message.
	ErrorMessage string `json:"error_message,omitempty"`
}

// The Sylvia-IoT API error response.
type ApiError struct {
	// Error code.
	Code string `json:"code"`
	// Detail message.
	Message string `json:"message,omitempty"`
}

// Response from OAuth2 token API.
type oauth2TokenRes struct {
	AccessToken string `json:"access_token"`
}

var _ error = (*Oauth2Error)(nil)
var _ error = (*ApiError)(nil)

// Create an instance.
func NewClient(opts ClientOptions) (*Client, error) {
	return &Client{
		client:       http.Client{},
		authBase:     opts.AuthBase,
		coremgrBase:  opts.CoremgrBase,
		clientID:     opts.ClientID,
		clientSecret: opts.ClientSecret,
	}, nil
}

// Execute a Sylvia-IoT API request.
//   - `apiPath` is the relative path (of the coremgr base) the API with query string.
//     For example: `/api/v1/user/list?contains=word`, the client will do a request with
//     `http://coremgr-host/coremgr/api/v1/user/list?contains=word` URL.
//   - `body` MUST be JSON format.
func (c *Client) Request(method string, apiPath string,
	body []byte) (statusCode int, resBody []byte, err error) {
	url := c.coremgrBase + apiPath
	for retry := 1; ; retry-- {
		token := c.accessToken
		if token == "" {
			token, err = c.authToken()
			if err != nil {
				return 0, nil, err
			}
		}
		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}
		req, err := http.NewRequest(method, url, bodyReader)
		if err != nil {
			return 0, nil, err
		}
		req.Header.Add("Authorization", "Bearer "+token)
		req.Header.Add("Content-Type", "application/json")
		res, err := c.client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return 0, nil, err
		}
		if res.StatusCode != http.StatusUnauthorized || retry <= 0 {
			return res.StatusCode, body, nil
		}
		c.mutex.Lock()
		c.accessToken = ""
		c.mutex.Unlock()
	}
}

func (e *Oauth2Error) Error() string {
	return fmt.Sprintf("error: %s, error_message: %s", e.ErrorCode, e.ErrorMessage)
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("code: %s, message: %s", e.Code, e.Message)
}

func (c *Client) authToken() (string, error) {
	body := url.Values{"grant_type": []string{"client_credentials"}}
	bodyReader := bytes.NewReader([]byte(body.Encode()))
	req, err := http.NewRequest(http.MethodPost, c.authBase+"/oauth2/token", bodyReader)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(url.QueryEscape(c.clientID), url.QueryEscape(c.clientSecret))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	res, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	var tokenRes oauth2TokenRes
	if err = json.Unmarshal(bodyBytes, &tokenRes); err != nil {
		return "", err
	}
	c.mutex.Lock()
	c.accessToken = tokenRes.AccessToken
	c.mutex.Unlock()

	return tokenRes.AccessToken, nil
}
