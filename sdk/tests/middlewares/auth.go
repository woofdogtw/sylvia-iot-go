package middlewares

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/gin-gonic/gin"
	. "github.com/onsi/gomega"
	"github.com/woofdogtw/sylvia-iot-go/sdk/middlewares"
)

type authTokenBody struct {
	AccessToken string `json:"access_token"`
}

// Pre-registered user/client in sylvia-iot-auth.
const (
	authInfoUserID   = "admin"
	authInfoAccount  = "admin"
	authInfoPassword = "admin"
	authInfoClinet   = "public"
	authInfoRedirect = "http://localhost:1080/auth/oauth2/redirect"
)

// API information.
const (
	authUri     = "http://localhost:1080/auth/api/v1/auth/tokeninfo"
	authUriBase = "http://localhost:1080/auth/oauth2"
)

var (
	testToken string
)

func authTest200() {
	var tokenInfo *middlewares.FullTokenInfo

	engine := gin.New()
	engine.Use(middlewares.AuthMiddleware(authUri))
	engine.GET("/", func(c *gin.Context) {
		info, exist := c.Get(middlewares.TokenInfoKey)
		Expect(exist).Should(BeTrue())
		tokenInfo = info.(*middlewares.FullTokenInfo)

		Expect(tokenInfo.Info.UserID).Should(Equal(authInfoUserID))
		Expect(tokenInfo.Info.Account).Should(Equal(authInfoAccount))
		Expect(tokenInfo.Info.ClientID).Should(Equal(authInfoClinet))

		c.AbortWithStatus(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Add("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	res := w.Result()
	defer res.Body.Close()
	_, err := io.ReadAll(res.Body)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(res.StatusCode).Should(Equal(http.StatusNoContent))
	Expect(tokenInfo).ShouldNot(BeNil())
	Expect(tokenInfo.Token).Should(Equal(testToken))
}

func authTest400() {
	engine := gin.New()
	engine.Use(middlewares.AuthMiddleware(authUri))
	engine.GET("/", func(c *gin.Context) {})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	res := w.Result()
	defer res.Body.Close()
	_, err := io.ReadAll(res.Body)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(res.StatusCode).Should(Equal(http.StatusBadRequest))
}

func authTest401() {
	engine := gin.New()
	engine.Use(middlewares.AuthMiddleware(authUri))
	engine.GET("/", func(c *gin.Context) {})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Add("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	res := w.Result()
	defer res.Body.Close()
	_, err := io.ReadAll(res.Body)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(res.StatusCode).Should(Equal(http.StatusUnauthorized))
}

func authTest503() {
	engine := gin.New()
	engine.Use(middlewares.AuthMiddleware("http://localhost:10811"))
	engine.GET("/", func(c *gin.Context) {})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Add("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	res := w.Result()
	defer res.Body.Close()
	_, err := io.ReadAll(res.Body)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(res.StatusCode).Should(Equal(http.StatusServiceUnavailable))
}

func authBeforeAll() {
	gin.SetMode(gin.ReleaseMode)

	testToken = authLogin()
}

// Do POST form data requests for sylvia-iot-auth OAuth2 APIs.
func authDoFormReq(uri string, form url.Values) (status int, res *http.Response, body []byte) {
	client := http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	res, err := client.PostForm(uri, form)
	Expect(err).ShouldNot(HaveOccurred())
	defer res.Body.Close()
	body, err = io.ReadAll(res.Body)
	Expect(err).ShouldNot(HaveOccurred())
	return res.StatusCode, res, body
}

// Login and get access token.
//
// Reference: https://woofdogtw.github.io/sylvia-iot-core/dev/oauth2.html#using-curl
func authLogin() string {
	// Post /login
	stateValues := url.Values{
		"response_type": []string{"code"},
		"client_id":     []string{authInfoClinet},
		"redirect_uri":  []string{authInfoRedirect},
	}
	postLoginBody := url.Values{
		"state":    []string{stateValues.Encode()},
		"account":  []string{authInfoAccount},
		"password": []string{authInfoPassword},
	}
	status, res, body := authDoFormReq(authUriBase+"/login", postLoginBody)
	Expect(status).Should(Equal(http.StatusFound), string(body))
	location := res.Header.Get("location")
	Expect(location).ShouldNot(BeEmpty())
	postLoginResURL, err := url.ParseRequestURI(location)
	Expect(err).ShouldNot(HaveOccurred())
	postLoginResValues, err := url.ParseQuery(postLoginResURL.RawQuery)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(postLoginResValues.Get("session_id")).ShouldNot(BeEmpty())

	// Post /authorize
	postAuthorizeBody := url.Values{
		"response_type": []string{"code"},
		"client_id":     []string{authInfoClinet},
		"redirect_uri":  []string{authInfoRedirect},
		"allow":         []string{"yes"},
		"session_id":    []string{postLoginResValues.Get("session_id")},
	}
	status, res, body = authDoFormReq(authUriBase+"/authorize", postAuthorizeBody)
	Expect(status).Should(Equal(http.StatusFound), string(body))
	location = res.Header.Get("location")
	Expect(location).ShouldNot(BeEmpty())
	postAuthorizeResURL, err := url.ParseRequestURI(location)
	Expect(err).ShouldNot(HaveOccurred())
	postAuthorizeResValues, err := url.ParseQuery(postAuthorizeResURL.RawQuery)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(postAuthorizeResValues.Get("code")).ShouldNot(BeEmpty())

	// Post /token
	postTokenBody := url.Values{
		"grant_type":   []string{"authorization_code"},
		"code":         []string{postAuthorizeResValues.Get("code")},
		"client_id":    []string{authInfoClinet},
		"redirect_uri": []string{authInfoRedirect},
	}
	status, _, body = authDoFormReq(authUriBase+"/token", postTokenBody)
	Expect(status).Should(Equal(http.StatusOK), string(body))
	var tokenInfo authTokenBody
	err = json.Unmarshal(body, &tokenInfo)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(tokenInfo.AccessToken).ShouldNot(BeEmpty())

	return tokenInfo.AccessToken
}
