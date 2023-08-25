package middlewares

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/woofdogtw/sylvia-iot-go/sdk/constants"
)

// The information contains `GetTokenInfoData` and access token.
type FullTokenInfo struct {
	Token string
	Info  GetTokenInfoData
}

// The user/client information of the token.
type GetTokenInfo struct {
	Data GetTokenInfoData `json:"data"`
}

// User/client information of the token.
type GetTokenInfoData struct {
	UserID   string          `json:"userId"`
	Account  string          `json:"account"`
	Roles    map[string]bool `json:"roles"`
	Name     string          `json:"name"`
	ClientID string          `json:"clientId"`
	Scopes   []string        `json:"scopes"`
}

// Constants.
const (
	TokenInfoKey = "FullTokenInfo"
)

// Generate the Gin authentication middleware.
func AuthMiddleware(authUri string) func(*gin.Context) {
	return func(c *gin.Context) {
		token := c.GetHeader("Authorization")
		token = strings.TrimSpace(token)
		if len(token) < 8 || strings.ToLower(token[:7]) != "bearer " {
			c.JSON(http.StatusBadRequest, gin.H{
				"code":    constants.ErrParam.String(),
				"message": "not bearer token",
			})
			return
		}

		req, err := http.NewRequest(http.MethodGet, authUri, nil)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"code":    constants.ErrRsc.String(),
				"message": err.Error(),
			})
			return
		}
		req.Header.Set("Authorization", "Bearer "+token[7:])
		client := http.Client{}
		res, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"code":    constants.ErrRsc.String(),
				"message": err.Error(),
			})
			return
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"code":    constants.ErrRsc.String(),
				"message": err.Error(),
			})
			return
		}
		if res.StatusCode == http.StatusUnauthorized {
			c.JSON(http.StatusUnauthorized, gin.H{"code": constants.ErrAuth.String()})
			return
		} else if res.StatusCode != http.StatusOK {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"code":    constants.ErrIntMsg.String(),
				"message": fmt.Sprintf("auth error with status code %d", res.StatusCode),
			})
			return
		}
		var tokenInfo GetTokenInfo
		err = json.Unmarshal(body, &tokenInfo)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"code":    constants.ErrIntMsg.String(),
				"message": err.Error(),
			})
			return
		}
		c.Set(TokenInfoKey, &FullTokenInfo{
			Token: token[7:],
			Info:  tokenInfo.Data,
		})
		c.Next()
	}
}
