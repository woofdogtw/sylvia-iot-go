package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/woofdogtw/sylvia-iot-go/sdk/constants"
)

type GetUserResData struct {
	UserID     string
	Account    string
	CreatedAt  time.Time
	ModifiedAt time.Time
	VerifiedAt time.Time // may be zero time
	Roles      map[string]bool
	Name       string
	Info       map[string]interface{}
}

type PatchUserReqData struct {
	// Empty will not change the original setting.
	Password string `json:"password,omitempty"`
	// The display name.
	//
	// `Note`: `nil` will not change setting. Empty string overwrite the original setting.
	Name *string `json:"name,omitempty"`
	// `nil` will not change the original setting.
	Info map[string]interface{} `json:"info,omitempty"`
}

type userGetRes struct {
	Data userGetResData `json:"data"`
}

type userGetResData struct {
	UserID     string                 `json:"userId"`
	Account    string                 `json:"account"`
	CreatedAt  string                 `json:"createdAt"`
	ModifiedAt string                 `json:"modifiedAt"`
	VerifiedAt string                 `json:"verifiedAt"`
	Roles      map[string]bool        `json:"roles"`
	Name       string                 `json:"name"`
	Info       map[string]interface{} `json:"info"`
}

type userPatchReq struct {
	Data PatchUserReqData `json:"data"`
}

// `GET /coremgr/api/v1/user`
func GetUser(c *Client) (*GetUserResData, *ApiError) {
	statusCode, body, err := c.Request(http.MethodGet, "/api/v1/user", nil)
	if err != nil {
		return nil, &ApiError{
			Code:    constants.ErrRsc.String(),
			Message: err.Error(),
		}
	} else if statusCode != http.StatusOK {
		var apiErr ApiError
		if err = json.Unmarshal(body, &apiErr); err != nil {
			return nil, &ApiError{
				Code:    constants.ErrUnknown.String(),
				Message: err.Error(),
			}
		}
		return nil, &apiErr
	}

	var data userGetRes
	if err = json.Unmarshal(body, &data); err != nil {
		return nil, &ApiError{
			Code:    constants.ErrUnknown.String(),
			Message: err.Error(),
		}
	}
	createdAt, err := time.Parse(constants.TimeFormat, data.Data.CreatedAt)
	if err != nil {
		return nil, &ApiError{
			Code:    constants.ErrUnknown.String(),
			Message: err.Error(),
		}
	}
	modifiedAt, err := time.Parse(constants.TimeFormat, data.Data.ModifiedAt)
	if err != nil {
		return nil, &ApiError{
			Code:    constants.ErrUnknown.String(),
			Message: err.Error(),
		}
	}
	var verifiedAt time.Time
	if data.Data.VerifiedAt != "" {
		verifiedAt, err = time.Parse(constants.TimeFormat, data.Data.VerifiedAt)
		if err != nil {
			return nil, &ApiError{
				Code:    constants.ErrUnknown.String(),
				Message: err.Error(),
			}
		}
	}
	return &GetUserResData{
		UserID:     data.Data.UserID,
		Account:    data.Data.Account,
		CreatedAt:  createdAt,
		ModifiedAt: modifiedAt,
		VerifiedAt: verifiedAt,
		Roles:      data.Data.Roles,
		Name:       data.Data.Name,
		Info:       data.Data.Info,
	}, nil
}

// `PATCH /coremgr/api/v1/user`
func UpdateUser(c *Client, data PatchUserReqData) *ApiError {
	bodyBytes, err := json.Marshal(userPatchReq{Data: data})
	if err != nil {
		return &ApiError{
			Code:    constants.ErrRsc.String(),
			Message: err.Error(),
		}
	}

	statusCode, body, err := c.Request(http.MethodPatch, "/api/v1/user", bodyBytes)
	if err != nil {
		return &ApiError{
			Code:    constants.ErrRsc.String(),
			Message: err.Error(),
		}
	} else if statusCode != http.StatusNoContent {
		var apiErr ApiError
		if err = json.Unmarshal(body, &apiErr); err != nil {
			return &ApiError{
				Code:    constants.ErrUnknown.String(),
				Message: err.Error(),
			}
		}
		return &apiErr
	}
	return nil
}
