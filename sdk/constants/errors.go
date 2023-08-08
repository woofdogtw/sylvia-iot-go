package constants

// Sylvia-IoT error response codes.
type ErrResp int

// Sylvia-IoT error response codes.
const (
	ErrAuth ErrResp = iota
	ErrDB
	ErrIntMsg
	ErrNotFound
	ErrParam
	ErrPerm
	ErrRsc
	ErrUnknown
)

func (e ErrResp) String() string {
	switch e {
	case ErrAuth:
		return "err_auth"
	case ErrDB:
		return "err_db"
	case ErrIntMsg:
		return "err_int_msg"
	case ErrNotFound:
		return "err_not_found"
	case ErrParam:
		return "err_param"
	case ErrPerm:
		return "err_perm"
	case ErrRsc:
		return "err_rsc"
	default:
		return "err_unknown"
	}
}
