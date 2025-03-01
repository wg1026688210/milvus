package errorutil

import (
	"errors"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/api/commonpb"
)

// ErrorList for print error log
type ErrorList []error

// Error method return an string representation of retry error list.
func (el ErrorList) Error() string {
	limit := 10
	var builder strings.Builder
	builder.WriteString("All attempts results:\n")
	for index, err := range el {
		// if early termination happens
		if err == nil {
			break
		}
		if index > limit {
			break
		}
		builder.WriteString(fmt.Sprintf("attempt #%d:%s\n", index+1, err.Error()))
	}
	return builder.String()
}

func UnhealthyStatus(code commonpb.StateCode) *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "proxy not healthy, StateCode=" + commonpb.StateCode_name[int32(code)],
	}
}

func UnhealthyError() error {
	return errors.New("unhealthy node")
}

func PermissionDenyError() error {
	return errors.New("permission deny")
}
