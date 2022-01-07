package errCode

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrCodeDict = map[int]string{}

func regErrCode(errMap map[int]string) {
	for k, v := range errMap {
		ErrCodeDict[k] = v
	}
}

func ToGrpcErr(code int, args ...interface{}) error {
	msg := "unknow"
	if v, ok := ErrCodeDict[code]; ok {
		msg = v
	}
	return status.Errorf(codes.Code(code), msg, args...)
}
