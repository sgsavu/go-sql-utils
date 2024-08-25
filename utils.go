package sqlutils

import (
	"encoding/base64"
	"runtime"
)

func isBase64(b []byte) bool {
	return len(b) > 0 && (b[len(b)-1] == '=' || b[len(b)-1] == '/')
}

func decodeBase64(b []byte) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func getCurrentFuncName() string {
	pc, _, _, _ := runtime.Caller(1)
	return runtime.FuncForPC(pc).Name()
}
