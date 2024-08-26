package sqlutils

import (
	"encoding/base64"
	"regexp"
	"runtime"
	"time"
	"unsafe"

	"golang.org/x/exp/rand"
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

var validTableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func isValidTableName(tableName string) bool {
	return validTableNameRegex.MatchString(tableName)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(uint64(time.Now().UnixNano()))

func getRandomString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Uint64(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Uint64(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func generateNewPrimaryKeyValue(dataType string) interface{} {
	switch dataType {
	case "int":
		return rand.Intn(1e6)
	case "string":
		return getRandomString(6)
	default:
		return nil
	}
}
