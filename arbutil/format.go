package arbutil

import (
	"encoding/hex"
	"unicode/utf8"
)

func ToStringOrHex(input []byte) string {
	if utf8.Valid(input) {
		return string(input)
	}
	return hex.EncodeToString(input)
}
