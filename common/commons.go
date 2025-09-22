package common

import (
	"crypto/sha1"
)

func Sha1ID(tokenID string) []byte {
	s := sha1.Sum([]byte(tokenID)) // [20]byte
	b := make([]byte, sha1.Size)
	copy(b, s[:])
	return b // esattamente 20 byte
}
