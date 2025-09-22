package common

import (
	"crypto/sha1"
	"fmt"
	"math/big"
)

func Sha1ID(tokenID string) []byte {
	s := sha1.Sum([]byte(tokenID)) // [20]byte
	b := make([]byte, sha1.Size)
	copy(b, s[:])
	return b // esattamente 20 byte
}

func XOR(a, b []byte) ([]byte, error) {
	if a == nil || b == nil {
		return nil, fmt.Errorf("nil input")
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("length mismatch: %d vs %d", len(a), len(b))
	}
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out, nil
}

func XorDist(a20 []byte, b20 []byte) *big.Int {
	nb := make([]byte, len(a20))
	for i := range a20 {
		nb[i] = a20[i] ^ b20[i]
	}
	return new(big.Int).SetBytes(nb)
}
