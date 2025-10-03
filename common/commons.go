package common

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/bits"
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

// attenzione la notazione è in BIg Endian in quanto la funzione sha1.Sum() ritorna in big endian
// MSBIndex mi calcala l'indce globale del buket (andano a prendere il primo byte piu significativo)
func MSBIndex(a, b []byte) (int, error) {
	d, err := XOR(a, b)
	if err != nil {
		return -1, err
	}
	n := len(d)
	if n == 0 {
		return -1, fmt.Errorf("empty input")
	}

	//serve a trovare l’indice del byte più significativo
	for i := 0; i < n; i++ {
		if d[i] != 0 {

			posInByte := bits.Len8(d[i]) - 1

			//trasformo in indice globale
			return (n-1-i)*8 + posInByte, nil
		}
	}

	return -1, nil
}
