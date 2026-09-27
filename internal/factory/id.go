package factory

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func newUUIDv4() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return formatUUIDv4(value[:]), nil
}

func formatUUIDv4(value []byte) string {
	if len(value) != 16 {
		panic("UUIDv4 requires 16 bytes")
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value)
	return fmt.Sprintf("%s-%s-%s-%s-%s", encoded[:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:])
}
