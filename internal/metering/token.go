package metering

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

func newLeaseToken() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate metering lease token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
