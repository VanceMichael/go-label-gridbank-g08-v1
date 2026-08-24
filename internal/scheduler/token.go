package scheduler

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

func newLeaseToken() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate scheduler lease token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
