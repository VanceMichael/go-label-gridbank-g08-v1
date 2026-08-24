package domain

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

var idPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{2,63}$`)

func NewID(prefix string) (string, error) {
	if !idPattern.MatchString(prefix) {
		return "", fmt.Errorf("invalid id prefix %q", prefix)
	}
	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(random), nil
}

func ValidateID(value, field string) error {
	value = strings.TrimSpace(value)
	if len(value) < 3 || len(value) > 96 {
		return Validation("validate_id", field+" has an invalid length")
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			continue
		}
		return Validation("validate_id", field+" contains unsupported characters")
	}
	return nil
}

func HashToken(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

func Fingerprint(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}
