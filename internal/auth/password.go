package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const passwordRounds = 180_000

func HashPassword(password string) (string, error) {
	if len(password) < 12 || len(password) > 256 {
		return "", errors.New("password must be between 12 and 256 bytes")
	}
	salt := make([]byte, 24)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	digest := derive(password, salt, passwordRounds)
	return "sha256$" + strconv.Itoa(passwordRounds) + "$" +
		base64.RawStdEncoding.EncodeToString(salt) + "$" +
		base64.RawStdEncoding.EncodeToString(digest), nil
}

func VerifyPassword(encoded, password string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "sha256" {
		return false
	}
	rounds, err := strconv.Atoi(parts[1])
	if err != nil || rounds < 100_000 || rounds > 1_000_000 {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil || len(salt) < 16 {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil || len(want) != sha256.Size {
		return false
	}
	got := derive(password, salt, rounds)
	return subtle.ConstantTimeCompare(got, want) == 1
}

func derive(password string, salt []byte, rounds int) []byte {
	state := make([]byte, 0, len(salt)+len(password))
	state = append(state, salt...)
	state = append(state, password...)
	digest := sha256.Sum256(state)
	for i := 1; i < rounds; i++ {
		h := sha256.New()
		h.Write(digest[:])
		h.Write(salt)
		digest = sha256.Sum256(h.Sum(nil))
	}
	return digest[:]
}
