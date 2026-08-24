package auth

import "testing"

func TestPasswordRoundTripAndSalt(t *testing.T) {
	password := "a-long-test-password"
	first, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("password hashes should use independent random salts")
	}
	if !VerifyPassword(first, password) || !VerifyPassword(second, password) {
		t.Fatal("valid password did not verify")
	}
	if VerifyPassword(first, "another-password") {
		t.Fatal("invalid password verified")
	}
}

func TestPasswordValidationAndMalformedHashes(t *testing.T) {
	invalidPasswords := []string{"", "short", "12345678901", string(make([]byte, 257))}
	for _, password := range invalidPasswords {
		if _, err := HashPassword(password); err == nil {
			t.Errorf("HashPassword accepted length %d", len(password))
		}
	}
	malformed := []string{
		"",
		"sha256",
		"md5$180000$c2FsdA$ZGlnZXN0",
		"sha256$invalid$c2FsdA$ZGlnZXN0",
		"sha256$5$c2FsdA$ZGlnZXN0",
		"sha256$180000$not-base64$not-base64",
		"sha256$180000$c2hvcnQ$c2hvcnQ",
	}
	for _, encoded := range malformed {
		if VerifyPassword(encoded, "a-long-test-password") {
			t.Errorf("malformed hash %q verified", encoded)
		}
	}
}
