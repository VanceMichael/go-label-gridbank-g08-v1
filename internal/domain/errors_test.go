package domain

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestDomainErrorPreservesKindAndCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("disk full")
	err := Wrap(ErrUnavailable, "ledger.publish", "ledger", "ledger-1", "persistence failed", cause)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatal("domain kind was lost from error chain")
	}
	if !errors.Is(err, cause) {
		t.Fatal("underlying cause was lost from error chain")
	}
	for _, fragment := range []string{"ledger.publish", "ledger ledger-1", "persistence failed", "disk full"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("error %q does not contain %q", err, fragment)
		}
	}
}

func TestErrorConstructorsExposeExpectedKinds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		kind error
	}{
		{name: "validation", err: Validation("workload.plan", "invalid pool"), kind: ErrValidation},
		{name: "not found", err: NotFound("workload.get", "workload", "c1"), kind: ErrNotFound},
		{name: "conflict", err: Conflict("workload.start", "workload", "c1", "version changed"), kind: ErrConflict},
		{name: "precondition", err: Precondition("ledger.freeze", "ledger", "d1", "empty"), kind: ErrPrecondition},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !errors.Is(test.err, test.kind) {
				t.Fatalf("%v does not wrap %v", test.err, test.kind)
			}
			var target *Error
			if !errors.As(test.err, &target) {
				t.Fatalf("%T cannot be read as *Error", test.err)
			}
			if target.Op == "" || target.Message == "" {
				t.Fatalf("constructor omitted operation or message: %+v", target)
			}
		})
	}
}

func TestFingerprintSeparatesBoundaries(t *testing.T) {
	t.Parallel()
	one := Fingerprint("POST", "/workloads", "tenant-a", "body")
	two := Fingerprint("POST", "/workloads", "tenant-b", "body")
	three := Fingerprint("POST", "/workload", "stenant-a", "body")
	if one == two {
		t.Fatal("tenant boundary was not included in fingerprint")
	}
	if one == three {
		t.Fatal("length-separated parts should not admit concatenation ambiguity")
	}
	if one != Fingerprint("POST", "/workloads", "tenant-a", "body") {
		t.Fatal("fingerprint must be deterministic")
	}
}

func TestIDsAndTokens(t *testing.T) {
	t.Parallel()
	first, err := NewID("workload")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewID("workload")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("random IDs unexpectedly collided")
	}
	if err := ValidateID(first, "workload_id"); err != nil {
		t.Fatalf("generated ID should validate: %v", err)
	}
	if _, err := NewID("Bad Prefix"); err == nil {
		t.Fatal("invalid ID prefix was accepted")
	}
	invalid := []string{"", "ab", "contains space", "contains/slash", strings.Repeat("x", 97)}
	for _, value := range invalid {
		if err := ValidateID(value, "id"); !errors.Is(err, ErrValidation) {
			t.Errorf("ValidateID(%q) = %v, want validation error", value, err)
		}
	}
	if HashToken("secret") == HashToken("different") {
		t.Fatal("different tokens should not share a hash")
	}
	if got := HashToken("secret"); len(got) != 64 {
		t.Fatalf("token hash length = %d, want 64", len(got))
	}
}

func TestErrorFormattingWithoutOptionalFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err  *Error
		want string
	}{
		{err: &Error{Kind: ErrConflict}, want: "conflict"},
		{err: &Error{Kind: ErrConflict, Message: "changed"}, want: "changed"},
		{err: &Error{Kind: ErrConflict, Op: "save", Message: "changed"}, want: "save: changed"},
		{err: &Error{Kind: ErrConflict, Entity: "job", ID: "j1", Message: "changed"}, want: "job j1: changed"},
		{err: &Error{Kind: ErrConflict, Op: "save", Entity: "job", ID: "j1", Message: "changed", Cause: fmt.Errorf("locked")}, want: "save: job j1: changed: locked"},
	}
	for _, test := range tests {
		if got := test.err.Error(); got != test.want {
			t.Errorf("Error() = %q, want %q", got, test.want)
		}
	}
}
