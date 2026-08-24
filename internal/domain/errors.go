package domain

import (
	"errors"
	"fmt"
)

var (
	ErrValidation          = errors.New("validation failed")
	ErrUnauthorized        = errors.New("unauthorized")
	ErrForbidden           = errors.New("forbidden")
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrPrecondition        = errors.New("precondition failed")
	ErrLeaseLost           = errors.New("lease ownership lost")
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
	ErrUnavailable         = errors.New("dependency unavailable")
)

type Error struct {
	Kind    error
	Op      string
	Entity  string
	ID      string
	Message string
	Cause   error
}

func (e *Error) Error() string {
	base := e.Message
	if base == "" {
		base = e.Kind.Error()
	}
	if e.Entity != "" && e.ID != "" {
		base = fmt.Sprintf("%s %s: %s", e.Entity, e.ID, base)
	}
	if e.Op != "" {
		base = e.Op + ": " + base
	}
	if e.Cause != nil {
		base += ": " + e.Cause.Error()
	}
	return base
}

func (e *Error) Unwrap() error {
	if e.Cause != nil {
		return errors.Join(e.Kind, e.Cause)
	}
	return e.Kind
}

func Wrap(kind error, op, entity, id, message string, cause error) error {
	return &Error{Kind: kind, Op: op, Entity: entity, ID: id, Message: message, Cause: cause}
}

func Validation(op, message string) error {
	return Wrap(ErrValidation, op, "", "", message, nil)
}

func NotFound(op, entity, id string) error {
	return Wrap(ErrNotFound, op, entity, id, "not found", nil)
}

func Conflict(op, entity, id, message string) error {
	return Wrap(ErrConflict, op, entity, id, message, nil)
}

func Precondition(op, entity, id, message string) error {
	return Wrap(ErrPrecondition, op, entity, id, message, nil)
}
