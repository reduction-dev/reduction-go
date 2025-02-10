package internal

import (
	"fmt"
	"net/http"
)

// Error represents an error with an associated HTTP status code.
type Error struct {
	StatusCode int
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("status %d: %s", e.StatusCode, e.Message)
}

// NewBadRequestError creates a new CustomError with a 400 status code.
func NewBadRequestError(message string) error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Message:    message,
	}
}

func NewBadRequestErrorf(format string, args ...interface{}) error {
	return &Error{
		StatusCode: http.StatusBadRequest,
		Message:    fmt.Sprintf(format, args...),
	}
}
