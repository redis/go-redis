package proto

import (
	"errors"
	"strings"
)

// Typed Redis errors for better error handling with wrapping support.
// These errors maintain backward compatibility by keeping the same error messages.

// LoadingError is returned when Redis is loading the dataset in memory.
type LoadingError struct {
	msg string
}

func (e *LoadingError) Error() string {
	return e.msg
}

func (e *LoadingError) RedisError() {}

func (e *LoadingError) Is(target error) bool {
	_, ok := target.(*LoadingError)
	return ok
}

// NewLoadingError creates a new LoadingError with the given message.
func NewLoadingError(msg string) *LoadingError {
	return &LoadingError{msg: msg}
}

// ReadOnlyError is returned when trying to write to a read-only replica.
type ReadOnlyError struct {
	msg string
}

func (e *ReadOnlyError) Error() string {
	return e.msg
}

func (e *ReadOnlyError) RedisError() {}

func (e *ReadOnlyError) Is(target error) bool {
	_, ok := target.(*ReadOnlyError)
	return ok
}

// NewReadOnlyError creates a new ReadOnlyError with the given message.
func NewReadOnlyError(msg string) *ReadOnlyError {
	return &ReadOnlyError{msg: msg}
}

// MovedError is returned when a key has been moved to a different node in a cluster.
type MovedError struct {
	msg  string
	addr string
}

func (e *MovedError) Error() string {
	return e.msg
}

func (e *MovedError) RedisError() {}

func (e *MovedError) Is(target error) bool {
	_, ok := target.(*MovedError)
	return ok
}

// Addr returns the address of the node where the key has been moved.
func (e *MovedError) Addr() string {
	return e.addr
}

// NewMovedError creates a new MovedError with the given message and address.
func NewMovedError(msg string, addr string) *MovedError {
	return &MovedError{msg: msg, addr: addr}
}

// AskError is returned when a key is being migrated and the client should ask another node.
type AskError struct {
	msg  string
	addr string
}

func (e *AskError) Error() string {
	return e.msg
}

func (e *AskError) RedisError() {}

func (e *AskError) Is(target error) bool {
	_, ok := target.(*AskError)
	return ok
}

// Addr returns the address of the node to ask.
func (e *AskError) Addr() string {
	return e.addr
}

// NewAskError creates a new AskError with the given message and address.
func NewAskError(msg string, addr string) *AskError {
	return &AskError{msg: msg, addr: addr}
}

// ClusterDownError is returned when the cluster is down.
type ClusterDownError struct {
	msg string
}

func (e *ClusterDownError) Error() string {
	return e.msg
}

func (e *ClusterDownError) RedisError() {}

func (e *ClusterDownError) Is(target error) bool {
	_, ok := target.(*ClusterDownError)
	return ok
}

// NewClusterDownError creates a new ClusterDownError with the given message.
func NewClusterDownError(msg string) *ClusterDownError {
	return &ClusterDownError{msg: msg}
}

// TryAgainError is returned when a command cannot be processed and should be retried.
type TryAgainError struct {
	msg string
}

func (e *TryAgainError) Error() string {
	return e.msg
}

func (e *TryAgainError) RedisError() {}

func (e *TryAgainError) Is(target error) bool {
	_, ok := target.(*TryAgainError)
	return ok
}

// NewTryAgainError creates a new TryAgainError with the given message.
func NewTryAgainError(msg string) *TryAgainError {
	return &TryAgainError{msg: msg}
}

// MasterDownError is returned when the master is down.
type MasterDownError struct {
	msg string
}

func (e *MasterDownError) Error() string {
	return e.msg
}

func (e *MasterDownError) RedisError() {}

func (e *MasterDownError) Is(target error) bool {
	_, ok := target.(*MasterDownError)
	return ok
}

// NewMasterDownError creates a new MasterDownError with the given message.
func NewMasterDownError(msg string) *MasterDownError {
	return &MasterDownError{msg: msg}
}

// MaxClientsError is returned when the maximum number of clients has been reached.
type MaxClientsError struct {
	msg string
}

func (e *MaxClientsError) Error() string {
	return e.msg
}

func (e *MaxClientsError) RedisError() {}

func (e *MaxClientsError) Is(target error) bool {
	_, ok := target.(*MaxClientsError)
	return ok
}

// NewMaxClientsError creates a new MaxClientsError with the given message.
func NewMaxClientsError(msg string) *MaxClientsError {
	return &MaxClientsError{msg: msg}
}

// parseTypedRedisError parses a Redis error message and returns a typed error if applicable.
// This function maintains backward compatibility by keeping the same error messages.
func parseTypedRedisError(msg string) error {
	// Check for specific error patterns and return typed errors
	switch {
	case strings.HasPrefix(msg, "LOADING "):
		return NewLoadingError(msg)
	case strings.HasPrefix(msg, "READONLY "):
		return NewReadOnlyError(msg)
	case strings.HasPrefix(msg, "MOVED "):
		// Extract address from "MOVED <slot> <addr>"
		addr := extractAddr(msg)
		return NewMovedError(msg, addr)
	case strings.HasPrefix(msg, "ASK "):
		// Extract address from "ASK <slot> <addr>"
		addr := extractAddr(msg)
		return NewAskError(msg, addr)
	case strings.HasPrefix(msg, "CLUSTERDOWN "):
		return NewClusterDownError(msg)
	case strings.HasPrefix(msg, "TRYAGAIN "):
		return NewTryAgainError(msg)
	case strings.HasPrefix(msg, "MASTERDOWN "):
		return NewMasterDownError(msg)
	case msg == "ERR max number of clients reached":
		return NewMaxClientsError(msg)
	default:
		// Return generic RedisError for unknown error types
		return RedisError(msg)
	}
}

// extractAddr extracts the address from MOVED/ASK error messages.
// Format: "MOVED <slot> <addr>" or "ASK <slot> <addr>"
func extractAddr(msg string) string {
	ind := strings.LastIndex(msg, " ")
	if ind == -1 {
		return ""
	}
	return msg[ind+1:]
}

// IsLoadingError checks if an error is a LoadingError, even if wrapped.
func IsLoadingError(err error) bool {
	var loadingErr *LoadingError
	return errors.As(err, &loadingErr)
}

// IsReadOnlyError checks if an error is a ReadOnlyError, even if wrapped.
func IsReadOnlyError(err error) bool {
	var readOnlyErr *ReadOnlyError
	return errors.As(err, &readOnlyErr)
}

// IsMovedError checks if an error is a MovedError, even if wrapped.
// Returns the error and a boolean indicating if it's a MovedError.
func IsMovedError(err error) (*MovedError, bool) {
	var movedErr *MovedError
	if errors.As(err, &movedErr) {
		return movedErr, true
	}
	return nil, false
}

// IsAskError checks if an error is an AskError, even if wrapped.
// Returns the error and a boolean indicating if it's an AskError.
func IsAskError(err error) (*AskError, bool) {
	var askErr *AskError
	if errors.As(err, &askErr) {
		return askErr, true
	}
	return nil, false
}

// IsClusterDownError checks if an error is a ClusterDownError, even if wrapped.
func IsClusterDownError(err error) bool {
	var clusterDownErr *ClusterDownError
	return errors.As(err, &clusterDownErr)
}

// IsTryAgainError checks if an error is a TryAgainError, even if wrapped.
func IsTryAgainError(err error) bool {
	var tryAgainErr *TryAgainError
	return errors.As(err, &tryAgainErr)
}

// IsMasterDownError checks if an error is a MasterDownError, even if wrapped.
func IsMasterDownError(err error) bool {
	var masterDownErr *MasterDownError
	return errors.As(err, &masterDownErr)
}

// IsMaxClientsError checks if an error is a MaxClientsError, even if wrapped.
func IsMaxClientsError(err error) bool {
	var maxClientsErr *MaxClientsError
	return errors.As(err, &maxClientsErr)
}

