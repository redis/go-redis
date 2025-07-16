package push

import (
	"errors"
	"fmt"
)

// Push notification error definitions
// This file contains all error types and messages used by the push notification system

// Common error variables for reuse
var (
	// ErrHandlerNil is returned when attempting to register a nil handler
	ErrHandlerNil = errors.New("handler cannot be nil")
)

// Registry errors

// ErrHandlerExists creates an error for when attempting to overwrite an existing handler
func ErrHandlerExists(pushNotificationName string) error {
	return NewHandlerError("register", pushNotificationName, "cannot overwrite existing handler", nil)
}

// ErrProtectedHandler creates an error for when attempting to unregister a protected handler
func ErrProtectedHandler(pushNotificationName string) error {
	return NewHandlerError("unregister", pushNotificationName, "handler is protected", nil)
}

// VoidProcessor errors

// ErrVoidProcessorRegister creates an error for when attempting to register a handler on void processor
func ErrVoidProcessorRegister(pushNotificationName string) error {
	return NewProcessorError("void_processor", "register", "push notifications are disabled", nil)
}

// ErrVoidProcessorUnregister creates an error for when attempting to unregister a handler on void processor
func ErrVoidProcessorUnregister(pushNotificationName string) error {
	return NewProcessorError("void_processor", "unregister", "push notifications are disabled", nil)
}

// Error message constants for consistency
const (
	// Error message templates
	MsgHandlerNil              = "handler cannot be nil"
	MsgHandlerExists           = "cannot overwrite existing handler for push notification: %s"
	MsgProtectedHandler        = "cannot unregister protected handler for push notification: %s"
	MsgVoidProcessorRegister   = "cannot register push notification handler '%s': push notifications are disabled (using void processor)"
	MsgVoidProcessorUnregister = "cannot unregister push notification handler '%s': push notifications are disabled (using void processor)"
)

// Error type definitions for advanced error handling

// HandlerError represents errors related to handler operations
type HandlerError struct {
	Operation            string // "register", "unregister", "get"
	PushNotificationName string
	Reason               string
	Err                  error
}

func (e *HandlerError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("handler %s failed for '%s': %s (%v)", e.Operation, e.PushNotificationName, e.Reason, e.Err)
	}
	return fmt.Sprintf("handler %s failed for '%s': %s", e.Operation, e.PushNotificationName, e.Reason)
}

func (e *HandlerError) Unwrap() error {
	return e.Err
}

// NewHandlerError creates a new HandlerError
func NewHandlerError(operation, pushNotificationName, reason string, err error) *HandlerError {
	return &HandlerError{
		Operation:            operation,
		PushNotificationName: pushNotificationName,
		Reason:               reason,
		Err:                  err,
	}
}

// ProcessorError represents errors related to processor operations
type ProcessorError struct {
	ProcessorType string // "processor", "void_processor"
	Operation     string // "process", "register", "unregister"
	Reason        string
	Err           error
}

func (e *ProcessorError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s %s failed: %s (%v)", e.ProcessorType, e.Operation, e.Reason, e.Err)
	}
	return fmt.Sprintf("%s %s failed: %s", e.ProcessorType, e.Operation, e.Reason)
}

func (e *ProcessorError) Unwrap() error {
	return e.Err
}

// NewProcessorError creates a new ProcessorError
func NewProcessorError(processorType, operation, reason string, err error) *ProcessorError {
	return &ProcessorError{
		ProcessorType: processorType,
		Operation:     operation,
		Reason:        reason,
		Err:           err,
	}
}

// Helper functions for common error scenarios

// IsHandlerNilError checks if an error is due to a nil handler
func IsHandlerNilError(err error) bool {
	return errors.Is(err, ErrHandlerNil)
}

// IsHandlerExistsError checks if an error is due to attempting to overwrite an existing handler
func IsHandlerExistsError(err error) bool {
	if handlerErr, ok := err.(*HandlerError); ok {
		return handlerErr.Operation == "register" && handlerErr.Reason == "cannot overwrite existing handler"
	}
	return false
}

// IsProtectedHandlerError checks if an error is due to attempting to unregister a protected handler
func IsProtectedHandlerError(err error) bool {
	if handlerErr, ok := err.(*HandlerError); ok {
		return handlerErr.Operation == "unregister" && handlerErr.Reason == "handler is protected"
	}
	return false
}

// IsVoidProcessorError checks if an error is due to void processor operations
func IsVoidProcessorError(err error) bool {
	if procErr, ok := err.(*ProcessorError); ok {
		return procErr.ProcessorType == "void_processor" && procErr.Reason == "push notifications are disabled"
	}
	return false
}

// extractNotificationName attempts to extract the notification name from error messages
func extractNotificationName(err error) string {
	if handlerErr, ok := err.(*HandlerError); ok {
		return handlerErr.PushNotificationName
	}
	if procErr, ok := err.(*ProcessorError); ok {
		// For ProcessorError, we don't have direct access to the notification name
		// but in a real implementation you could store this in the struct
		return "unknown"
	}
	return "unknown"
}
