// Package api exposes the Courierbox HTTP API: target registration, event
// submission, event and attempt queries, the dead queue and, in test mode, the
// deterministic control plane. Responses use strict JSON, request size limits,
// structured error codes and a correlation id.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Error codes are machine-decidable identifiers carried in error.code.
const (
	CodeBadRequest            = "BAD_REQUEST"
	CodeMissingIdempotencyKey = "MISSING_IDEMPOTENCY_KEY"
	CodePayloadTooLarge       = "PAYLOAD_TOO_LARGE"
	CodeInvalidJSON           = "INVALID_JSON"
	CodeUnknownField          = "UNKNOWN_FIELD"
	CodeInvalidURL            = "INVALID_URL"
	CodeNotFound              = "NOT_FOUND"
	CodeIdempotencyConflict   = "IDEMPOTENCY_CONFLICT"
	CodeConflict              = "CONFLICT"
	MethodNotAllowed          = "METHOD_NOT_ALLOWED"
	CodeInternal              = "INTERNAL"
)

// APIError is the JSON body returned for any non-2xx response.
type APIError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

// statusForCode maps an error code to its HTTP status.
func statusForCode(code string) int {
	switch code {
	case CodeBadRequest, CodeMissingIdempotencyKey, CodeInvalidJSON, CodeUnknownField:
		return http.StatusBadRequest
	case CodePayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	case CodeInvalidURL:
		return http.StatusUnprocessableEntity
	case CodeNotFound:
		return http.StatusNotFound
	case CodeIdempotencyConflict, CodeConflict:
		return http.StatusConflict
	case MethodNotAllowed:
		return http.StatusMethodNotAllowed
	default:
		return http.StatusInternalServerError
	}
}

// writeError writes a structured error response.
func writeError(w http.ResponseWriter, r *http.Request, code, message string) {
	status := statusForCode(code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	reqID := requestID(r)
	_ = json.NewEncoder(w).Encode(APIError{Code: code, Message: message, RequestID: reqID})
}

// writeJSON writes a 2xx JSON response.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// storeError maps a store error to an API error response.
func storeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, errNotFound):
		writeError(w, r, CodeNotFound, "resource not found")
	case errors.Is(err, errIdempotencyConflict):
		writeError(w, r, CodeIdempotencyConflict, "idempotency key reused with a different payload")
	case errors.Is(err, errNotDead):
		writeError(w, r, CodeConflict, "event is not in the dead queue")
	default:
		writeError(w, r, CodeInternal, "internal error")
	}
}
