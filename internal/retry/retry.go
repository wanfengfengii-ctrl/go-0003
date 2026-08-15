// Package retry classifies delivery outcomes and computes bounded exponential
// backoff. The policy is deliberately small and free of side effects so it can
// be unit-tested and driven by a manual clock.
package retry

import (
	"net/http"
	"strconv"
	"time"
)

// Category labels the kind of outcome for audit records.
type Category string

const (
	// CategorySuccess is a 2xx response.
	CategorySuccess Category = "success"
	// CategoryNetworkError is a transport-level failure (timeout, refused, DNS).
	CategoryNetworkError Category = "network_error"
	// CategoryRetryableHTTP is a 408/425/429/5xx response.
	CategoryRetryableHTTP Category = "retryable_http"
	// CategoryNonRetryableHTTP is any other 4xx response.
	CategoryNonRetryableHTTP Category = "non_retryable_http"
)

// Outcome describes the result of a single delivery attempt.
type Outcome struct {
	Category     Category
	Retryable    bool
	StatusCode   int
	ErrorMessage string
	// RetryAfter is parsed from a 429 Retry-After header (seconds form). It is
	// zero when absent or unparseable.
	RetryAfter time.Duration
}

// ClassifyResponse maps an HTTP response to an Outcome. The response body must
// already have been consumed by the caller; this function only inspects the
// status code and Retry-After header.
func ClassifyResponse(resp *http.Response) Outcome {
	code := resp.StatusCode
	o := Outcome{StatusCode: code}
	switch {
	case code >= 200 && code < 300:
		o.Category = CategorySuccess
		o.Retryable = false
	case code == http.StatusRequestTimeout, // 408
		code == http.StatusTooEarly,        // 425
		code == http.StatusTooManyRequests, // 429
		code >= 500:
		o.Category = CategoryRetryableHTTP
		o.Retryable = true
		if code == http.StatusTooManyRequests {
			o.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		}
	default:
		o.Category = CategoryNonRetryableHTTP
		o.Retryable = false
	}
	return o
}

// ClassifyError maps a transport error to an Outcome. All transport errors are
// treated as retryable network errors.
func ClassifyError(err error) Outcome {
	return Outcome{
		Category:     CategoryNetworkError,
		Retryable:    true,
		ErrorMessage: err.Error(),
	}
}

// parseRetryAfter parses the seconds form of the Retry-After header. The
// HTTP-date form is intentionally not supported because it cannot be evaluated
// against a manual clock without a real wall-clock reference.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

// ComputeDelay returns the delay before the next attempt after the n-th failed
// attempt (n is 1-indexed). The base backoff is base*2^(n-1) capped at max.
//
// When retryAfter is greater than zero (a 429 carried a Retry-After), it
// overrides the backoff, itself capped at retryAfterMax. A zero retryAfterMax
// means Retry-After is honoured without an extra cap.
func ComputeDelay(n int, base, max, retryAfter, retryAfterMax time.Duration) time.Duration {
	if n < 1 {
		n = 1
	}
	backoff := base
	for i := 1; i < n; i++ {
		if backoff >= max {
			break
		}
		backoff *= 2
		// Guard against overflow on extreme attempt counts.
		if backoff < 0 {
			backoff = max
			break
		}
	}
	if backoff > max {
		backoff = max
	}
	if retryAfter > 0 {
		ra := retryAfter
		if retryAfterMax > 0 && ra > retryAfterMax {
			ra = retryAfterMax
		}
		return ra
	}
	return backoff
}
