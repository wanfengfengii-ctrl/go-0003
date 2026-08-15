package retry

import (
	"net/http"
	"testing"
	"time"
)

func TestClassifyResponse(t *testing.T) {
	cases := []struct {
		code       int
		retryAfter string
		retryable  bool
		category   Category
	}{
		{200, "", false, CategorySuccess},
		{204, "", false, CategorySuccess},
		{301, "", false, CategoryNonRetryableHTTP},
		{400, "", false, CategoryNonRetryableHTTP},
		{404, "", false, CategoryNonRetryableHTTP},
		{408, "", true, CategoryRetryableHTTP},
		{425, "", true, CategoryRetryableHTTP},
		{429, "7", true, CategoryRetryableHTTP},
		{500, "", true, CategoryRetryableHTTP},
		{503, "", true, CategoryRetryableHTTP},
	}
	for _, c := range cases {
		resp := &http.Response{StatusCode: c.code, Header: http.Header{}}
		if c.retryAfter != "" {
			resp.Header.Set("Retry-After", c.retryAfter)
		}
		o := ClassifyResponse(resp)
		if o.Retryable != c.retryable {
			t.Errorf("code %d: retryable = %v, want %v", c.code, o.Retryable, c.retryable)
		}
		if o.Category != c.category {
			t.Errorf("code %d: category = %s, want %s", c.code, o.Category, c.category)
		}
		if c.code == 429 && o.RetryAfter != 7*time.Second {
			t.Errorf("code 429: retry-after = %v, want 7s", o.RetryAfter)
		}
	}
}

func TestRetryAfterParsing(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"7", 7 * time.Second},
		{"0", 0},
		{"-1", 0},
		{"not-a-number", 0},
	}
	for _, c := range cases {
		resp := &http.Response{StatusCode: 429, Header: http.Header{}}
		resp.Header.Set("Retry-After", c.in)
		if got := ClassifyResponse(resp).RetryAfter; got != c.want {
			t.Errorf("Retry-After %q: got %v, want %v", c.in, got, c.want)
		}
	}
}

func TestComputeDelayExponential(t *testing.T) {
	base := 1 * time.Second
	max := 10 * time.Second
	cases := []struct {
		n    int
		want time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // capped
		{6, 10 * time.Second}, // still capped
	}
	for _, c := range cases {
		if got := ComputeDelay(c.n, base, max, 0, 0); got != c.want {
			t.Errorf("n=%d: got %v, want %v", c.n, got, c.want)
		}
	}
}

func TestComputeDelayRetryAfterOverrides(t *testing.T) {
	base := 1 * time.Second
	max := 10 * time.Second
	// Retry-After of 7s overrides the 1s backoff after the first attempt.
	if got := ComputeDelay(1, base, max, 7*time.Second, 0); got != 7*time.Second {
		t.Errorf("retry-after override: got %v, want 7s", got)
	}
	// Retry-After is capped by retryAfterMax.
	if got := ComputeDelay(1, base, max, 7*time.Second, 3*time.Second); got != 3*time.Second {
		t.Errorf("retry-after cap: got %v, want 3s", got)
	}
	// No retry-after falls back to backoff.
	if got := ComputeDelay(2, base, max, 0, 0); got != 2*time.Second {
		t.Errorf("no retry-after: got %v, want 2s", got)
	}
}

func TestComputeDelayOverflowSafe(t *testing.T) {
	// A very large attempt count must not panic or wrap negative.
	d := ComputeDelay(100, 1*time.Second, 10*time.Second, 0, 0)
	if d != 10*time.Second {
		t.Errorf("overflow case: got %v, want 10s", d)
	}
}
