package receiver_test

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"courierbox/testutil/receiver"
)

func TestReceiverScriptSequence(t *testing.T) {
	r := receiver.New()
	defer r.Close()
	r.SetResponses(
		receiver.Response{Status: 500},
		receiver.Response{Status: 204},
	)
	for i := 0; i < 2; i++ {
		resp, err := http.Post(r.URL()+"/x", "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && resp.StatusCode != 500 {
			t.Fatalf("first status = %d, want 500", resp.StatusCode)
		}
		if i == 1 && resp.StatusCode != 204 {
			t.Fatalf("second status = %d, want 204", resp.StatusCode)
		}
		resp.Body.Close()
	}
	if c := r.CaptureCount(); c != 2 {
		t.Fatalf("captures = %d, want 2", c)
	}
}

func TestReceiverDefaultAfterExhausted(t *testing.T) {
	r := receiver.New()
	defer r.Close()
	r.SetResponses(receiver.Response{Status: 418})
	r.SetDefault(receiver.Response{Status: 200})
	// First: scripted 418.
	resp, _ := http.Post(r.URL()+"/x", "application/json", nil)
	if resp.StatusCode != 418 {
		t.Fatalf("first = %d, want 418", resp.StatusCode)
	}
	resp.Body.Close()
	// Next: default 200.
	resp, _ = http.Post(r.URL()+"/x", "application/json", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("second = %d, want 200 (default)", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestReceiverGateAndPeaks(t *testing.T) {
	r := receiver.New()
	defer r.Close()
	r.CloseGate()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(r.URL()+"/p", "application/json", nil)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}

	// Wait until all 5 are in flight at the gate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.InFlight() == 5 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if r.InFlight() != 5 {
		t.Fatalf("in flight = %d, want 5", r.InFlight())
	}
	if p := r.PeakInFlight(); p != 5 {
		t.Fatalf("peak = %d, want 5", p)
	}
	if p := r.PeakInFlightFor("/p"); p != 5 {
		t.Fatalf("peak /p = %d, want 5", p)
	}

	r.OpenGate()
	wg.Wait()
	if p := r.PeakInFlight(); p != 5 {
		t.Fatalf("final peak = %d, want 5", p)
	}
}

func TestReceiverCapturesHeadersAndBody(t *testing.T) {
	r := receiver.New()
	defer r.Close()
	req, _ := http.NewRequest("POST", r.URL()+"/sig", stringReader(`{"b":1}`))
	req.Header.Set("X-Courierbox-Signature", "v1=abc")
	req.Header.Set("X-Courierbox-Timestamp", "123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	c := r.LastCapture()
	if c == nil {
		t.Fatal("no capture")
	}
	if c.Signature != "v1=abc" {
		t.Fatalf("signature = %q", c.Signature)
	}
	if c.Timestamp != "123" {
		t.Fatalf("timestamp = %q", c.Timestamp)
	}
	if string(c.Body) != `{"b":1}` {
		t.Fatalf("body = %q", c.Body)
	}
}

func stringReader(s string) *strings.Reader {
	return strings.NewReader(s)
}
