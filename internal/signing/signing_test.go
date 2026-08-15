package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
)

func TestSignMatchesIndependentComputation(t *testing.T) {
	secret := "s3cret"
	body := []byte(`{"event":"order.created","id":42}`)
	ts := int64(1700000000)

	got := Sign(secret, ts, body)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10) + "." + string(body)))
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))

	if got != want {
		t.Fatalf("signature mismatch:\n got %q\nwant %q", got, want)
	}
	if !Verify(secret, got, ts, body) {
		t.Fatal("Verify rejected a valid signature")
	}
}

func TestVerifyRejectsTamperedBody(t *testing.T) {
	secret := "s3cret"
	body := []byte(`{"a":1}`)
	ts := int64(1)
	sig := Sign(secret, ts, body)
	if Verify(secret, sig, ts, []byte(`{"a":2}`)) {
		t.Fatal("Verify accepted a signature for a different body")
	}
	if Verify("other", sig, ts, body) {
		t.Fatal("Verify accepted a signature for a different secret")
	}
	if Verify(secret, "v1=deadbeef", ts, body) {
		t.Fatal("Verify accepted a bogus signature")
	}
}

func TestHeadersContainAllRequiredHeaders(t *testing.T) {
	h := Headers("order.created", "del-123", 1700000000, []byte("body"), "k")
	for _, k := range []string{HeaderEvent, HeaderDelivery, HeaderTimestamp, HeaderSignature} {
		if _, ok := h[k]; !ok {
			t.Errorf("missing header %q", k)
		}
	}
	if h[HeaderEvent] != "order.created" {
		t.Errorf("event header = %q", h[HeaderEvent])
	}
	if h[HeaderDelivery] != "del-123" {
		t.Errorf("delivery header = %q", h[HeaderDelivery])
	}
	if h[HeaderTimestamp] != "1700000000" {
		t.Errorf("timestamp header = %q", h[HeaderTimestamp])
	}
	if !Verify("k", h[HeaderSignature], 1700000000, []byte("body")) {
		t.Fatal("header signature failed verification")
	}
}
