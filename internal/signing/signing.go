// Package signing implements the HMAC-SHA256 request signing sent with every
// webhook delivery.
//
// The signed payload is the canonical string "<timestamp>.<body>" where
// <timestamp> is the decimal Unix-seconds value carried in the
// X-Courierbox-Timestamp header and <body> is the exact, unmodified request body
// delivered to the target. The signature header has the form "v1=<hex>".
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// Header names attached to every outbound delivery.
const (
	HeaderEvent     = "X-Courierbox-Event"
	HeaderDelivery  = "X-Courierbox-Delivery"
	HeaderTimestamp = "X-Courierbox-Timestamp"
	HeaderSignature = "X-Courierbox-Signature"
)

// Prefix is the version prefix emitted before the hex digest.
const Prefix = "v1="

// CanonicalString returns the value that is HMAC-signed: the decimal timestamp,
// a literal dot, and the raw body bytes.
func CanonicalString(timestamp int64, body []byte) string {
	return strconv.FormatInt(timestamp, 10) + "." + string(body)
}

// Sign returns the signature header value "v1=<hex>" for the given secret,
// timestamp and raw body.
func Sign(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(CanonicalString(timestamp, body)))
	return Prefix + hex.EncodeToString(mac.Sum(nil))
}

// Verify reports whether sigHeader (the full "v1=<hex>" value) matches the
// signature recomputed from secret, timestamp and body. The comparison is
// constant time.
func Verify(secret, sigHeader string, timestamp int64, body []byte) bool {
	expected := Sign(secret, timestamp, body)
	return hmac.Equal([]byte(expected), []byte(sigHeader))
}

// Headers returns the set of signing headers to attach to an outbound request.
// eventType is forwarded as X-Courierbox-Event, deliveryID as
// X-Courierbox-Delivery, and timestamp (Unix seconds) as X-Courierbox-Timestamp.
func Headers(eventType, deliveryID string, timestamp int64, body []byte, secret string) map[string]string {
	return map[string]string{
		HeaderEvent:     eventType,
		HeaderDelivery:  deliveryID,
		HeaderTimestamp: strconv.FormatInt(timestamp, 10),
		HeaderSignature: Sign(secret, timestamp, body),
	}
}
