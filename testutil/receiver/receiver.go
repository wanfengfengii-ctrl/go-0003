// Package receiver provides a scriptable local HTTP receiver for Courierbox
// acceptance tests. It can return a sequence of status codes, honour a
// Retry-After header, block requests behind a gate (for concurrency tests),
// respect request cancellation, and capture the raw headers and body of every
// request so signatures can be verified independently.
package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// Response is a scripted response to return from the receiver.
type Response struct {
	Status     int
	Body       string
	RetryAfter string // seconds, set as the Retry-After header
	Headers    map[string]string
}

// Capture holds a single received request.
type Capture struct {
	Path      string
	Method    string
	Headers   http.Header
	Body      []byte
	Timestamp string
	Signature string
	Event     string
	Delivery  string
	At        time.Time
}

// Receiver is a scriptable HTTP receiver.
type Receiver struct {
	server *http.Server
	url    string

	mu          sync.Mutex
	responses   []Response
	idx         int
	defaultResp Response

	captures []Capture

	gateClosed bool
	gateCh     chan struct{}

	inFlight     int
	peakInFlight int
	perPath      map[string]int
	peakPerPath  map[string]int
}

// New returns a Receiver listening on a random local port.
func New() *Receiver {
	r := &Receiver{
		defaultResp: Response{Status: http.StatusNoContent},
		gateCh:      make(chan struct{}),
		perPath:     make(map[string]int),
		peakPerPath: make(map[string]int),
	}
	close(r.gateCh) // open by default
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", r.handle)
	r.server = &http.Server{Handler: mux}
	r.url = "http://" + ln.Addr().String()
	go func() { _ = r.server.Serve(ln) }()
	return r
}

// URL returns the base URL of the receiver. Targets should use URL()+"/<tag>".
func (r *Receiver) URL() string { return r.url }

// SetResponses sets the scripted response sequence. After the sequence is
// exhausted the default response (204) is returned.
func (r *Receiver) SetResponses(rs ...Response) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.responses = rs
	r.idx = 0
}

// SetDefault sets the response used once the scripted sequence is exhausted.
func (r *Receiver) SetDefault(resp Response) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultResp = resp
}

// CloseGate makes every request block until OpenGate is called.
func (r *Receiver) CloseGate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gateClosed {
		return
	}
	r.gateClosed = true
	r.gateCh = make(chan struct{})
}

// OpenGate unblocks requests waiting in CloseGate mode.
func (r *Receiver) OpenGate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.gateClosed {
		return
	}
	r.gateClosed = false
	close(r.gateCh)
}

// Close shuts the receiver down.
func (r *Receiver) Close() {
	_ = r.server.Shutdown(context.Background())
}

// Captures returns a copy of all captured requests.
func (r *Receiver) Captures() []Capture {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Capture, len(r.captures))
	copy(out, r.captures)
	return out
}

// CaptureCount returns the number of captured requests.
func (r *Receiver) CaptureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.captures)
}

// LastCapture returns the most recent capture, or nil.
func (r *Receiver) LastCapture() *Capture {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.captures) == 0 {
		return nil
	}
	c := r.captures[len(r.captures)-1]
	return &c
}

// InFlight returns the current number of in-flight requests.
func (r *Receiver) InFlight() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inFlight
}

// PeakInFlight returns the peak total concurrency observed.
func (r *Receiver) PeakInFlight() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peakInFlight
}

// PeakInFlightFor returns the peak concurrency for a given path tag.
func (r *Receiver) PeakInFlightFor(path string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	// path may include a leading slash; normalise.
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	return r.peakPerPath[path]
}

func (r *Receiver) begin(path string) {
	r.mu.Lock()
	r.inFlight++
	if r.inFlight > r.peakInFlight {
		r.peakInFlight = r.inFlight
	}
	tag := pathTag(path)
	r.perPath[tag]++
	if r.perPath[tag] > r.peakPerPath[tag] {
		r.peakPerPath[tag] = r.perPath[tag]
	}
	r.mu.Unlock()
}

func (r *Receiver) end(path string) {
	r.mu.Lock()
	r.inFlight--
	if r.inFlight < 0 {
		r.inFlight = 0
	}
	tag := pathTag(path)
	r.perPath[tag]--
	r.mu.Unlock()
}

func pathTag(path string) string {
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	return path
}

func (r *Receiver) handle(w http.ResponseWriter, req *http.Request) {
	r.begin(req.URL.Path)
	defer r.end(req.URL.Path)

	body, _ := io.ReadAll(req.Body)
	req.Body.Close()

	// Capture.
	r.mu.Lock()
	c := Capture{
		Path:      req.URL.Path,
		Method:    req.Method,
		Headers:   req.Header.Clone(),
		Body:      body,
		Timestamp: req.Header.Get("X-Courierbox-Timestamp"),
		Signature: req.Header.Get("X-Courierbox-Signature"),
		Event:     req.Header.Get("X-Courierbox-Event"),
		Delivery:  req.Header.Get("X-Courierbox-Delivery"),
		At:        time.Now(),
	}
	r.captures = append(r.captures, c)
	gateCh := r.gateCh
	resp := r.nextLocked()
	r.mu.Unlock()

	// Block on the gate until opened or the request is cancelled.
	select {
	case <-gateCh:
	case <-req.Context().Done():
		return
	}

	for k, v := range resp.Headers {
		w.Header().Set(k, v)
	}
	if resp.RetryAfter != "" {
		w.Header().Set("Retry-After", resp.RetryAfter)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Status)
	_, _ = w.Write([]byte(resp.Body))
}

func (r *Receiver) nextLocked() Response {
	if r.idx < len(r.responses) {
		resp := r.responses[r.idx]
		r.idx++
		return resp
	}
	return r.defaultResp
}

// JSONBody is a helper to build a Response body from a value.
func JSONBody(v interface{}) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	_ = enc.Encode(v)
	return buf.String()
}

// Statuses builds a response script from a list of status codes.
func Statuses(codes ...int) []Response {
	out := make([]Response, len(codes))
	for i, c := range codes {
		out[i] = Response{Status: c}
	}
	return out
}

// String returns a short description for logging in tests.
func (r *Receiver) String() string {
	return fmt.Sprintf("receiver@%s", r.url)
}
