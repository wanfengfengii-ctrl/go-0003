# Courierbox

> 可审计、可重放的 Webhook 投递服务（Auditable, replayable webhook delivery service）。

Courierbox is a persistent webhook delivery service written in Go. Callers register
delivery targets and submit JSON events over HTTP; the service deduplicates
submissions with idempotency keys, persists events, and delivers them
asynchronously with timestamped HMAC-SHA256 signatures and a full attempt
history. The delivery engine enforces global and per-target concurrency limits,
applies bounded exponential backoff to network errors, 429 and 5xx responses,
and routes exhausted or non-retryable deliveries to a dead queue that operators
can inspect, replay or discard.

The state machine is explicit — `queued → delivering → retry_wait → succeeded |
dead` — and survives process restarts: deliveries left in flight are recovered
to a retryable state on startup. A deterministic test control plane (manual
clock + `/_test/dispatch` + `/_test/advance`) lets every behaviour be reproduced
without real-time sleeps.

## Features

- **Idempotent intake** — `Idempotency-Key` scoped per target; identical payload
  returns the original event id; a reused key with a different payload returns
  `409 IDEMPOTENCY_CONFLICT`.
- **Signed deliveries** — HMAC-SHA256 over the raw request body; headers
  `X-Courierbox-Event`, `X-Courierbox-Delivery`, `X-Courierbox-Timestamp`,
  `X-Courierbox-Signature: v1=<hex>`. Secret rotation is supported.
- **Bounded retry** — network errors, 408/425/429/5xx are retried with
  `min(base*2^(n-1), max)` backoff; a 429 `Retry-After` overrides backoff within
  a configurable cap. Other 4xx go straight to the dead queue.
- **Multi-level concurrency** — a global semaphore and per-target semaphores;
  per-target overrides are hot-updatable without cancelling in-flight requests.
- **Dead queue & replay** — final status, truncated body, error category and
  attempt times are retained; single or batch replay creates a new delivery
  cycle while preserving history; duplicate replays deduplicate by operation key.
- **Crash recovery** — in-flight deliveries are recovered on startup; attempt
  numbers stay continuous and audit records are never duplicated.
- **Graceful shutdown** — on SIGTERM the service stops claiming, drains in-flight
  work within the deadline, then cancels and persists the remainder as
  recoverable retries before exiting.
- **Deterministic test plane** — `test_mode` enables a manual clock and the
  `/_test/*` routes; production mode returns `404` for them.

## Quick start

```bash
make build
./bin/courierbox -config config.example.json
```

Register a target and submit an event:

```bash
curl -s -X POST http://localhost:8080/v1/targets \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/hooks","secret":"topsecret"}'
# -> 201 {"id":"...","url":"https://example.com/hooks",...}

curl -s -X POST http://localhost:8080/v1/targets/<id>/events \
  -H 'Idempotency-Key: order-42' \
  -H 'X-Courierbox-Event-Type: order.created' \
  -d '{"order_id":42,"total":99}'
# -> 202 {"id":"...","status":"queued",...}
```

## Signature specification

Every delivery is signed with the target's current secret. The signed value is
the canonical string:

```
<timestamp>.<raw_body>
```

where `<timestamp>` is the decimal Unix-seconds value sent in the
`X-Courierbox-Timestamp` header and `<raw_body>` is the exact, unmodified
request body delivered to the target (byte-for-byte the submitted payload). The
signature is:

```
X-Courierbox-Signature: v1=<lowercase-hex(HMAC-SHA256(secret, canonical))>
```

Receivers verify with:

```go
expected := signing.Sign(secret, timestampUnix, body)
ok := hmac.Equal([]byte(received), []byte(expected))
```

`X-Courierbox-Event` carries the event type, `X-Courierbox-Delivery` carries the
attempt id, and `X-Courierbox-Timestamp` carries the signing timestamp.

## Configuration

Configuration is loaded from a JSON file (see `config.example.json`) and
overridden by `COURIERBOX_*` environment variables. Key fields:

| Field | Env | Default | Description |
|---|---|---|---|
| `db_path` | `COURIERBOX_DB_PATH` | `courierbox.db` | SQLite path (`:memory:` for ephemeral) |
| `addr` | `COURIERBOX_ADDR` | `:8080` | HTTP listen address |
| `test_mode` | `COURIERBOX_TEST_MODE` | `false` | enable the deterministic control plane |
| `global_concurrency` | `COURIERBOX_GLOBAL_CONCURRENCY` | `16` | max in-flight deliveries |
| `default_target_concurrency` | `COURIERBOX_DEFAULT_TARGET_CONCURRENCY` | `4` | per-target cap default |
| `base_delay` | `COURIERBOX_BASE_DELAY` | `1s` | first retry backoff |
| `max_delay` | `COURIERBOX_MAX_DELAY` | `5m` | backoff cap |
| `max_attempts` | `COURIERBOX_MAX_ATTEMPTS` | `25` | attempt ceiling |
| `retry_after_max` | `COURIERBOX_RETRY_AFTER_MAX` | `1m` | 429 Retry-After cap |
| `http_timeout` | `COURIERBOX_HTTP_TIMEOUT` | `30s` | per-request timeout |
| `max_payload_size` | `COURIERBOX_MAX_PAYLOAD_SIZE` | `1048576` | event body limit |
| `shutdown_timeout` | `COURIERBOX_SHUTDOWN_TIMEOUT` | `30s` | graceful drain deadline |

## HTTP API

| Method & path | Description |
|---|---|
| `POST /v1/targets` | register a target `{url, secret, max_concurrency?}` → `201` |
| `GET /v1/targets/{id}` | fetch a target |
| `PATCH /v1/targets/{id}` | update url/secret/max_concurrency (secret rotates) |
| `POST /v1/targets/{id}/events` | submit an event (raw body, `Idempotency-Key`, optional `X-Courierbox-Event-Type`) → `202` |
| `GET /v1/events/{id}` | event status |
| `GET /v1/events/{id}/attempts` | full attempt history |
| `GET /v1/dead?target_id=&limit=&offset=` | dead queue (paginated) |
| `POST /v1/dead/{id}/replay` | replay `{operation_key}` → new cycle |
| `POST /v1/dead/replay` | batch replay `{target_id, operation_key}` |
| `DELETE /v1/dead/{id}` | permanently discard |
| `GET /healthz` `/readyz` | liveness / readiness |

### Error codes

Every non-2xx response is JSON with a machine-decidable `error.code`:

| code | HTTP | meaning |
|---|---|---|
| `BAD_REQUEST` | 400 | malformed request |
| `MISSING_IDEMPOTENCY_KEY` | 400 | event submission without `Idempotency-Key` |
| `INVALID_JSON` | 400 | undecodable JSON body |
| `UNKNOWN_FIELD` | 400 | strict decoder rejected an unknown field |
| `PAYLOAD_TOO_LARGE` | 413 | body exceeded `max_payload_size` |
| `INVALID_URL` | 422 | target URL missing/unparseable/wrong scheme |
| `NOT_FOUND` | 404 | resource does not exist |
| `IDEMPOTENCY_CONFLICT` | 409 | key reused with a different payload |
| `CONFLICT` | 409 | operation not valid for current state |
| `INTERNAL` | 500 | unexpected internal error |

## Deterministic test control plane

When `test_mode` is enabled:

- `POST /_test/dispatch {"wait": bool}` — claim and dispatch due work; `wait=true`
  runs until the engine is quiescent, `wait=false` leaves in-flight requests
  running (used for concurrency observation).
- `POST /_test/advance {"duration": "1s"}` — advance the manual clock then run a
  dispatch cycle.
- `GET /_test/status` — current `now` and in-flight count.

When `test_mode` is off, all `/_test/*` routes return `404`.

## Project layout

```
cmd/courierbox        main entrypoint (config, signals, healthcheck mode)
internal/api          HTTP server, handlers, errors, test control plane
internal/app          composition root (store + engine + api)
internal/config       JSON + env configuration
internal/clock        Clock interface, real & manual clocks
internal/signing      HMAC-SHA256 request signing
internal/retry        outcome classification & backoff policy
internal/model        domain types & status enums
internal/store        Store interface + SQLite implementation
internal/engine       claim/dispatch/state machine/concurrency/recovery/shutdown
migrations            embedded SQL schema (001_init.sql)
testutil/receiver     scriptable test HTTP receiver
acceptance            end-to-end acceptance tests (public API only)
benzhi.Dockerfile     multi-stage non-root image
build_benzhi_docker.sh multi-arch buildx driver
```

## Development

```bash
make test        # go test ./...
make test-race   # go test -race ./...
make vet         # go vet ./...
make fmt-check   # gofmt check
```

## Docker

See `BENZHI_README.md` for full build/multi-arch details. In short:

```bash
./build_benzhi_docker.sh linux/amd64,linux/arm64 courierbox:latest
```

The image runs as a non-root user (`65532:65532`), ships a static binary built
with a full official Go toolchain (dependencies pre-downloaded, `go build ./...`
run as a compile check), and exposes a `HEALTHCHECK` via the binary's
`-healthcheck` mode.

## Persistence & recovery

State is held in SQLite with explicit transactions for every transition
(`migrations/001_init.sql`). Timestamps are stored as fixed-width UTC RFC3339
strings so lexical ordering matches chronological ordering. On startup,
`RecoverDelivering` resets any events left in `delivering` to `retry_wait` and
marks their `started` attempts as `interrupted`, so a crashed process never
leaves an event permanently in flight; the next attempt continues the numbering
sequence without duplicate audit records.
