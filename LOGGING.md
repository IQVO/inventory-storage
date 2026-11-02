# Structured Logging

This service uses Go's standard library [`log/slog`](https://pkg.go.dev/log/slog)
for structured, leveled logging — no third-party logging dependency.

## Why `log/slog`

- **Zero new dependency.** `log/slog` shipped in the Go 1.21 standard library;
  no extra module to vet, pin, or upgrade.
- **Ecosystem standard.** It is the de facto structured-logging API for
  modern Go services and libraries increasingly accept/return `*slog.Logger`.
- **OpenTelemetry-bridge ready.** `slog.Handler` composes cleanly with
  OTel log bridges (e.g. `go.opentelemetry.io/contrib/bridges/otelslog`) if
  trace-correlated logging is added later, without changing call sites.
- **zerolog / zap considered, not needed.** Both are faster under very high
  throughput, but this is a study project with modest request volume; the
  stdlib option meets the need without an extra dependency to justify.

## Configuration

- **`LOG_LEVEL`** (env var): `debug` | `info` | `warn` | `error`,
  case-insensitive. Defaults to `info` when unset or unrecognized.

## Format

Logs are JSON, written to stdout, via `slog.NewJSONHandler`. Each line is a
single JSON object with at least `time`, `level`, `msg`, and any structured
attributes attached to the call (e.g. `event_name`, `payload`, `error`).

Example:

```json
{"time":"2026-08-23T12:00:00Z","level":"INFO","msg":"http server listening","addr":":8080"}
{"time":"2026-08-23T12:00:01Z","level":"INFO","msg":"http request","method":"POST","path":"/stock-receipts","status":202,"duration_ms":3,"bytes":128,"request_id":"abcd1234"}
{"time":"2026-08-23T12:00:01Z","level":"INFO","msg":"domain event published","event_name":"StockReceived","payload":{"sku":"SKU-1","quantity":10}}
```

## Where it's wired in

- `cmd/inventory/main.go`: builds the process-wide `*slog.Logger` via
  `newLogger(LOG_LEVEL)` and calls `slog.SetDefault(logger)` as the first
  step of `run()`. Startup/shutdown and adapter-wiring messages use it.
- `internal/adapters/inbound/http/logging.go`: `RequestLogger` chi
  middleware logs every HTTP request (method, path, status, duration_ms,
  bytes, request_id) at `Info`, or `Error` for 5xx responses. Installed in
  `NewRouter` after `middleware.RequestID` and before `middleware.Recoverer`.
- `internal/adapters/outbound/events/log_publisher.go`: `LogPublisher` logs
  each published domain event as a structured `Info` entry
  (`event_name`, `payload`) instead of a formatted string.
