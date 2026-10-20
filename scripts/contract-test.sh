#!/usr/bin/env bash
# Contract-test the REST API with Schemathesis (property-based testing
# against apis/openapi.yaml): builds the service, boots it against the
# Postgres named by DATABASE_URL and the Kafka broker named by KAFKA_BROKERS
# on a loopback port, waits for /readyz, generates valid AND invalid requests
# for every operation, and asserts the responses conform to the spec (status
# codes, content types, response schemas; positive data accepted, negative
# data rejected).
#
# Mirrors the `contract` job in .github/workflows/ci.yml — same pinned
# Schemathesis version, same flags — so a local pass means a CI pass.
#
# This service has no in-memory mode (the real-wiring ADR): cmd/inventory
# requires DATABASE_URL (it migrates and serves from Postgres), KAFKA_BROKERS
# (the outbox relay, the consumers and /readyz), LOCATION_LOOKUP_MODE and four
# consumer-group ids. CI provides a postgres:16 and a Kafka service container;
# for a local run, start disposable ones first (the broker must advertise the
# address you pass in KAFKA_BROKERS):
#
#   docker run -d --rm --name inv-contract-pg -p 18099:5432 \
#     -e POSTGRES_USER=inventory -e POSTGRES_PASSWORD=inventory \
#     -e POSTGRES_DB=inventory postgres:16
#   docker run -d --rm --name inv-contract-kafka -p 19099:9092 \
#     -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:19099 \
#     confluentinc/confluent-local:7.6.1
#   export DATABASE_URL='postgres://inventory:inventory@localhost:18099/inventory?sslmode=disable'
#   export KAFKA_BROKERS='localhost:19099'
#
# (credentials/URL shape mirror the CI `contract` job's postgres service).
#
# Requires `st` on PATH:
#   python3 -m pip install --user 'schemathesis==4.28.0'
set -euo pipefail

SCHEMATHESIS_VERSION="4.28.0"
PORT="${CONTRACT_PORT:-18080}"
BASE_URL="http://127.0.0.1:${PORT}"
MAX_EXAMPLES="${CONTRACT_MAX_EXAMPLES:-100}"

if ! command -v st >/dev/null 2>&1; then
  echo "schemathesis (st) is not installed (or not on PATH)."
  echo "Install the exact version CI pins:"
  echo "  python3 -m pip install --user 'schemathesis==${SCHEMATHESIS_VERSION}'"
  exit 1
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL is not set — this service has no in-memory mode"
  echo "(cmd/inventory requires Postgres and applies migrations on boot)."
  echo "Start a disposable one and export DATABASE_URL, e.g.:"
  echo "  docker run -d --rm --name inv-contract-pg -p 18099:5432 \\"
  echo "    -e POSTGRES_USER=inventory -e POSTGRES_PASSWORD=inventory \\"
  echo "    -e POSTGRES_DB=inventory postgres:16"
  echo "  export DATABASE_URL='postgres://inventory:inventory@localhost:18099/inventory?sslmode=disable'"
  exit 1
fi

if [ -z "${KAFKA_BROKERS:-}" ]; then
  echo "KAFKA_BROKERS is not set — this service has no log sink and no optional relay"
  echo "(cmd/inventory requires a real Kafka broker; /readyz depends on it)."
  echo "Start a disposable one and export KAFKA_BROKERS, e.g.:"
  echo "  docker run -d --rm --name inv-contract-kafka -p 19099:9092 \\"
  echo "    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:19099 \\"
  echo "    confluentinc/confluent-local:7.6.1"
  echo "  export KAFKA_BROKERS='localhost:19099'"
  exit 1
fi

cd "$(dirname "$0")/.."
BIN="$(mktemp -d)/inventory"
go build -o "$BIN" ./cmd/inventory

# The old in-memory boot started EMPTY (no seed data), and so does this one:
# the migrations create the schema and insert no rows, so Schemathesis still
# starts from an empty store and creates what it needs itself. The four
# consumer groups and the location lookup mode are mandatory; the consumed
# topics are auto-created by the broker, so the consumers simply idle.
export LOCATION_LOOKUP_MODE="${LOCATION_LOOKUP_MODE:-kafka}"
export TRANSFER_ALLOCATION_CONSUMER_GROUP="${TRANSFER_ALLOCATION_CONSUMER_GROUP:-inventory-storage-contract-transfer}"
export PRODUCT_MASTER_CONSUMER_GROUP="${PRODUCT_MASTER_CONSUMER_GROUP:-inventory-storage-contract-product-master}"
export INBOUND_RECEIPT_CONSUMER_GROUP="${INBOUND_RECEIPT_CONSUMER_GROUP:-inventory-storage-contract-receipts}"
export TASK_COMPLETED_CONSUMER_GROUP="${TASK_COMPLETED_CONSUMER_GROUP:-inventory-storage-contract-confirm-pick}"
LOG_LEVEL="${LOG_LEVEL:-warn}" HTTP_ADDR="127.0.0.1:${PORT}" "$BIN" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait for /readyz: it is 200 only while Postgres answers a ping AND the
# broker answers a metadata request (up to ~60s: migrations + broker + the
# facility location cache's initial replay).
for _ in $(seq 1 300); do
  if curl -sf "${BASE_URL}/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf "${BASE_URL}/readyz" >/dev/null # fail loudly if it never became ready

# The Reports tag (/reports/*) is excluded because those operations are served
# by the separate inventory-reports binary (read-only analytical pool), not by
# the OLTP binary booted here.
#
# stageTransferReceipt/stowTransferStock are excluded from the
# positive-data-rejected check only via their documented 422/409: a WELL-FORMED
# stage (positive data) is still QUARANTINED with 422 when the
# transfer_allocations ledger has no ALLOCATED row for the line — the exact
# domain behavior ADR-0031 requires (an unrecognized scan must never raise
# stock). Schemathesis 4.28's positive_data_accepted check cannot model
# "valid shape, refused by domain state", so the operations are excluded from
# that check; the endpoint's contract (status codes, schemas, problem shapes)
# is still exercised by the unit and integration suites
# (transfer_receipt_handler_test.go, transfer_receipt_integration_test.go).
# relocateStock (POST /relocations) is excluded for the same reason as the
# transfer ops above: Schemathesis's positive-data-acceptance check cannot
# model "valid shape, refused by a domain invariant". A schema-compliant
# body with fromBin == toBin is refused with the documented 422 same-bin
# problem (ADR 0010's invariant: a relocation's source and destination bin
# must differ), which the fuzzer reads as a rejected valid request. The
# 422 itself is documented on the operation and pinned by the unit and BDD
# suites (relocation tests, features/).
#
# Idempotency-Key (ADR-0016, enforced in every deployment now): the hooks file
# gives every request that carries the header a fresh UUID, like a real client
# sending one per logical submit (see scripts/contract_hooks.py).
#
# --generation-allow-x00 false: the schemas cannot say "no U+0000", and
# PostgreSQL text cannot hold one. The service answers a NUL in a body, a
# query or a path with a client error before any other middleware
# (internal/adapters/inbound/http/nul.go, pinned by nul_test.go), which
# Schemathesis would otherwise report as "API rejected schema-compliant
# request".
export SCHEMATHESIS_HOOKS="scripts/contract_hooks.py"
st run apis/openapi.yaml \
  --url "${BASE_URL}" \
  --max-examples "${MAX_EXAMPLES}" \
  --workers 4 \
  --generation-allow-x00 false \
  --exclude-operation-id stageTransferReceipt \
  --exclude-operation-id stowTransferStock \
  --exclude-operation-id relocateStock \
  --exclude-tag Reports
