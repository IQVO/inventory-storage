#!/usr/bin/env bash
# Chart render assertions for charts/inventory-storage (run in CI by the
# chart-tests job, next to test_service_selectors.py and test_kong_config.py).
#
# Postgres and Kafka are the only runtime: cmd/inventory exits at boot without
# DATABASE_URL, KAFKA_BROKERS, LOCATION_LOOKUP_MODE in {kafka,http} and the four
# consumer-group ids, and accepts EVENT_PUBLISHER / TRANSFER_ALLOCATION_CONSUMER_MODE /
# TASK_COMPLETED_CONSUMER_MODE only unset or kafka. The chart therefore refuses to
# render (helm template fails, naming the value) a release that would produce a pod
# that cannot boot, and still renders with the values warehouse-infra really passes
# (tests/warehouse-infra-*-values.yaml, verbatim copies of warehouse-infra
# origin/develop helm-values/).
#
# Run: bash charts/inventory-storage/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$chart_dir/../.." && pwd)"
release=inventory-storage
# Dummy DSN (no password) and broker: the chart refuses to render without them.
base=(--set database.url=postgres://u@example.invalid:5432/db
  --set kafka.brokers=kafka.invalid:9092)
everything=(--set analytics.enabled=true
  --set analytics.database.projectorUrl=postgres://a@example.invalid:5432/analytics
  --set mcp.enabled=true
  --set a2a.enabled=true)

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render() { helm template "$release" "$chart_dir" "${base[@]}" "$@"; }

# env_value DOC NAME: the value of the first `- name: NAME` entry in DOC.
env_value() {
  awk -v n="$2" '$0 ~ "- name: " n "$" {found=1; next} found && /value:/ {gsub(/^ *value: *"?|"?$/, ""); print; exit} found {exit}' <<<"$1"
}

# --- the chart renders a real runtime ------------------------------------
api_deployment="$(render --show-only templates/deployment.yaml)"
configmap="$(render --show-only templates/configmap.yaml)"
all_docs="$(render "${everything[@]}")"
configmap_name="$(awk '/^  name:/ {print $2; exit}' <<<"$configmap")"
[[ -n "$configmap_name" ]] || fail "could not read the ConfigMap name"

[[ "$(env_value "$api_deployment" EVENT_PUBLISHER)" == "kafka" ]] || fail "EVENT_PUBLISHER must default to kafka"
[[ "$(env_value "$api_deployment" LOCATION_LOOKUP_MODE)" == "kafka" ]] || fail "LOCATION_LOOKUP_MODE must default to kafka"
[[ "$(env_value "$api_deployment" TRANSFER_ALLOCATION_CONSUMER_MODE)" == "kafka" ]] || fail "TRANSFER_ALLOCATION_CONSUMER_MODE must default to kafka"
[[ "$(env_value "$api_deployment" TASK_COMPLETED_CONSUMER_MODE)" == "kafka" ]] || fail "TASK_COMPLETED_CONSUMER_MODE must default to kafka"
[[ "$(env_value "$api_deployment" TRANSFER_ALLOCATION_CONSUMER_GROUP)" == "inventory-storage-transfer-allocation" ]] || fail "TRANSFER_ALLOCATION_CONSUMER_GROUP default"
[[ "$(env_value "$api_deployment" PRODUCT_MASTER_CONSUMER_GROUP)" == "inventory-storage-product-master" ]] || fail "PRODUCT_MASTER_CONSUMER_GROUP default"
[[ "$(env_value "$api_deployment" INBOUND_RECEIPT_CONSUMER_GROUP)" == "inventory-storage-inbound-receipts" ]] || fail "INBOUND_RECEIPT_CONSUMER_GROUP default"
[[ "$(env_value "$api_deployment" TASK_COMPLETED_CONSUMER_GROUP)" == "inventory-storage-confirm-pick" ]] || fail "TASK_COMPLETED_CONSUMER_GROUP default"
[[ "$(env_value "$api_deployment" KAFKA_BROKERS)" == "kafka.invalid:9092" ]] || fail "KAFKA_BROKERS is not rendered from kafka.brokers"
grep -A4 'name: DATABASE_URL$' <<<"$api_deployment" | grep -q 'secretKeyRef:' || fail "DATABASE_URL must come from a Secret (secretKeyRef)"
if grep -qE '^ *(- )?(value|[A-Z_]+): "?(log|permissive|off)"?$' <<<"$all_docs"; then fail "the render carries a log/permissive/off value, which the binary rejects at boot"; fi
if grep -q 'DATABASE_URL: ""' <<<"$all_docs"; then fail "an empty DATABASE_URL must never be rendered"; fi
if grep -qi 'in-memory' <<<"$all_docs"; then fail "the render mentions an in-memory mode"; fi

# Every optional component renders its mandatory variable.
mcp_doc="$(render "${everything[@]}" --show-only templates/mcp-deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$mcp_doc" | grep -q 'secretKeyRef:' || fail "mcp must render DATABASE_URL from a Secret"
[[ "$(env_value "$mcp_doc" EVENT_PUBLISHER)" == "kafka" ]] || fail "mcp must render EVENT_PUBLISHER=kafka"
a2a_doc="$(render "${everything[@]}" --show-only templates/a2a-deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$a2a_doc" | grep -q 'secretKeyRef:' || fail "a2a must render DATABASE_URL from a Secret"
[[ "$(env_value "$a2a_doc" EVENT_PUBLISHER)" == "kafka" ]] || fail "a2a must render EVENT_PUBLISHER=kafka"
[[ "$(env_value "$a2a_doc" A2A_ADDR)" == ":8093" ]] || fail "the a2a agent must listen on :8093 (8090 is the MCP server, 8091 the projector admin, 8092 the reports API)"
grep -q 'command: \["/app/a2a"\]' <<<"$a2a_doc" || fail "the a2a Deployment must run /app/a2a"
# The Agent Card must advertise a dialable absolute URL: it defaults to the
# release's in-cluster Service DNS (namespace-derived, so pattern-match) and
# stays overridable.
a2a_url="$(env_value "$a2a_doc" A2A_AGENT_URL || true)" # a missing var must reach the fail message below, not abort under set -e
[[ "$a2a_url" =~ ^http://[a-z0-9.-]+-a2a\.[a-z0-9-]+\.svc\.cluster\.local:8093/$ ]] || fail "A2A_AGENT_URL default is not the in-cluster Service DNS: $a2a_url"
a2a_override="$(render --set a2a.enabled=true --set a2a.agentUrl=http://gw.example/api/a2a/inventory-storage/ --show-only templates/a2a-deployment.yaml)"
[[ "$(env_value "$a2a_override" A2A_AGENT_URL || true)" == "http://gw.example/api/a2a/inventory-storage/" ]] || fail "a2a.agentUrl does not override A2A_AGENT_URL"
# A2A is off by default: no a2a resource renders unless asked for.
if render --show-only templates/a2a-deployment.yaml 2>/dev/null | grep -q 'kind: Deployment'; then fail "the a2a Deployment rendered with default values"; fi
# Every variable cmd/a2a's boot config names as REQUIRED is rendered, derived
# from the Go source so a new mandatory variable fails until the chart has it.
a2a_vars="$(for f in "$repo_dir"/cmd/a2a/*.go; do [[ "$f" == *_test.go ]] || grep -ho '[A-Z][A-Z0-9_]\{2,\} is required' "$f" || true; done | sed -E 's/ is required//' | sort -u)"
[[ -n "$a2a_vars" ]] || fail "found no mandatory variables for cmd/a2a; the scan is looking in the wrong place"
for var in $a2a_vars; do
  grep -q "name: $var\$" <<<"$a2a_doc" || fail "the a2a Deployment does not render $var, which cmd/a2a requires"
done
proj_doc="$(render "${everything[@]}" --show-only templates/projector-deployment.yaml)"
grep -q 'name: ANALYTICS_DATABASE_URL$' <<<"$proj_doc" || fail "projector must render ANALYTICS_DATABASE_URL"
[[ "$(env_value "$proj_doc" KAFKA_BROKERS)" == "kafka.invalid:9092" ]] || fail "projector must render KAFKA_BROKERS"
reports_doc="$(render "${everything[@]}" --show-only templates/reports-deployment.yaml)"
grep -q 'name: ANALYTICS_DATABASE_URL$' <<<"$reports_doc" || fail "reports must render ANALYTICS_DATABASE_URL"

# D14 (2026-10-12): every config.* value is rendered into the chart ConfigMap,
# and the api Deployment -- the only workload whose binary (cmd/inventory)
# reads those keys (HTTP_ADDR, OUTBOX_RELAY_INTERVAL, OUTBOX_MAX_ATTEMPTS,
# HOUSEKEEPING_INTERVAL, ...) -- must mount it with envFrom, or a value that
# is only in the ConfigMap silently does nothing.
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q 'configMapRef:' \
  || fail "api Deployment does not mount the ConfigMap via envFrom"
grep -A3 'envFrom:' <<<"$api_deployment" | grep -q "name: $configmap_name" \
  || fail "api Deployment envFrom does not reference ConfigMap $configmap_name"

# A tuned config.* value reaches the ConfigMap the api mounts.
tuned="$(render --set config.outboxRelayInterval=7s --set config.outboxMaxAttempts=5 --show-only templates/configmap.yaml)"
grep -q 'OUTBOX_RELAY_INTERVAL: "7s"' <<<"$tuned" || fail "config.outboxRelayInterval is not rendered into the ConfigMap"
grep -q 'OUTBOX_MAX_ATTEMPTS: "5"' <<<"$tuned" || fail "config.outboxMaxAttempts is not rendered into the ConfigMap"
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS must default to 20 (ADR 0038)"
grep -q 'KAFKA_BROKERS: "kafka.invalid:9092"' <<<"$configmap" || fail "KAFKA_BROKERS must always be rendered into the ConfigMap"

# The other workloads read their own explicit env and must NOT inherit the
# api's HTTP_ADDR (reports would bind :8080) or OTEL_SERVICE_NAME from it.
for template in reports-deployment.yaml projector-deployment.yaml mcp-deployment.yaml a2a-deployment.yaml; do
  rendered="$(render "${everything[@]}" --show-only "templates/$template")"
  if grep -q 'configMapRef:' <<<"$rendered"; then
    fail "$template must not mount the api ConfigMap"
  fi
done

# --- the chart refuses to render a pod that cannot boot ------------------
expect_refusal() {
  local why="$1" needle="$2"
  shift 2
  local err
  if err="$(helm template "$release" "$chart_dir" "$@" 2>&1 >/dev/null)"; then
    fail "the chart must refuse to render: $why"
  fi
  grep -qF -- "$needle" <<<"$err" || fail "the refusal for '$why' does not name '$needle': $err"
}
db=(--set database.url=postgres://u@example.invalid:5432/db)
kafka=(--set kafka.brokers=kafka.invalid:9092)
# (1) database source
expect_refusal "no database source" "database.url or database.existingSecret" "${kafka[@]}"
expect_refusal "mcp without a database" "database.url or database.existingSecret" "${kafka[@]}" --set mcp.enabled=true
# (2) kafka
expect_refusal "a2a without a database" "database.url or database.existingSecret" "${kafka[@]}" --set a2a.enabled=true
expect_refusal "no kafka brokers" "kafka.brokers is required" "${db[@]}"
expect_refusal "empty kafka.brokers" "kafka.brokers is required" "${db[@]}" --set kafka.brokers=
expect_refusal "kafka.enabled=false" "kafka.enabled=false is not supported" "${base[@]}" --set kafka.enabled=false
# (3) publisher
expect_refusal "log publisher" "config.eventPublisher" "${base[@]}" --set config.eventPublisher=log
# (4) location lookup
expect_refusal "permissive lookup mode" "config.locationLookupMode" "${base[@]}" --set config.locationLookupMode=permissive
expect_refusal "empty lookup mode" "config.locationLookupMode" "${base[@]}" --set config.locationLookupMode=
expect_refusal "http lookup without a base URL" "config.facilityLayoutBaseUrl" "${base[@]}" --set config.locationLookupMode=http
# (5) consumer modes
expect_refusal "transfer allocation consumer off" "config.transferAllocationConsumerMode" "${base[@]}" --set config.transferAllocationConsumerMode=off
expect_refusal "task completed consumer off" "config.taskCompletedConsumerMode" "${base[@]}" --set config.taskCompletedConsumerMode=off
# (6) consumer groups
for group in transferAllocationConsumerGroup productMasterConsumerGroup inboundReceiptConsumerGroup taskCompletedConsumerGroup; do
  expect_refusal "empty $group" "config.$group" "${base[@]}" --set "config.$group="
done
# (7) analytics
expect_refusal "analytics without a DSN source" "analytics.database.projectorUrl or analytics.database.existingSecret" \
  "${base[@]}" --set analytics.enabled=true
# (8) extraEnv / mcp.extraEnv overrides
expect_refusal "EVENT_PUBLISHER=log in extraEnv" "EVENT_PUBLISHER" "${base[@]}" \
  --set 'extraEnv[0].name=EVENT_PUBLISHER' --set 'extraEnv[0].value=log'
expect_refusal "LOCATION_LOOKUP_MODE=permissive in extraEnv" "LOCATION_LOOKUP_MODE" "${base[@]}" \
  --set 'extraEnv[0].name=LOCATION_LOOKUP_MODE' --set 'extraEnv[0].value=permissive'
expect_refusal "TRANSFER_ALLOCATION_CONSUMER_MODE=off in extraEnv" "TRANSFER_ALLOCATION_CONSUMER_MODE" "${base[@]}" \
  --set 'extraEnv[0].name=TRANSFER_ALLOCATION_CONSUMER_MODE' --set 'extraEnv[0].value=off'
expect_refusal "TASK_COMPLETED_CONSUMER_MODE=off in extraEnv" "TASK_COMPLETED_CONSUMER_MODE" "${base[@]}" \
  --set 'extraEnv[0].name=TASK_COMPLETED_CONSUMER_MODE' --set 'extraEnv[0].value=off'
expect_refusal "empty TASK_COMPLETED_CONSUMER_GROUP in extraEnv" "TASK_COMPLETED_CONSUMER_GROUP" "${base[@]}" \
  --set 'extraEnv[0].name=TASK_COMPLETED_CONSUMER_GROUP' --set 'extraEnv[0].value='
expect_refusal "empty KAFKA_BROKERS in extraEnv" "KAFKA_BROKERS" "${base[@]}" \
  --set 'extraEnv[0].name=KAFKA_BROKERS' --set 'extraEnv[0].value='
expect_refusal "empty DATABASE_URL in extraEnv" "DATABASE_URL" "${base[@]}" \
  --set 'extraEnv[0].name=DATABASE_URL' --set 'extraEnv[0].value='
expect_refusal "EVENT_PUBLISHER=log in mcp.extraEnv" "mcp.extraEnv" "${base[@]}" \
  --set 'mcp.extraEnv[0].name=EVENT_PUBLISHER' --set 'mcp.extraEnv[0].value=log'
expect_refusal "empty DATABASE_URL in mcp.extraEnv" "mcp.extraEnv" "${base[@]}" \
  --set 'mcp.extraEnv[0].name=DATABASE_URL' --set 'mcp.extraEnv[0].value='
expect_refusal "EVENT_PUBLISHER=log in a2a.extraEnv" "a2a.extraEnv" "${base[@]}" \
  --set 'a2a.extraEnv[0].name=EVENT_PUBLISHER' --set 'a2a.extraEnv[0].value=log'
expect_refusal "empty DATABASE_URL in a2a.extraEnv" "a2a.extraEnv" "${base[@]}" \
  --set 'a2a.extraEnv[0].name=DATABASE_URL' --set 'a2a.extraEnv[0].value='

# Tolerated: the explicit real values, in any case, and unrelated extraEnv.
render --set kafka.enabled=true --set config.eventPublisher=KAFKA --set config.locationLookupMode=Kafka \
  --set config.transferAllocationConsumerMode=kafka --set config.taskCompletedConsumerMode= >/dev/null \
  || fail "the explicit real values must render"
render --set config.locationLookupMode=http --set config.facilityLayoutBaseUrl=http://facility-layout.invalid >/dev/null \
  || fail "locationLookupMode=http with facilityLayoutBaseUrl must render"
render --set 'extraEnv[0].name=EVENT_PUBLISHER' --set 'extraEnv[0].value=kafka' \
  --set 'extraEnv[1].name=SOME_OTHER_VAR' --set 'extraEnv[1].value=x' >/dev/null \
  || fail "EVENT_PUBLISHER=kafka and an unrelated extraEnv entry must render"

# --- the values warehouse-infra really passes still render --------------
# Same layering as the ArgoCD Application: _platform, then the app file.
infra=(--values "$chart_dir/tests/warehouse-infra-platform-values.yaml"
  --values "$chart_dir/tests/warehouse-infra-values.yaml")
infra_all="$(helm template "$release" "$chart_dir" "${infra[@]}")" || fail "the warehouse-infra values no longer render"
infra_api="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/deployment.yaml)"
[[ "$(env_value "$infra_api" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: KAFKA_BROKERS"
[[ "$(env_value "$infra_api" EVENT_PUBLISHER)" == "kafka" ]] || fail "infra values: EVENT_PUBLISHER"
[[ "$(env_value "$infra_api" LOCATION_LOOKUP_MODE)" == "kafka" ]] || fail "infra values: LOCATION_LOOKUP_MODE"
[[ "$(env_value "$infra_api" TRANSFER_ALLOCATION_CONSUMER_MODE)" == "kafka" ]] || fail "infra values: TRANSFER_ALLOCATION_CONSUMER_MODE"
[[ "$(env_value "$infra_api" TRANSFER_ALLOCATION_CONSUMER_GROUP)" == "inventory-storage-transfer-allocation" ]] || fail "infra values: TRANSFER_ALLOCATION_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" PRODUCT_MASTER_CONSUMER_GROUP)" == "inventory-storage-product-master" ]] || fail "infra values: PRODUCT_MASTER_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" INBOUND_RECEIPT_CONSUMER_GROUP)" == "inventory-storage-inbound-receipts" ]] || fail "infra values: INBOUND_RECEIPT_CONSUMER_GROUP"
[[ "$(env_value "$infra_api" TASK_COMPLETED_CONSUMER_MODE)" == "kafka" ]] || fail "infra values: TASK_COMPLETED_CONSUMER_MODE"
[[ "$(env_value "$infra_api" TASK_COMPLETED_CONSUMER_GROUP)" == "inventory-storage-confirm-pick" ]] || fail "infra values: TASK_COMPLETED_CONSUMER_GROUP"
grep -A4 'name: DATABASE_URL$' <<<"$infra_api" | grep -q 'inventory-storage-db' || fail "infra values: DATABASE_URL must come from the inventory-storage-db secret"
# A duplicate env entry breaks ArgoCD's diff.
dups="$(grep -E '^ {12}- name: ' <<<"$infra_api" | sort | uniq -d)"
[[ -z "$dups" ]] || fail "infra values: duplicate env entries in the api Deployment: $dups"
infra_mcp="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/mcp-deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$infra_mcp" | grep -q 'inventory-storage-db' || fail "infra values: mcp DATABASE_URL"
infra_a2a="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/a2a-deployment.yaml)" || fail "infra values: a2a"
grep -A4 'name: DATABASE_URL$' <<<"$infra_a2a" | grep -q 'inventory-storage-db' || fail "infra values: a2a DATABASE_URL"
helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/a2a-route.yaml | grep -q "replacePrefixMatch: /$" || fail "infra values: the a2a route must rewrite to /"
infra_proj="$(helm template "$release" "$chart_dir" "${infra[@]}" --show-only templates/projector-deployment.yaml)"
grep -A4 'name: ANALYTICS_DATABASE_URL$' <<<"$infra_proj" | grep -q 'inventory-storage-analytics-db' || fail "infra values: projector ANALYTICS_DATABASE_URL"
[[ "$(env_value "$infra_proj" KAFKA_BROKERS)" == "kafka.warehouse-systems.svc.cluster.local:9092" ]] || fail "infra values: projector KAFKA_BROKERS"
for component in api mcp a2a analytics-projector analytics-reports frontend; do
  grep -q "app.kubernetes.io/component: $component" <<<"$infra_all" || fail "infra values: component $component not rendered"
done

# The chart's own readinessProbe is /readyz (a real dependency check).
grep -A3 'readinessProbe:' <<<"$api_deployment" | grep -A2 'httpGet:' | grep -q 'path: /readyz' || fail "readinessProbe must hit /readyz"

echo "PASS: inventory-storage chart render assertions"
