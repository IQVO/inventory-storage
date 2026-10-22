{{/*
Expand the name of the chart.
*/}}
{{- define "inventory-storage.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "inventory-storage.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "inventory-storage.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "inventory-storage.labels" -}}
helm.sh/chart: {{ include "inventory-storage.chart" . }}
{{ include "inventory-storage.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "inventory-storage.selectorLabels" -}}
app.kubernetes.io/name: {{ include "inventory-storage.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "inventory-storage.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "inventory-storage.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding DATABASE_URL, when the chart creates its own.
*/}}
{{- define "inventory-storage.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "inventory-storage.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
Fully qualified name of the analytics projector deployment (ADR-0011).
*/}}
{{- define "inventory-storage.projectorFullname" -}}
{{- include "inventory-storage.fullname" . }}-projector
{{- end }}

{{/*
Fully qualified name of the analytics reports deployment/service (ADR-0011).
*/}}
{{- define "inventory-storage.reportsFullname" -}}
{{- include "inventory-storage.fullname" . }}-reports
{{- end }}

{{/*
Name of the Secret holding the analytics DSNs, when the chart creates its own.
*/}}
{{- define "inventory-storage.analyticsSecretName" -}}
{{- if .Values.analytics.database.existingSecret }}
{{- .Values.analytics.database.existingSecret }}
{{- else }}
{{- include "inventory-storage.fullname" . }}-analytics
{{- end }}
{{- end }}

{{/*
Fully qualified name of the MCP server deployment/service (ADR-0008).
*/}}
{{- define "inventory-storage.mcpFullname" -}}
{{- include "inventory-storage.fullname" . }}-mcp
{{- end }}

{{/*
Fully qualified name of the frontend Module Federation remote deployment/service.

The remote is served by its own nginx pod and reached through warehouse-infra's
Nginx web gateway at /mfes/inventory-storage/. It is deliberately a separate
workload from the API: Kong never routes to it, and the OLTP Service must never
select it.
*/}}
{{- define "inventory-storage.frontendFullname" -}}
{{- include "inventory-storage.fullname" . }}-frontend
{{- end }}

{{/*
Postgres and Kafka are the only runtime. cmd/inventory (loadConfig) exits at
boot without DATABASE_URL, KAFKA_BROKERS, LOCATION_LOOKUP_MODE in {kafka,http}
and the four consumer-group ids; EVENT_PUBLISHER and the two *_CONSUMER_MODE
variables are accepted only unset or "kafka". cmd/mcp needs DATABASE_URL (and
accepts EVENT_PUBLISHER only unset/kafka); cmd/inventory-projector needs
ANALYTICS_DATABASE_URL and KAFKA_BROKERS. The chart therefore refuses to render
a release that would produce a pod that cannot boot, naming the value.
*/}}
{{- define "inventory-storage.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "database.url or database.existingSecret is required: DATABASE_URL is mandatory, this service runs only on a real Postgres" -}}
{{- end -}}
{{- end }}

{{/*
kafka.enabled=false is refused and kafka.brokers is required (KAFKA_BROKERS).
*/}}
{{- define "inventory-storage.requireKafka" -}}
{{- if not .Values.kafka.enabled -}}
{{- fail "kafka.enabled=false is not supported: this service runs only on a real Kafka; leave kafka.enabled at true" -}}
{{- end -}}
{{- $_ := required "kafka.brokers is required (KAFKA_BROKERS): every binary of this chart runs on a real Kafka" .Values.kafka.brokers -}}
{{- end }}

{{/*
config.eventPublisher: empty or kafka (case-insensitive), never log.
*/}}
{{- define "inventory-storage.requireEventPublisher" -}}
{{- $v := lower (trim (toString (default "" .Values.config.eventPublisher))) -}}
{{- if and (ne $v "") (ne $v "kafka") -}}
{{- fail (printf "config.eventPublisher=%q is not supported: this service publishes through the outbox to a real Kafka, the only value is kafka (or empty)" $v) -}}
{{- end -}}
{{- end }}

{{/*
config.locationLookupMode: kafka or http; permissive and empty were removed
because they validate no location at all. http also needs the base URL.
*/}}
{{- define "inventory-storage.requireLocationLookup" -}}
{{- $v := lower (trim (toString (default "" .Values.config.locationLookupMode))) -}}
{{- if not (or (eq $v "kafka") (eq $v "http")) -}}
{{- fail (printf "config.locationLookupMode=%q is not supported: set kafka (facility-events cache) or http (facility-layout REST); permissive and empty were removed" $v) -}}
{{- end -}}
{{- if and (eq $v "http") (not .Values.config.facilityLayoutBaseUrl) -}}
{{- fail "config.facilityLayoutBaseUrl is required when config.locationLookupMode=http (FACILITY_LAYOUT_BASE_URL)" -}}
{{- end -}}
{{- end }}

{{/*
The transfer-allocation and TaskCompleted consumers always run: the mode is
empty or kafka, never off.
*/}}
{{- define "inventory-storage.requireConsumers" -}}
{{- range $key := list "transferAllocationConsumerMode" "taskCompletedConsumerMode" -}}
{{- $v := lower (trim (toString (default "" (index $.Values.config $key)))) -}}
{{- if and (ne $v "") (ne $v "kafka") -}}
{{- fail (printf "config.%s=%q is not supported: the consumer always runs, the only value is kafka (or empty); off was removed" $key $v) -}}
{{- end -}}
{{- end -}}
{{- range $key := list "transferAllocationConsumerGroup" "productMasterConsumerGroup" "inboundReceiptConsumerGroup" "returnLineConsumerGroup" "taskCompletedConsumerGroup" -}}
{{- if not (trim (toString (default "" (index $.Values.config $key)))) -}}
{{- fail (printf "config.%s is required: the consumer always runs and needs a STABLE consumer group id (a restarted pod resumes from its committed offsets)" $key) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
inventory-projector and inventory-reports exit at boot without
ANALYTICS_DATABASE_URL; the projector also needs KAFKA_BROKERS (requireKafka).
*/}}
{{- define "inventory-storage.requireAnalytics" -}}
{{- if and .Values.analytics.enabled (not (or .Values.analytics.database.projectorUrl .Values.analytics.database.existingSecret)) -}}
{{- fail "analytics.database.projectorUrl or analytics.database.existingSecret is required while analytics.enabled: ANALYTICS_DATABASE_URL is mandatory for the projector and the reports API" -}}
{{- end -}}
{{- end }}

{{/*
extraEnv / mcp.extraEnv must not override a guarded variable with a value the
binary rejects at boot (or with an empty literal for a mandatory one).
valueFrom entries are not inspected. Call with (dict "env" <list> "where" <name>).
*/}}
{{- define "inventory-storage.requireRealEnv" -}}
{{- $where := .where -}}
{{- range .env -}}
{{- if hasKey . "value" -}}
{{- $name := .name -}}
{{- $v := lower (trim (toString .value)) -}}
{{- if and (eq $name "EVENT_PUBLISHER") (ne $v "") (ne $v "kafka") -}}
{{- fail (printf "%s sets EVENT_PUBLISHER=%s: not supported, this service runs only on a real Kafka and the binary rejects it at boot" $where (toString .value)) -}}
{{- end -}}
{{- if and (or (eq $name "TRANSFER_ALLOCATION_CONSUMER_MODE") (eq $name "TASK_COMPLETED_CONSUMER_MODE")) (ne $v "") (ne $v "kafka") -}}
{{- fail (printf "%s sets %s=%s: not supported, the consumer always runs and the binary rejects it at boot" $where $name (toString .value)) -}}
{{- end -}}
{{- if and (eq $name "LOCATION_LOOKUP_MODE") (ne $v "kafka") (ne $v "http") -}}
{{- fail (printf "%s sets LOCATION_LOOKUP_MODE=%s: not supported, only kafka or http, the binary rejects it at boot" $where (toString .value)) -}}
{{- end -}}
{{- if and (has $name (list "DATABASE_URL" "KAFKA_BROKERS" "TRANSFER_ALLOCATION_CONSUMER_GROUP" "PRODUCT_MASTER_CONSUMER_GROUP" "INBOUND_RECEIPT_CONSUMER_GROUP" "RETURN_LINE_CONSUMER_GROUP" "TASK_COMPLETED_CONSUMER_GROUP")) (eq $v "") -}}
{{- fail (printf "%s sets %s to an empty value: it is mandatory, the binary rejects it at boot" $where $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
One entry point for the api Deployment, which every release renders, so any
release fails whichever optional component is enabled.
*/}}
{{- define "inventory-storage.requireRealRuntime" -}}
{{- include "inventory-storage.requireDatabase" . -}}
{{- include "inventory-storage.requireKafka" . -}}
{{- include "inventory-storage.requireEventPublisher" . -}}
{{- include "inventory-storage.requireLocationLookup" . -}}
{{- include "inventory-storage.requireConsumers" . -}}
{{- include "inventory-storage.requireAnalytics" . -}}
{{- include "inventory-storage.requireRealEnv" (dict "env" .Values.extraEnv "where" "extraEnv") -}}
{{- include "inventory-storage.requireRealEnv" (dict "env" .Values.mcp.extraEnv "where" "mcp.extraEnv") -}}
{{- end }}

{{/*
Comma-joined names of this chart's enabled KongPlugins, for the
konghq.com/plugins annotation on every HTTPRoute this chart renders.
Empty string (annotation omitted) when none are enabled — standalone
installs without Kong are unaffected.
*/}}
{{- define "inventory-storage.kongPluginNames" -}}
{{- $names := list -}}
{{- range $name, $cfg := .Values.kongPlugins -}}
{{- if $cfg.enabled -}}
{{- $names = append $names (printf "%s-%s" (include "inventory-storage.fullname" $) $name) -}}
{{- end -}}
{{- end -}}
{{- join "," $names -}}
{{- end }}
