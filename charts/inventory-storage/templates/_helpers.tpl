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
