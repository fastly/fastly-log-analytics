{{/*
Expand the name of the chart.
*/}}
{{- define "clickhouse.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "clickhouse.fullname" -}}
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
Common labels
*/}}
{{- define "clickhouse.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "clickhouse.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "clickhouse.selectorLabels" -}}
app.kubernetes.io/name: {{ include "clickhouse.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Secret name
*/}}
{{- define "clickhouse.secretName" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecret }}
{{- else }}
{{- printf "%s-auth" (include "clickhouse.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Validate topology settings
*/}}
{{- define "clickhouse.validate" -}}
{{- if and (not .Values.auth.existingSecret) (not .Values.auth.password) }}
{{- fail "auth.password is required when auth.existingSecret is unset" }}
{{- end }}
{{- if lt (int .Values.shards) 1 }}
{{- fail "shards must be at least 1" }}
{{- end }}
{{- if lt (int .Values.replicas) 1 }}
{{- fail "replicas must be at least 1" }}
{{- end }}
{{- if and (or (gt (int .Values.shards) 1) (gt (int .Values.replicas) 1)) (not .Values.keeper.enabled) }}
{{- fail "clustering requires keeper.enabled=true" }}
{{- end }}
{{- if and .Values.keeper.enabled (lt (int .Values.keeper.replicas) 1) }}
{{- fail "keeper.replicas must be at least 1" }}
{{- end }}
{{- if .Values.provisioner.enabled }}
{{- if not (regexMatch "^[A-Za-z_][A-Za-z0-9_]{0,63}$" .Values.provisioner.username) }}
{{- fail "provisioner.username must be a simple identifier" }}
{{- end }}
{{- if eq .Values.provisioner.username .Values.auth.username }}
{{- fail "provisioner.username must differ from auth.username" }}
{{- end }}
{{- if and (not .Values.auth.existingSecret) (not .Values.provisioner.password) }}
{{- fail "provisioner.password is required when provisioner.enabled and auth.existingSecret is unset" }}
{{- end }}
{{- end }}
{{- end }}
