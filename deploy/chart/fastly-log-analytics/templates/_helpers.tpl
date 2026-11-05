{{/*
Expand the name of the chart.
*/}}
{{- define "fastly-log-analytics.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "fastly-log-analytics.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "fastly-log-analytics.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Name of the ServiceAccount to use.
*/}}
{{- define "fastly-log-analytics.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "fastly-log-analytics.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding CELERY_BROKER_URL / METADATA_DSN.
*/}}
{{- define "fastly-log-analytics.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- printf "%s-secrets" (include "fastly-log-analytics.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Whether this chart templates its own connection-string Secret.
*/}}
{{- define "fastly-log-analytics.managedSecret" -}}
{{- if .Values.secrets.existingSecret -}}
{{- else if or .Values.secrets.metadataDsn .Values.secrets.celeryBrokerUrl .Values.secrets.clickhousePassword -}}
true
{{- end -}}
{{- end }}

{{/*
Whether a Secret with the connection strings is available at all (pre-created
or chart-managed).
*/}}
{{- define "fastly-log-analytics.hasSecret" -}}
{{- if or .Values.secrets.existingSecret (include "fastly-log-analytics.managedSecret" .) -}}
true
{{- end -}}
{{- end }}

{{/*
Name of the PVC holding configs/data.
*/}}
{{- define "fastly-log-analytics.pvcName" -}}
{{- if .Values.persistence.existingClaim }}
{{- .Values.persistence.existingClaim }}
{{- else }}
{{- printf "%s-data" (include "fastly-log-analytics.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Env block shared by backend and worker: broker/DSN via secretKeyRef
plus ClickHouse analytical store settings.
*/}}
{{- define "fastly-log-analytics.sharedEnv" -}}
- name: CLICKHOUSE_ENABLED
  value: "true"
{{- if .Values.config.clickhouseUrl }}
- name: CLICKHOUSE_URL
  value: {{ .Values.config.clickhouseUrl | quote }}
- name: HIGH_SCALE_CLICKHOUSE_URL
  value: {{ .Values.config.clickhouseUrl | quote }}
{{- end }}
{{- if .Values.config.clickhouseHost }}
- name: CLICKHOUSE_HOST
  value: {{ .Values.config.clickhouseHost | quote }}
{{- end }}
{{- if .Values.config.clickhousePort }}
- name: CLICKHOUSE_PORT
  value: {{ .Values.config.clickhousePort | quote }}
{{- end }}
{{- if .Values.config.clickhouseDatabase }}
- name: CLICKHOUSE_DATABASE
  value: {{ .Values.config.clickhouseDatabase | quote }}
{{- end }}
{{- if .Values.config.clickhouseUser }}
- name: CLICKHOUSE_USER
  value: {{ .Values.config.clickhouseUser | quote }}
{{- end }}
{{- if .Values.config.clickhouseSecure }}
- name: CLICKHOUSE_SECURE
  value: {{ .Values.config.clickhouseSecure | quote }}
{{- end }}
{{- if include "fastly-log-analytics.hasSecret" . }}
- name: CELERY_BROKER_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "fastly-log-analytics.secretName" . }}
      key: CELERY_BROKER_URL
      optional: true
- name: METADATA_DSN
  valueFrom:
    secretKeyRef:
      name: {{ include "fastly-log-analytics.secretName" . }}
      key: METADATA_DSN
      optional: {{ not (or .Values.secrets.existingSecret .Values.secrets.metadataDsn) }}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "fastly-log-analytics.secretName" . }}
      key: CLICKHOUSE_PASSWORD
      optional: true
{{- end }}
{{- if .Values.config.hotS3Endpoint }}
- name: HOT_S3_ENDPOINT
  value: {{ .Values.config.hotS3Endpoint | quote }}
{{- end }}
{{- end }}

{{/*
OTEL exporter selection shared by the backend and worker. Explicit
operator settings override the chart defaults; otherwise OTLP targets the
in-cluster Prometheus/Tempo when observability is on and Prometheus scraping
is used when it is off.
*/}}
{{- define "fastly-log-analytics.otelEnv" -}}
- name: OTEL_EXPORTER
{{- if .Values.otel.exporter }}
  value: {{ .Values.otel.exporter | quote }}
{{- else if or .Values.observability.enabled .Values.global.privateObservability.enabled }}
  value: "otlp"
{{- else }}
  value: "prometheus"
{{- end }}
{{- if .Values.otel.exporter }}
{{- if eq .Values.otel.exporter "otlp" }}
- name: OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
  value: {{ .Values.otel.metricsEndpoint | quote }}
- name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
  value: {{ .Values.otel.tracesEndpoint | quote }}
{{- end }}
{{- else if or .Values.observability.enabled .Values.global.privateObservability.enabled }}
- name: OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
  value: "http://prometheus:9090/api/v1/otlp/v1/metrics"
- name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
  value: "http://tempo:4318/v1/traces"
{{- end }}
{{- end }}

{{/*
Volume + volumeMount blocks for pods that need the shared PVC.
*/}}
{{- define "fastly-log-analytics.dataVolumes" -}}
- name: app-state
{{- if .Values.persistence.enabled }}
  persistentVolumeClaim:
    claimName: {{ include "fastly-log-analytics.pvcName" . }}
{{- else }}
  emptyDir:
    sizeLimit: 1Gi
{{- end }}
{{- end }}

{{- define "fastly-log-analytics.dataVolumeMounts" -}}
- name: app-state
  mountPath: /app/configs
  subPath: configs
- name: app-state
  mountPath: /app/data
  subPath: data
{{- end }}
