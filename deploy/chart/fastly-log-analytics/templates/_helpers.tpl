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
Name of the PVC holding configs/data/cache.
*/}}
{{- define "fastly-log-analytics.pvcName" -}}
{{- if .Values.persistence.existingClaim }}
{{- .Values.persistence.existingClaim }}
{{- else }}
{{- printf "%s-data" (include "fastly-log-analytics.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Env block shared by backend, worker, and beat: broker/DSN via secretKeyRef
plus the DuckLake settings.
*/}}
{{- define "fastly-log-analytics.sharedEnv" -}}
- name: INGEST_MODE
  value: {{ .Values.config.ingestMode | quote }}
- name: SCHEDULER_MODE
  value: {{ .Values.config.schedulerMode | quote }}
- name: CELERY_BROKER_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "fastly-log-analytics.secretName" . }}
      key: CELERY_BROKER_URL
- name: METADATA_DSN
  valueFrom:
    secretKeyRef:
      name: {{ include "fastly-log-analytics.secretName" . }}
      key: METADATA_DSN
      optional: true
{{- if .Values.config.ducklakeCatalog }}
- name: DUCKLAKE_CATALOG
  value: {{ .Values.config.ducklakeCatalog | quote }}
{{- end }}
{{- if .Values.config.ducklakeDataPath }}
- name: DUCKLAKE_DATA_PATH
  value: {{ .Values.config.ducklakeDataPath | quote }}
{{- end }}
{{- if .Values.config.hotS3Endpoint }}
- name: HOT_S3_ENDPOINT
  value: {{ .Values.config.hotS3Endpoint | quote }}
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
  emptyDir: {}
{{- end }}
{{- end }}

{{- define "fastly-log-analytics.dataVolumeMounts" -}}
- name: app-state
  mountPath: /app/configs
  subPath: configs
- name: app-state
  mountPath: /app/data
  subPath: data
- name: app-state
  mountPath: /app/cache
  subPath: cache
{{- end }}
