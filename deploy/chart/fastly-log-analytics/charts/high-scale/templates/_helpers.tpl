{{- define "high-scale.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "high-scale.fullname" -}}
{{- printf "%s-high-scale" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "high-scale.labels" -}}
app.kubernetes.io/name: {{ include "high-scale.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: fastly-log-analytics
{{- end }}

{{- define "high-scale.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "high-scale.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "high-scale.workloadName" -}}
{{- printf "%s-%s" (include "high-scale.fullname" .root) .workload | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "high-scale.workloadLabels" -}}
{{ include "high-scale.labels" .root }}
app.kubernetes.io/component: {{ .workload }}
{{- end }}
