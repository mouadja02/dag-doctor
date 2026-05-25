{{- define "dag-doctor.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "dag-doctor.labels" -}}
app.kubernetes.io/name: dag-doctor
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "dag-doctor.selectorLabels" -}}
app.kubernetes.io/name: dag-doctor
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
