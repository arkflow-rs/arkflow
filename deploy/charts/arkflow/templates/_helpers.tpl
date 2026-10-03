{{/* Expand the name of the chart. */}}
{{- define "arkflow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fullname of the release. */}}
{{- define "arkflow.fullname" -}}
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

{{/* Chart name and version label. */}}
{{- define "arkflow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Common selector labels. */}}
{{- define "arkflow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arkflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Full metadata labels. */}}
{{- define "arkflow.labels" -}}
helm.sh/chart: {{ include "arkflow.chart" . }}
{{ include "arkflow.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* ServiceAccount name. */}}
{{- define "arkflow.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "arkflow.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Effective replica count: locked to 1 unless the unsafe opt-in is set. */}}
{{- define "arkflow.replicas" -}}
{{- if .Values.unsafe.allowMultipleReplicas -}}
{{- .Values.unsafe.replicas }}
{{- else -}}
1
{{- end }}
{{- end }}

{{/* Image reference. */}}
{{- define "arkflow.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/* True when the engine workload runs as an agent (joins a Hub). */}}
{{- define "arkflow.agentEnabled" -}}
{{- or (eq .Values.mode "agent") (and (eq .Values.mode "control-plane") .Values.controlPlane.agent.enabled) }}
{{- end }}

{{/* Engine workload name: suffixed in control-plane mode to sit next to Hub/Console. */}}
{{- define "arkflow.workloadName" -}}
{{- if eq .Values.mode "control-plane" }}{{ include "arkflow.fullname" . }}-agent{{- else }}{{ include "arkflow.fullname" . }}{{- end }}
{{- end }}

{{/* Hub and Console object names in control-plane mode. */}}
{{- define "arkflow.hubName" -}}{{ include "arkflow.fullname" . }}-hub{{- end }}
{{- define "arkflow.consoleName" -}}{{ include "arkflow.fullname" . }}-console{{- end }}

{{/* Node credential Secret name/key: generated unless the user supplies one. */}}
{{- define "arkflow.nodeTokenSecretName" -}}
{{- if .Values.controlPlane.nodeToken.existingSecret }}{{ .Values.controlPlane.nodeToken.existingSecret }}{{- else }}{{ include "arkflow.fullname" . }}-node-token{{- end }}
{{- end }}
{{- define "arkflow.nodeTokenSecretKey" -}}
{{- if .Values.controlPlane.nodeToken.existingSecret }}{{ default "node_token" .Values.controlPlane.nodeToken.existingSecretKey }}{{- else }}node_token{{- end }}
{{- end }}

{{/* Persistence: opt-in for engine modes; for the control-plane Hub it
defaults ON (SQLite durability) unless Postgres or an explicit opt-out. */}}
{{- define "arkflow.persistenceEnabled" -}}
{{- if eq .Values.mode "control-plane" -}}
{{- and (not .Values.controlPlane.hub.storage.postgresURL) .Values.controlPlane.hub.storage.persistenceEnabled }}
{{- else -}}
{{- .Values.persistence.enabled }}
{{- end -}}
{{- end }}
