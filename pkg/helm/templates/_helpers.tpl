{{/*
Expand the name of the chart.
*/}}
{{- define "keydb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "keydb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "keydb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "keydb.labels" -}}
helm.sh/chart: {{ include "keydb.chart" . }}
{{ include "keydb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "keydb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "keydb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "keydb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "keydb.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Return the image name
*/}}
{{- define "keydb.image" -}}
{{- printf "%s:%s" .Values.image.repository (toString .Values.image.tag) -}}
{{- end -}}

{{/*
Return true if a password secret is needed
*/}}
{{- define "keydb.needsPasswordSecret" -}}
{{- if and .Values.auth.password (not .Values.auth.existingSecret) -}}
true
{{- end -}}
{{- end -}}

{{/*
Return the password secret name
*/}}
{{- define "keydb.passwordSecretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else -}}
{{- include "keydb.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
Return true if authentication is enabled
*/}}
{{- define "keydb.authEnabled" -}}
{{- if or .Values.auth.password .Values.auth.existingSecret -}}
true
{{- end -}}
{{- end -}}

{{/*
Renders a value that contains template expressions.
Usage: {{ include "keydb.tplValue" (dict "value" .Values.someValue "context" $) }}
*/}}
{{- define "keydb.tplValue" -}}
{{- if typeIs "string" .value -}}
{{- tpl .value .context -}}
{{- else -}}
{{- tpl (.value | toYaml) .context -}}
{{- end -}}
{{- end -}}
