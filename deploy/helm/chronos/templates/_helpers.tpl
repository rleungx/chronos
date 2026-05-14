{{- define "chronos.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "chronos.fullname" -}}
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

{{- define "chronos.labels" -}}
app.kubernetes.io/name: {{ include "chronos.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/component: tso
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "chronos.selectorLabels" -}}
app.kubernetes.io/name: {{ include "chronos.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: tso
{{- end -}}

{{- define "chronos.serviceAccountName" -}}
{{- include "chronos.fullname" . -}}
{{- end -}}

{{- define "chronos.headlessServiceName" -}}
{{- printf "%s-headless" (include "chronos.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "chronos.ownershipPlanId" -}}
{{- if .Values.ownership.planId -}}
{{- .Values.ownership.planId -}}
{{- else -}}
{{- printf "%s-static-%d" (include "chronos.fullname" .) (int .Values.replicaCount) -}}
{{- end -}}
{{- end -}}

{{- define "chronos.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
