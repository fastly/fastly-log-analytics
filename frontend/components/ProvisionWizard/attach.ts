import { client, unwrapClient } from '@/lib/api'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import type { components } from '@/types/api.generated'
import type { LogFieldsConfig, ProvisionConfig, ValidateResponse } from './types'

// Cloud's attach mode logs a service whose capture VCL another install
// manages; Cloud then owns only its marker, this endpoint and its condition.
export const ATTACH_ENDPOINT_NAME = "FLP Cloud Logs"

const ATTACHABLE_ERRORS = new Set(["service_managed_by_other_install", "managed_resources_not_owned"])

export type AttachOffer =
  | { serviceId: string; available: true; missingFields: string[]; data: ValidateResponse }
  | { serviceId: string; available: false; reason: string }

const ALL_LOG_FIELD_GROUPS = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"]

export type ScoringAttachCustomField = {
  name: string
  label: string
  description: string
  vcl_log_expression: string
  collection_stage: string
  sql_type: string
  duckdb_type: string
  value_type: string
  bytes_estimate: number
  enabled: boolean
  filterable: boolean
  show_in_dashboard: boolean
  show_in_logs: boolean
}

export const SCORING_ATTACH_CUSTOM_FIELDS: readonly Omit<ScoringAttachCustomField, "enabled">[] = [
  {
    name: "edge_score",
    label: "Edge Score",
    description: "Combined session-anomaly score (0-100, quantized to nearest 5) from the edge scorer.",
    vcl_log_expression: "req.http.x-edge-score:score",
    collection_stage: "deliver",
    sql_type: "INTEGER",
    duckdb_type: "INTEGER",
    value_type: "numeric",
    bytes_estimate: 4,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
  {
    name: "edge_score_l1",
    label: "Edge Score (Layer 1)",
    description: "Layer-1 (universal behavioral) score contribution.",
    vcl_log_expression: "req.http.x-edge-score:l1",
    collection_stage: "deliver",
    sql_type: "INTEGER",
    duckdb_type: "INTEGER",
    value_type: "numeric",
    bytes_estimate: 4,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
  {
    name: "edge_score_l2",
    label: "Edge Score (Layer 2)",
    description: "Layer-2 (route transition) score contribution.",
    vcl_log_expression: "req.http.x-edge-score:l2",
    collection_stage: "deliver",
    sql_type: "INTEGER",
    duckdb_type: "INTEGER",
    value_type: "numeric",
    bytes_estimate: 4,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
  {
    name: "edge_cookie_compliance",
    label: "Cookie Compliance",
    description: "ok | missing | tampered | unknown.",
    vcl_log_expression: "req.http.x-edge-score:compliance",
    collection_stage: "deliver",
    sql_type: "VARCHAR",
    duckdb_type: "VARCHAR",
    value_type: "string",
    bytes_estimate: 10,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
  {
    name: "edge_score_reason",
    label: "Score Reason",
    description: "Comma-separated list of fired scoring rules.",
    vcl_log_expression: "req.http.x-edge-score:reason",
    collection_stage: "deliver",
    sql_type: "VARCHAR",
    duckdb_type: "VARCHAR",
    value_type: "string",
    bytes_estimate: 60,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
  {
    name: "edge_sid",
    label: "Session ID",
    description: "12-hex-char rotating session id from the edge scorer cookie.",
    vcl_log_expression: "req.http.x-edge-score:sid",
    collection_stage: "deliver",
    sql_type: "VARCHAR",
    duckdb_type: "VARCHAR",
    value_type: "string",
    bytes_estimate: 12,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
  {
    name: "edge_score_rtt_us",
    label: "Scorer Round-Trip (us)",
    description: "Edge-observed round-trip latency for the scoring sub-fetch, in microseconds.",
    vcl_log_expression: "req.http.x-edge-score:rtt",
    collection_stage: "deliver",
    sql_type: "INTEGER",
    duckdb_type: "INTEGER",
    value_type: "numeric",
    bytes_estimate: 6,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
  {
    name: "edge_score_exec_us",
    label: "Scorer Exec (us)",
    description: "Scorer-reported Wasm execution time in microseconds.",
    vcl_log_expression: "req.http.x-edge-score:exec",
    collection_stage: "deliver",
    sql_type: "INTEGER",
    duckdb_type: "INTEGER",
    value_type: "numeric",
    bytes_estimate: 5,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
  {
    name: "edge_matrix_version",
    label: "Matrix Version",
    description: "Transition-matrix version (YYYY-MM-DD-suffix) that scored this request.",
    vcl_log_expression: "req.http.x-edge-score:matrix",
    collection_stage: "deliver",
    sql_type: "VARCHAR",
    duckdb_type: "VARCHAR",
    value_type: "string",
    bytes_estimate: 18,
    filterable: true,
    show_in_dashboard: false,
    show_in_logs: true,
  },
]

function mergeScoringAttachFields(
  existing: LogFieldsConfig["custom_fields"] | undefined,
  missingFields: readonly string[],
): NonNullable<LogFieldsConfig["custom_fields"]> {
  const existingList = Array.isArray(existing) ? existing : []
  const scoringByName = new Map(
    SCORING_ATTACH_CUSTOM_FIELDS.map((field) => [
      field.name,
      { ...field, enabled: !missingFields.includes(field.name) },
    ]),
  )
  const seen = new Set<string>()
  const merged = existingList.map((cf) => {
    const name = typeof (cf as { name?: unknown })?.name === "string" ? String((cf as { name: string }).name) : ""
    if (name && scoringByName.has(name)) {
      seen.add(name)
      return { ...cf, ...scoringByName.get(name)! }
    }
    return cf
  })
  for (const [name, field] of scoringByName) {
    if (!seen.has(name)) merged.push(field)
  }
  return merged as NonNullable<LogFieldsConfig["custom_fields"]>
}

/** Asks Cloud whether a service another install manages can be attached to. */
export async function resolveAttachOffer(
  error: unknown,
  token: string,
  serviceId: string,
  logFields: LogFieldsConfig,
): Promise<AttachOffer | null> {
  if (!isCloudMode() || !(error instanceof Error) || !ATTACHABLE_ERRORS.has(error.message)) return null
  const body = {
    token,
    service_id: serviceId,
    capture: "attached",
    ...((logFields.groups?.length ?? 0) > 0 ? { log_fields: logFields } : {}),
  } as components["schemas"]["ProvisionValidateRequest"]
  try {
    let data = await unwrapClient(client.POST("/api/provision/validate", { body }))
    let capture = (data as { capture?: { missing_fields?: unknown; scoring_present?: unknown } } | undefined)?.capture
    let missingFields = Array.isArray(capture?.missing_fields) ? capture.missing_fields.map(String) : []
    const existingNames = new Set(
      (logFields.custom_fields ?? []).map((cf) => String((cf as { name?: unknown })?.name ?? "")),
    )
    if (capture?.scoring_present === true && SCORING_ATTACH_CUSTOM_FIELDS.some((f) => !existingNames.has(f.name))) {
      const candidateFields: LogFieldsConfig = {
        ...logFields,
        groups: (logFields.groups?.length ?? 0) > 0 ? logFields.groups : [...ALL_LOG_FIELD_GROUPS],
        custom_fields: mergeScoringAttachFields(logFields.custom_fields, []),
      }
      data = await unwrapClient(
        client.POST("/api/provision/validate", {
          body: { token, service_id: serviceId, capture: "attached", log_fields: candidateFields } as components["schemas"]["ProvisionValidateRequest"],
        }),
      )
      capture = (data as { capture?: { missing_fields?: unknown; scoring_present?: unknown } } | undefined)?.capture
      missingFields = Array.isArray(capture?.missing_fields) ? capture.missing_fields.map(String) : []
    }
    return { serviceId, available: true, missingFields, data: data as ValidateResponse }
  } catch (attachError) {
    return { serviceId, available: false, reason: attachError instanceof Error ? attachError.message : String(attachError) }
  }
}

export function applyAttach(prev: ProvisionConfig, offer?: AttachOffer): ProvisionConfig {
  const base: ProvisionConfig = {
    ...prev,
    capture: "attached",
    endpoint_name: ATTACH_ENDPOINT_NAME,
    logging_enabled: true,
  }
  if (!offer?.available) return base
  const capture = (offer.data as {
    capture?: { rum_present?: unknown; cmcd_present?: unknown; scoring_present?: unknown }
  } | undefined)?.capture
  const fieldOverrides = Object.fromEntries(offer.missingFields.map((id) => [id, false]))
  const customFields = capture?.scoring_present === true
    ? mergeScoringAttachFields(prev.log_fields.custom_fields, offer.missingFields)
    : (prev.log_fields.custom_fields ?? [])
  return {
    ...base,
    rum_enabled: capture?.rum_present === true,
    cmcd_enabled: capture?.cmcd_present === true,
    log_fields: {
      ...prev.log_fields,
      groups: [...ALL_LOG_FIELD_GROUPS],
      custom_fields: customFields,
      field_overrides: fieldOverrides,
    },
  }
}

export function clearAttach(prev: ProvisionConfig): ProvisionConfig {
  if (prev.capture !== "attached") return prev
  return {
    ...prev,
    capture: "managed",
    endpoint_name: prev.endpoint_name === ATTACH_ENDPOINT_NAME ? "" : prev.endpoint_name,
  }
}

export function attachUnavailableMessage(reason: string): string {
  return reason === "capture_vcl_missing"
    ? "Another install manages this service, but its capture VCL snippets are missing, so Cloud cannot attach to it."
    : `Another install manages this service and Cloud cannot attach to it (${reason}).`
}
