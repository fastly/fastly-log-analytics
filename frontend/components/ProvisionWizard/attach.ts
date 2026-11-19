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
    const data = await unwrapClient(client.POST("/api/provision/validate", { body }))
    const capture = (data as { capture?: { missing_fields?: unknown } } | undefined)?.capture
    const missingFields = Array.isArray(capture?.missing_fields) ? capture.missing_fields.map(String) : []
    return { serviceId, available: true, missingFields, data: data as ValidateResponse }
  } catch (attachError) {
    return { serviceId, available: false, reason: attachError instanceof Error ? attachError.message : String(attachError) }
  }
}

const ALL_LOG_FIELD_GROUPS = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"]

export function applyAttach(prev: ProvisionConfig, offer?: AttachOffer): ProvisionConfig {
  const base: ProvisionConfig = {
    ...prev,
    capture: "attached",
    endpoint_name: ATTACH_ENDPOINT_NAME,
    logging_enabled: true,
  }
  if (!offer?.available) return base
  const capture = (offer.data as { capture?: { rum_present?: unknown; cmcd_present?: unknown } } | undefined)?.capture
  const fieldOverrides = Object.fromEntries(offer.missingFields.map((id) => [id, false]))
  return {
    ...base,
    rum_enabled: capture?.rum_present === true,
    cmcd_enabled: capture?.cmcd_present === true,
    log_fields: {
      ...prev.log_fields,
      groups: [...ALL_LOG_FIELD_GROUPS],
      custom_fields: prev.log_fields.custom_fields ?? [],
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
