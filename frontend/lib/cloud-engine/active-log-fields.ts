import { fetchCloudMetadata } from "./metadata-cache";

/**
 * The service's enabled log field ids, as its record stored them when its log field settings were
 * last saved. The source of truth for "is field X enabled?"; raw file columns are not, since Cloud
 * writes every built-in column whether or not it is logged. Config writes clear the listing cache.
 */
export async function cloudActiveLogFieldIds(serviceId: string | undefined, signal: AbortSignal): Promise<Set<string>> {
  const response = await fetchCloudMetadata("/api/services", signal);
  if (!response.ok) throw new Error(`Cloud services listing failed (${response.status})`);
  const { services = [] } = await response.json() as {
    services?: Array<{ service_id?: string; active_log_field_ids?: string[] }>;
  };
  const service = serviceId === undefined ? services[0] : services.find((item) => item.service_id === serviceId);
  return new Set(service?.active_log_field_ids ?? []);
}
