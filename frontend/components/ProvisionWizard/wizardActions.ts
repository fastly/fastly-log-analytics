import type { QueryClient } from '@tanstack/react-query'
import { z } from 'zod'
import { client, unwrapClient, queryKeys, adminFetch } from '@/lib/api'
import { downloadBlob } from '@/lib/utils'
import type { Service } from '@/stores/filterStore'
import type { components } from '@/types/api.generated'
import {
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  type FosStatus,
  type JoinPhase,
  type LakeInfo,
  type PersistedConfig,
  type ProvisionConfig,
  type ProvisionService,
  type Step,
  type ValidateResponse,
  type WizardDraft,
  type WizardMode,
} from './types'

function buildTerraformPayload(token: string, selectedService: ProvisionService, config: ProvisionConfig) {
  return {
    token,
    logging_service_id: selectedService.id,
    service_name: selectedService.name,
    endpoint_name: config.endpoint_name,
    fos_region: config.fos_region,
    fos_bucket_name: config.fos_bucket_name,
    fos_prefix: config.fos_prefix,
    sample_rate: String(config.sample_rate),
    edge_only: config.edge_only,
    custom_condition: config.custom_condition,
    log_period: String(config.log_period),
    rum_log_period: String(config.rum_log_period || 300),
    cdn_service_name: config.cdn_service_name,
    cdn_prefix: config.cdn_prefix,
    cdn_shield: config.cdn_shield,
    log_fields: config.log_fields,
  }
}

export interface FetchTerraformPreviewArgs {
  token: string
  selectedService: ProvisionService | null
  config: ProvisionConfig
  setIsFetchingTerraform: (b: boolean) => void
  setTerraformFiles: (f: Record<string, string>) => void
  setSelectedTfFile: (f: string) => void
}

export async function runFetchTerraformPreview(args: FetchTerraformPreviewArgs) {
  const { token, selectedService, config, setIsFetchingTerraform, setTerraformFiles, setSelectedTfFile } = args
  if (!selectedService) return
  setIsFetchingTerraform(true)
  try {
    const files = await unwrapClient(
      client.POST("/api/provision/terraform/preview", {
        body: buildTerraformPayload(token, selectedService, config),
      }),
    )
    if (files) {
      setTerraformFiles(files)
      const preferred = files["instructions"]
        ? "instructions"
        : files["main.tf"]
          ? "main.tf"
          : Object.keys(files)[0]
      if (preferred) setSelectedTfFile(preferred)
    }
  } catch (e) {
    console.error(e)
  } finally {
    setIsFetchingTerraform(false)
  }
}

export async function runExportTerraform(args: {
  token: string
  selectedService: ProvisionService | null
  config: ProvisionConfig
}) {
  const { token, selectedService, config } = args
  if (!selectedService) return
  try {
    const response = await adminFetch("/api/provision/terraform/export", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildTerraformPayload(token, selectedService, config)),
    })
    if (!response.ok) throw new Error("Export failed")
    downloadBlob(await response.blob(), "fastly-log-analysis-terraform.zip")
  } catch (e) {
    console.error("Failed to export Terraform", e)
  }
}

export interface ModalCloseDeps {
  status: string
  isDone: boolean
  onOpenChange: (open: boolean) => void
  selectedService: ProvisionService | null
  setActiveServiceId: (id: string) => void
  queryClient: Pick<QueryClient, "invalidateQueries">
  setStep: (s: Step) => void
  setMode: (m: WizardMode) => void
  setSearch: (s: string) => void
  setSelectedService: (s: ProvisionService | null) => void
  setIsDeploying: (b: boolean) => void
  setFosStatus: (s: FosStatus) => void
  setFosError: (s: string) => void
  setLakeInfo: (l: LakeInfo | null) => void
  setIsAnalyzing: (b: boolean) => void
  setImportMode: (m: "all" | "range") => void
  setSyncEnabled: (b: boolean) => void
  reset: () => void
  resetConfig: () => void
  setNgwafWorkspaces: (w: { id: string; name: string }[]) => void
  setNgwafFetching: (b: boolean) => void
  setNgwafFetchError: (s: string) => void
}

export function buildHandleModalClose(deps: ModalCloseDeps) {
  return (isOpen: boolean) => {
    if (deps.status === "streaming") return
    deps.onOpenChange(isOpen)
    if (!isOpen) {
      if (deps.status === "done" || deps.isDone) {
        if (deps.selectedService?.id) deps.setActiveServiceId(deps.selectedService.id)
        deps.queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
        clearDraft()
        window.location.reload()
      } else {
        setTimeout(() => {
          deps.setStep("mode")
          deps.setMode(null)
          deps.setSearch("")
          deps.setSelectedService(null)
          deps.setIsDeploying(false)
          deps.setFosStatus("idle")
          deps.setFosError("")
          deps.setLakeInfo(null)
          deps.setIsAnalyzing(false)
          deps.setImportMode("all")
          deps.setSyncEnabled(true)
          deps.reset()
          deps.resetConfig()
          deps.setNgwafWorkspaces([])
          deps.setNgwafFetching(false)
          deps.setNgwafFetchError("")
        }, 300)
      }
    }
  }
}

export interface DeployArgs {
  token: string
  selectedService: ProvisionService | null
  config: ProvisionConfig
  setIsDeploying: (b: boolean) => void
  start: (urlPath: string, body?: Record<string, unknown>) => void
}

export function runDeploy({ token, selectedService, config, setIsDeploying, start }: DeployArgs) {
  if (!selectedService) return
  setIsDeploying(true)
  const body: Record<string, unknown> = {
    token,
    service_id: selectedService.id,
    service_name: selectedService.name,
    endpoint_name: config.endpoint_name,
    fos_region: config.fos_region,
    fos_bucket_name: config.fos_bucket_name,
    fos_prefix: config.fos_prefix,
    sample_rate: String(config.sample_rate),
    edge_only: config.edge_only,
    custom_condition: config.custom_condition,
    log_period: String(config.log_period),
    rum_log_period: String(config.rum_log_period || 300),
    cdn_service_name: config.cdn_service_name,
    cdn_shield: config.cdn_shield,
    enable_cron_sync: config.enable_cron_sync,
    delete_after: config.delete_after,
    commit_interval_mins: Number(config.commit_interval_mins),
    enable_cron_compact: config.enable_cron_compact,
    log_retention_days: Number(config.log_retention_days),
    rum_retention_days: Number(config.rum_retention_days),
    log_fields: config.log_fields ? JSON.stringify(config.log_fields) : null,
    cmcd_enabled: config.cmcd_enabled,
    cmcd_mode: config.cmcd_mode,
    cmcd_version: config.cmcd_version,
    logging_enabled: config.logging_enabled,
    rum_enabled: config.rum_enabled,
    rum_custom_condition: config.rum_custom_condition,
    faro_version: config.faro_version || null,
  }
  if (config.cdn_prefix) body.cdn_url = `https://${config.cdn_prefix}.global.ssl.fastly.net`
  start("/api/provision/execute", body)
}

export interface JoinArgs {
  config: ProvisionConfig
  analystPathASupported: boolean
  syncIntervalMins: string
  syncEnabled: boolean
  icebergMetadataLocation: string
  importMode: "all" | "range"
  importRange: { start: string; end: string }
  setIsDeploying: (b: boolean) => void
  setJoinPhase: (p: JoinPhase) => void
  setStep: (s: Step) => void
  reset: () => void
  start: (urlPath: string, body?: Record<string, unknown>) => void
}

export function runJoin(args: JoinArgs) {
  const {
    config,
    analystPathASupported,
    syncIntervalMins,
    syncEnabled,
    icebergMetadataLocation,
    importMode,
    importRange,
    setIsDeploying,
    setJoinPhase,
    setStep,
    reset,
    start,
  } = args
  if (
    !analystPathASupported ||
    !config.endpoint_name ||
    !config.cdn_service_name ||
    !config.fos_bucket_name ||
    !config.fos_region ||
    !config.fos_access_key ||
    !config.fos_secret_key
  ) {
    return
  }
  setIsDeploying(true)
  setJoinPhase("connecting")
  setStep("join")
  reset()
  const params: Record<string, string> = {
    service_id: config.cdn_service_name,
    service_name: config.endpoint_name,
    fos_bucket_name: config.fos_bucket_name,
    fos_region: config.fos_region,
    fos_endpoint: config.fos_endpoint || "",
    fos_access_key: config.fos_access_key,
    fos_secret_key: config.fos_secret_key,
    cdn_url: config.cdn_url || "",
    cdn_service_id: config.cdn_service_name || "",
    cdn_secret: config.cdn_secret || "",
    sync_interval_mins: syncIntervalMins,
    sync_enabled: String(syncEnabled),
    iceberg_metadata_location: icebergMetadataLocation || "",
  }
  if (importMode === "range") {
    if (importRange.start) params.start_time = importRange.start
    if (importRange.end) params.end_time = importRange.end
  }
  start(`/api/provision/join?${new URLSearchParams(params).toString()}`)
}

export function registerServiceAndInvalidate<T extends { id: string; name: string; accessLevel?: string }>(
  id: string,
  name: string,
  accessLevel: "read_write" | "read_only",
  services: T[],
  setServices: (s: T[]) => void,
  setActiveServiceId: (id: string) => void,
  setJoinedServiceId: (id: string) => void,
  queryClient: Pick<QueryClient, "invalidateQueries">,
) {
  setJoinedServiceId(id)
  if (id) {
    setActiveServiceId(id)
    if (!services.some((s) => s.id === id)) {
      setServices([...services, { id, name, accessLevel } as T])
    }
  }
  queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
}

export interface AdminIngestArgs {
  token: string
  selectedService: ProvisionService | null
  selectedCdnService: ProvisionService | null
  config: ProvisionConfig
  services: Service[]
  setIsDeploying: (b: boolean) => void
  setJoinedServiceId: (id: string) => void
  setActiveServiceId: (id: string) => void
  setServices: (services: Service[]) => void
  queryClient: Pick<QueryClient, "invalidateQueries">
  setJoinPhase: (p: JoinPhase) => void
  setStep: (s: Step) => void
}

export async function runAdminIngest(args: AdminIngestArgs) {
  const {
    token,
    selectedService,
    selectedCdnService,
    config,
    services,
    setIsDeploying,
    setJoinedServiceId,
    setActiveServiceId,
    setServices,
    queryClient,
    setJoinPhase,
    setStep,
  } = args
  if (!selectedService) return
  setIsDeploying(true)
  try {
    const data = await unwrapClient(
      client.POST("/api/provision/ingest", {
        body: {
          token,
          service_id: selectedService.id,
          service_name: selectedService.name,
          endpoint_name: config.endpoint_name,
          fos_region: config.fos_region,
          fos_bucket_name: config.fos_bucket_name,
          fos_prefix: config.fos_prefix,
          sample_rate: String(config.sample_rate),
          edge_only: config.edge_only,
          custom_condition: config.custom_condition,
          log_period: String(config.log_period),
          cdn_service_id: selectedCdnService?.id || config.cdn_service_name,
          cdn_service_name: selectedCdnService?.name || config.cdn_service_name,
          cdn_url: config.cdn_url || (config.cdn_prefix ? `https://${config.cdn_prefix}.global.ssl.fastly.net` : ""),
          cdn_shield: config.cdn_shield,
          enable_cron_sync: config.enable_cron_sync,
          delete_after: config.delete_after,
          commit_interval_mins: config.commit_interval_mins,
          enable_cron_compact: config.enable_cron_compact,
          log_retention_days: config.log_retention_days,
          rum_retention_days: config.rum_retention_days,
          log_fields: config.log_fields,
          rum_custom_condition: config.rum_custom_condition,
          fos_access_key: config.fos_access_key,
          fos_secret_key: config.fos_secret_key,
        },
      }),
    )
    if (data?.ok) {
      registerServiceAndInvalidate(
        selectedService.id,
        selectedService.name,
        "read_write",
        services,
        setServices,
        setActiveServiceId,
        setJoinedServiceId,
        queryClient,
      )
      setJoinPhase("done")
      setStep("join")
    }
  } catch (e) {
    console.error("Ingest failed", e)
  } finally {
    setIsDeploying(false)
  }
}

export interface ValidateMutationDeps {
  token: string
  setTokenInfo: (info: { id: string; name: string; type: "user" | "automation" }) => void
  setConfig: (updater: (prev: ProvisionConfig) => ProvisionConfig) => void
  setStep: (s: Step) => void
  mode: string | null
}

export const validateMutationFn = (token: string) => async (serviceId: string) =>
  await unwrapClient(client.POST("/api/provision/validate", { body: { token, service_id: serviceId } }))

export function buildValidateOnSuccess(deps: ValidateMutationDeps) {
  return (data: ValidateResponse | undefined) => {
    const tokenInfo = data?.token_info
    if (tokenInfo?.id && tokenInfo.name && tokenInfo.type) {
      deps.setTokenInfo({ id: tokenInfo.id, name: tokenInfo.name, type: tokenInfo.type })
    }
    const defaults = data?.defaults
    if (defaults) {
      deps.setConfig((prev) => ({
        ...prev,
        endpoint_name: defaults.endpoint_name || "Fastly Object Storage Logs",
        fos_region: defaults.fos_region || "us-east-1",
        fos_bucket_name: defaults.fos_bucket_name?.toLowerCase() || "",
        fos_prefix: defaults.fos_prefix || "",
        cdn_service_name: defaults.cdn_service_name || `${data.service_name} (CDN)`,
        cdn_prefix: (
          defaults.cdn_prefix ||
          (defaults.fos_bucket_name ? `fos-${defaults.fos_bucket_name.split("-").slice(0, 2).join("-")}` : "")
        ).toLowerCase(),
      }))
    }
    deps.setStep(deps.mode === "join" || deps.mode === "ingest" ? "join" : "features")
  }
}

export interface CheckConfigArgs {
  token: string
  selectedService: ProvisionService | null
  selectedCdnService: ProvisionService | null
  config: ProvisionConfig
  setIsCheckingConfig: (b: boolean) => void
  setConfigStatus: (s: components["schemas"]["ProvisionCheckConfigResponse"] | null) => void
}

export async function runCheckConfig(args: CheckConfigArgs) {
  const { token, selectedService, selectedCdnService, config, setIsCheckingConfig, setConfigStatus } = args
  if (!selectedService || !selectedCdnService || !config.fos_bucket_name) return
  setIsCheckingConfig(true)
  try {
    const data = await unwrapClient(
      client.GET("/api/provision/check-config", {
        params: {
          query: {
            token,
            service_id: selectedService.id,
            cdn_service_id: selectedCdnService.id,
            bucket: config.fos_bucket_name,
          },
        },
      }),
    )
    if (data) setConfigStatus(data)
  } catch (e) {
    console.error("Failed to check config", e)
  } finally {
    setIsCheckingConfig(false)
  }
}

export interface CheckFosArgs {
  vals?: { bucket?: string; region?: string; access_key?: string; secret_key?: string }
  config: ProvisionConfig
  setFosStatus: (s: FosStatus) => void
  setFosError: (s: string) => void
}

export async function runCheckFos({ vals, config, setFosStatus, setFosError }: CheckFosArgs) {
  const bucket = vals?.bucket ?? config.fos_bucket_name
  const region = vals?.region ?? config.fos_region
  const access_key = vals?.access_key ?? config.fos_access_key
  const secret_key = vals?.secret_key ?? config.fos_secret_key
  if (!bucket || !region || !access_key || !secret_key) return
  setFosStatus("checking")
  setFosError("")
  try {
    const data = await unwrapClient(
      client.POST("/api/provision/check-fos", { body: { bucket, region, access_key, secret_key } }),
    )
    if (data?.ok) {
      setFosStatus("success")
    } else {
      setFosStatus("error")
      setFosError(data?.error || "Failed to connect.")
    }
  } catch (err) {
    setFosStatus("error")
    setFosError(err instanceof Error && err.message ? err.message : "An error occurred.")
  }
}

export async function runCheckDomain({
  prefix,
  setDomainStatus,
  setDomainMessage,
}: {
  prefix: string
  setDomainStatus: (s: "idle" | "checking" | "available" | "taken" | "error") => void
  setDomainMessage: (m: string) => void
}) {
  if (!prefix || prefix.length < 3) return
  setDomainStatus("checking")
  try {
    const data = await unwrapClient(client.GET("/api/provision/check-domain", { params: { query: { prefix } } }))
    if (data?.available) {
      setDomainStatus("available")
      setDomainMessage("Domain available!")
    } else {
      setDomainStatus("taken")
      setDomainMessage("This domain prefix is already in use.")
    }
  } catch {
    setDomainStatus("error")
  }
}

export interface AnalyzeLakeArgs {
  config: ProvisionConfig
  icebergMetadataLocation: string
  setIsAnalyzing: (b: boolean) => void
  setLakeInfo: (l: LakeInfo) => void
  setImportRange: (r: { start: string; end: string }) => void
  setStep: (s: Step) => void
  setFosStatus: (s: FosStatus) => void
  setFosError: (s: string) => void
}

export async function runAnalyzeLake(args: AnalyzeLakeArgs) {
  const { config, icebergMetadataLocation, setIsAnalyzing, setLakeInfo, setImportRange, setStep, setFosStatus, setFosError } = args
  setIsAnalyzing(true)
  try {
    const data = await unwrapClient(
      client.POST("/api/provision/lake-info", {
        body: {
          bucket: config.fos_bucket_name,
          region: config.fos_region,
          access_key: config.fos_access_key,
          secret_key: config.fos_secret_key,
          prefix: config.fos_prefix,
          endpoint: config.fos_endpoint || undefined,
          iceberg_metadata_location: icebergMetadataLocation || undefined,
        },
      }),
    )
    if (data?.ok) {
      setLakeInfo(data)
      if (data.range?.start && data.range?.end) {
        setImportRange({ start: data.range.start, end: data.range.end })
      }
      setStep("analyze")
    } else {
      setFosStatus("error")
      setFosError(data?.error || "Failed to analyze data lake.")
    }
  } catch (e) {
    setFosStatus("error")
    setFosError(e instanceof Error ? e.message : String(e))
  } finally {
    setIsAnalyzing(false)
  }
}

export const WIZARD_DRAFT_KEY = "provision-wizard-draft-v1"
const SECRET_KEYS = ["fos_access_key", "fos_secret_key", "cdn_secret"] as const

export function stripSecretsFromConfig(config: ProvisionConfig): PersistedConfig {
  const out: Partial<ProvisionConfig> = { ...config }
  for (const key of SECRET_KEYS) delete out[key]
  return out as PersistedConfig
}

export function mergePersistedConfig(persisted: PersistedConfig): ProvisionConfig {
  return { ...INITIAL_CONFIG, ...persisted, fos_access_key: "", fos_secret_key: "", cdn_secret: "" }
}

function safeLocalStorage(): Storage | null {
  try {
    return typeof window === "undefined" ? null : window.localStorage
  } catch {
    return null
  }
}

const WizardDraftHeaderSchema = z.object({
  version: z.number(),
  mode: z.enum(["provision", "join", "ingest"]),
  step: z.string(),
})

export function loadDraft(): WizardDraft | null {
  const ls = safeLocalStorage()
  if (!ls) return null
  try {
    const raw = ls.getItem(WIZARD_DRAFT_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as WizardDraft
    if (!parsed || typeof parsed !== "object") return null
    if (parsed.version !== WIZARD_DRAFT_VERSION) {
      try {
        ls.removeItem(WIZARD_DRAFT_KEY)
      } catch {
        /* ignore */
      }
      return null
    }
    const check = WizardDraftHeaderSchema.safeParse(parsed)
    return !check.success || parsed.step === "mode" ? null : parsed
  } catch {
    return null
  }
}

export function saveDraft(draft: WizardDraft): void {
  const ls = safeLocalStorage()
  if (!ls) return
  try {
    ls.setItem(
      WIZARD_DRAFT_KEY,
      JSON.stringify({
        ...draft,
        config: stripSecretsFromConfig({ ...INITIAL_CONFIG, ...(draft.config as ProvisionConfig) }),
      }),
    )
  } catch {
    /* private-mode browsers throw; intentional no-op */
  }
}

export function clearDraft(): void {
  const ls = safeLocalStorage()
  if (ls) {
    try {
      ls.removeItem(WIZARD_DRAFT_KEY)
    } catch {
      /* ignore */
    }
  }
}

export function subscribeDraft(cb: (next: WizardDraft | null) => void): () => void {
  if (typeof window === "undefined") return () => undefined
  const handler = (e: StorageEvent) => {
    if (e.key !== WIZARD_DRAFT_KEY) return
    if (e.newValue === null) {
      cb(null)
      return
    }
    try {
      const parsed = JSON.parse(e.newValue) as WizardDraft
      cb(parsed && parsed.version === WIZARD_DRAFT_VERSION ? parsed : null)
    } catch {
      cb(null)
    }
  }
  window.addEventListener("storage", handler)
  return () => window.removeEventListener("storage", handler)
}

export function makeDraftId(): string {
  return typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `draft-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}
