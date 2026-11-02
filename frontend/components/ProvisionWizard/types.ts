import type React from 'react'
import type { useMutation, useQueryClient } from '@tanstack/react-query'
import type { Service } from '@/stores/filterStore'
import type { SSELine, SSEStatus } from '@/hooks/useSSE'
import type { Schema } from '@/types/api'

export type ProvisionService = Schema<"ProvisionService">
export type LogFieldsCatalog = Schema<"LogFieldsCatalogResponse">
export type LogFieldsConfig = Schema<"LogFieldsConfig">
export type ValidateResponse = Schema<"ProvisionValidateResponse">

export const WIZARD_DRAFT_VERSION = 2

export type Step =
  | "mode"
  | "token"
  | "service"
  | "features"
  | "storage"
  | "ngwaf"
  | "fields"
  | "execute"
  | "terraform"
  | "connect"

export type WizardMode = "provision" | "ingest" | null
export type FosStatus = "idle" | "checking" | "success" | "error"
export type DomainStatus = "idle" | "checking" | "available" | "taken" | "error"
export type SetupPhase = "form" | "done"

export interface TokenInfo {
  id: string
  name: string
  type: "user" | "automation"
}

export interface ProvisionConfig {
  endpoint_name: string
  fos_region: string
  fos_endpoint: string
  fos_bucket_name: string
  fos_prefix: string
  fos_access_key: string
  fos_secret_key: string
  sample_rate: number
  edge_only: boolean
  custom_condition: string
  log_period: number
  cdn_service_name: string
  cdn_prefix: string
  cdn_shield: string
  cdn_url: string
  cdn_secret: string
  enable_cron_sync: boolean
  delete_after: boolean
  commit_interval_mins: number
  enable_cron_compact: boolean
  log_fields: LogFieldsConfig
  ngwaf_workspace_id: string
  cmcd_enabled: boolean
  cmcd_mode: string
  cmcd_version: number
  logging_enabled: boolean
  rum_enabled: boolean
  rum_log_period: number
  log_retention_days: number
  rum_retention_days: number
  cache_retention_days: number
  rollup_retention_months: number
  rum_custom_condition: string
  faro_version: string | null
}

export const INITIAL_CONFIG: ProvisionConfig = {
  endpoint_name: "",
  fos_region: "us-east-1",
  fos_endpoint: "",
  fos_bucket_name: "",
  fos_prefix: "",
  fos_access_key: "",
  fos_secret_key: "",
  sample_rate: 100,
  edge_only: true,
  custom_condition: "",
  log_period: 60,
  rum_log_period: 300,
  cdn_service_name: "",
  cdn_prefix: "",
  cdn_shield: "iad-va-us",
  cdn_url: "",
  cdn_secret: "",
  enable_cron_sync: true,
  delete_after: true,
  commit_interval_mins: 5,
  enable_cron_compact: true,
  log_fields: { groups: [], field_overrides: {} },
  ngwaf_workspace_id: "",
  cmcd_enabled: false,
  cmcd_mode: "query_string",
  cmcd_version: 1,
  logging_enabled: true,
  rum_enabled: false,
  log_retention_days: 30,
  rum_retention_days: 30,
  cache_retention_days: 14,
  rollup_retention_months: 12,
  rum_custom_condition: "",
  faro_version: null,
}

export type PersistedConfig = Omit<ProvisionConfig, "fos_access_key" | "fos_secret_key" | "cdn_secret">

export interface WizardDraft {
  version: number
  draftId: string
  mode: Exclude<WizardMode, null>
  step: Step
  currentStep: Step
  selectedServiceId: string | null
  selectedServiceName: string | null
  selectedCdnServiceId: string | null
  selectedCdnServiceName: string | null
  tokenInfo: TokenInfo | null
  config: PersistedConfig
  updatedAt: string
  createdAt: string
}

export function getStepsForMode(
  mode: WizardMode,
  config?: { logging_enabled?: boolean },
): { id: Step; label: string }[] {
  const steps: { id: Step; label: string }[] = [
    { id: "mode", label: "Role" },
    { id: "token", label: "Auth" },
    { id: "service", label: "Service" },
  ]
  if (mode === "ingest") steps.push({ id: "connect", label: "Connect" })
  steps.push({ id: "features", label: "Features" }, { id: "storage", label: "Storage" })
  if (!config || config.logging_enabled !== false) {
    steps.push({ id: "ngwaf", label: "NGWAF" }, { id: "fields", label: "Log Fields" })
  }
  steps.push({ id: "execute", label: "Review" })
  return steps
}

export interface ProvisionWizardProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

export interface WizardState {
  setActiveServiceId: (id: string) => void
  setServices: (services: Service[]) => void
  services: Service[]
  timezone: string
  queryClient: ReturnType<typeof useQueryClient>
  step: Step
  setStep: (s: Step) => void
  mode: WizardMode
  setMode: (m: WizardMode) => void
  token: string
  setToken: (t: string) => void
  tokenInfo: TokenInfo | null
  search: string
  setSearch: (s: string) => void
  selectedService: ProvisionService | null
  setSelectedService: (s: ProvisionService | null) => void
  selectedCdnService: ProvisionService | null
  setSelectedCdnService: (s: ProvisionService | null) => void
  isDeploying: boolean
  fosStatus: FosStatus
  fosError: string
  terraformFiles: Record<string, string>
  selectedTfFile: string
  setSelectedTfFile: (f: string) => void
  isFetchingTerraform: boolean
  configStatus: Schema<"ProvisionCheckConfigResponse"> | null
  isCheckingConfig: boolean
  handleCheckConfig: () => Promise<void>
  ngwafWorkspaces: { id: string; name: string }[]
  ngwafFetching: boolean
  ngwafFetchError: string
  ngwafDebugRaw: string
  lines: SSELine[]
  status: SSEStatus
  isDone: boolean
  sseError: string | null
  stop: () => void
  handleModalClose: (isOpen: boolean) => void
  onOpenChange: (open: boolean) => void
  config: ProvisionConfig
  setConfig: React.Dispatch<React.SetStateAction<ProvisionConfig>>
  catalog: LogFieldsCatalog | undefined
  isLoadingCatalog: boolean
  toggleGroup: (groupId: string, checked: boolean) => void
  toggleField: (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => void
  updateFieldLimit: (fieldId: string, limit?: number) => void
  togglePreset: (presetGroups: string[]) => void
  isPresetActive: (groups: string[]) => boolean
  estimatedBytes: number
  servicesData: ProvisionService[] | undefined
  servicesError: Error | null
  isLoadingServices: boolean
  filteredServices: ProvisionService[]
  handleTokenSubmit: () => Promise<void>
  validateMutation: ReturnType<typeof useMutation<ValidateResponse | undefined, Error, string>>
  handleServiceSelect: (service: ProvisionService) => void
  handleCheckFos: (vals?: { bucket?: string; region?: string; access_key?: string; secret_key?: string }) => Promise<void>
  checkDomain: (prefix: string) => Promise<void>
  domainStatus: DomainStatus
  domainMessage: string
  setupPhase: SetupPhase
  handleDeploy: () => void
  fetchTerraformPreview: () => Promise<void>
  handleExportTerraform: () => Promise<void>
  handleAdminIngest: () => Promise<void>
  STEPS: { id: Step; label: string }[]
  pendingDraft: WizardDraft | null
  resumeDraft: () => void
  discardDraft: () => void
}
