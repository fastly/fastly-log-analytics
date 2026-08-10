import type React from "react";
import type { useMutation, useQueryClient } from "@tanstack/react-query";
import type { Service } from "@/stores/serviceStore";
import type { SSELine, SSEStatus } from "@/hooks/useSSE";
import type { components } from "@/types/api.generated";

export type ProvisionService = components["schemas"]["ProvisionService"];

export const WIZARD_DRAFT_VERSION = 1;

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
  | "join"
  | "analyze"
  | "settings"
  | "confirm";

export type WizardMode = "provision" | "join" | "ingest" | null;

export type FosStatus = "idle" | "checking" | "success" | "error";

export type DomainStatus = "idle" | "checking" | "available" | "taken" | "error";

export type JoinPhase = "form" | "connecting" | "importing" | "done";

export interface TokenInfo {
  id: string;
  name: string;
  type: "user" | "automation";
}

export interface ProvisionConfig {
  endpoint_name: string;
  fos_region: string;
  fos_endpoint: string;
  fos_bucket_name: string;
  fos_prefix: string;
  fos_access_key: string;
  fos_secret_key: string;
  sample_rate: number;
  edge_only: boolean;
  custom_condition: string;
  log_period: number;
  cdn_service_name: string;
  cdn_prefix: string;
  cdn_shield: string;
  cdn_url: string;
  cdn_secret: string;
  enable_cron_sync: boolean;
  delete_after: boolean;
  commit_interval_mins: number;
  enable_cron_compact: boolean;
  log_fields: any;
  ngwaf_workspace_id: string;
  cmcd_enabled: boolean;
  cmcd_mode: string;
  cmcd_version: number;
  logging_enabled: boolean;
  rum_enabled: boolean;
  log_retention_days: number;
  rum_retention_days: number;
  // Pinned Faro Web SDK version chosen in the RUM version picker
  // (StorageStep). null = unpinned — the backend serves whatever is
  // currently pinned on the service, or nothing if RUM was never enabled.
  faro_version: string | null;
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
  cdn_service_name: "",
  cdn_prefix: "",
  cdn_shield: "iad-va-us",
  cdn_url: "",
  cdn_secret: "",
  enable_cron_sync: true,
  delete_after: true,
  commit_interval_mins: 5,
  enable_cron_compact: true,
  log_fields: { groups: [], field_overrides: {} } as any,
  ngwaf_workspace_id: "",
  cmcd_enabled: false,
  cmcd_mode: "query_string",
  cmcd_version: 1,
  logging_enabled: true,
  rum_enabled: false,
  log_retention_days: 14,
  rum_retention_days: 90,
  faro_version: null,
};

export type PersistedConfig = Omit<
  ProvisionConfig,
  "fos_access_key" | "fos_secret_key" | "cdn_secret"
>;

export interface WizardDraft {
  version: number;
  draftId: string;
  mode: Exclude<WizardMode, null>;
  step: Step;
  currentStep: Step;
  selectedServiceId: string | null;
  selectedServiceName: string | null;
  selectedCdnServiceId: string | null;
  selectedCdnServiceName: string | null;
  tokenInfo: TokenInfo | null;
  config: PersistedConfig;
  importMode: "all" | "range";
  importRange: { start: string; end: string };
  syncEnabled: boolean;
  syncIntervalMins: string;
  icebergMetadataLocation: string;
  updatedAt: string;
  createdAt: string;
}

// Mapping from Fastly Object Storage region to Fastly Shield POP
export const SHIELD_MAP: Record<string, string> = {
  "us-east-1": "iad-va-us", // Ashburn, VA
  "us-west": "bfi-wa-us", // Seattle, WA (BFI)
  "us-central-1": "chi-il-us", // Chicago, IL (CHI)
  "eu-central": "frankfurt-de", // Frankfurt, Germany
  "eu-south-1": "mxp-milan-it", // Milan, Italy
  "uk-east-1": "london-uk", // London, UK
  "jp-central-1": "nrt-tokyo-jp", // Tokyo, Japan (NRT)
  "au-east-1": "sydney-au", // Sydney, Australia
};

export const REGION_LABELS: Record<string, string> = {
  "us-east-1": "US East (Ashburn)",
  "us-west": "US West (Seattle)",
  "us-central-1": "US Central (Chicago)",
  "eu-central": "EU Central (Frankfurt)",
  "eu-south-1": "EU South (Milan)",
  "uk-east-1": "UK East (London)",
  "jp-central-1": "JP Central (Tokyo)",
  "au-east-1": "AU East (Sydney)",
};

export const SHIELD_LABELS: Record<string, string> = {
  none: "None",
  "iad-va-us": "IAD (Ashburn)",
  "bfi-wa-us": "BFI (Seattle)",
  "chi-il-us": "CHI (Chicago)",
  "frankfurt-de": "FRA (Frankfurt)",
  "mxp-milan-it": "MXP (Milan)",
  "london-uk": "LHR (London)",
  "nrt-tokyo-jp": "NRT (Tokyo)",
  "sydney-au": "SYD (Sydney)",
};

export function getStepsForMode(
  mode: WizardMode,
  config?: { logging_enabled?: boolean }
): { id: Step; label: string }[] {
  if (mode === "join") {
    return [
      { id: "mode", label: "Role" },
      { id: "join", label: "Connect" },
      { id: "analyze", label: "Analyze" },
      { id: "settings", label: "Settings" },
      { id: "confirm", label: "Confirm" },
    ];
  }

  const steps = [
    { id: "mode" as Step, label: "Role" },
    { id: "token" as Step, label: "Auth" },
    { id: "service" as Step, label: "Service" },
    { id: "features" as Step, label: "Features" },
    { id: "storage" as Step, label: "Storage" },
  ];

  if (!config || config.logging_enabled !== false) {
    steps.push(
      { id: "ngwaf" as Step, label: "NGWAF" },
      { id: "fields" as Step, label: "Log Fields" }
    );
  }

  steps.push({ id: "execute" as Step, label: "Review" });
  return steps;
}

export const PERIOD_LABELS: Record<string, string> = {
  "1": "1 second",
  "5": "5 seconds",
  "10": "10 seconds",
  "20": "20 seconds",
  "30": "30 seconds",
  "60": "1 minute",
  "120": "2 minutes",
  "300": "5 minutes",
};

export interface ProvisionWizardProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export interface WizardState {
  // Stores
  setActiveServiceId: (id: string) => void;
  setServices: (services: Service[]) => void;
  services: Service[];
  timezone: string;
  queryClient: ReturnType<typeof useQueryClient>;

  // Step state
  step: Step;
  setStep: (s: Step) => void;
  mode: WizardMode;
  setMode: (m: WizardMode) => void;

  // Token / Service
  token: string;
  setToken: (t: string) => void;
  tokenInfo: TokenInfo | null;
  search: string;
  setSearch: (s: string) => void;
  selectedService: ProvisionService | null;
  setSelectedService: (s: ProvisionService | null) => void;
  selectedCdnService: ProvisionService | null;
  setSelectedCdnService: (s: ProvisionService | null) => void;

  // Provision / Deploy
  isDeploying: boolean;
  fosStatus: FosStatus;
  fosError: string;
  terraformFiles: Record<string, string>;
  selectedTfFile: string;
  setSelectedTfFile: (f: string) => void;
  isFetchingTerraform: boolean;
  configStatus: {
    logging_service: { ok: boolean; details: string };
    cdn_service: { ok: boolean; details: string };
  } | null;
  isCheckingConfig: boolean;
  handleCheckConfig: () => Promise<void>;

  // NGWAF
  ngwafWorkspaces: { id: string; name: string }[];
  ngwafFetching: boolean;
  ngwafFetchError: string;
  ngwafDebugRaw: string;

  // Analyst Flow
  lakeInfo: any;
  isAnalyzing: boolean;
  importMode: "all" | "range";
  setImportMode: (m: "all" | "range") => void;
  importRange: { start: string; end: string };
  setImportRange: React.Dispatch<
    React.SetStateAction<{ start: string; end: string }>
  >;
  syncEnabled: boolean;
  setSyncEnabled: (b: boolean) => void;

  // SSE
  lines: SSELine[];
  status: SSEStatus;
  isDone: boolean;
  sseError: string | null;
  stop: () => void;

  // Modal
  handleModalClose: (isOpen: boolean) => void;
  onOpenChange: (open: boolean) => void;

  // Config
  config: ProvisionConfig;
  setConfig: React.Dispatch<React.SetStateAction<ProvisionConfig>>;

  // Catalog
  catalog: any;
  isLoadingCatalog: boolean;

  // Field helpers
  toggleGroup: (groupId: string, checked: boolean) => void;
  toggleField: (
    fieldId: string,
    checked: boolean,
    defaultEnabledByGroup: boolean,
  ) => void;
  updateFieldLimit: (fieldId: string, limit?: number) => void;
  togglePreset: (presetGroups: string[]) => void;
  isPresetActive: (groups: string[]) => boolean;
  estimatedBytes: number;

  // Services list
  servicesData: any;
  servicesError: Error | null;
  isLoadingServices: boolean;
  filteredServices: any[];

  // Handlers
  handleTokenSubmit: () => Promise<void>;
  validateMutation: ReturnType<typeof useMutation<any, any, string>>;
  handleServiceSelect: (service: ProvisionService) => void;
  handleCheckFos: (vals?: {
    bucket?: string;
    region?: string;
    access_key?: string;
    secret_key?: string;
  }) => Promise<void>;
  checkDomain: (prefix: string) => Promise<void>;
  domainStatus: DomainStatus;
  domainMessage: string;

  // Join flow
  joinPhase: JoinPhase;
  joinedServiceId: string;
  syncIntervalMins: string;
  setSyncIntervalMins: (s: string) => void;
  icebergMetadataLocation: string;
  setIcebergMetadataLocation: (s: string) => void;
  estimatedImportSize: number;
  handleAnalyzeLake: () => Promise<void>;
  handleJoin: () => void;
  handleFinishJoin: () => void;

  handleDeploy: () => void;
  fetchTerraformPreview: () => Promise<void>;
  handleExportTerraform: () => Promise<void>;
  handleAdminIngest: () => Promise<void>;

  STEPS: { id: Step; label: string }[];

  // U-7 Resume-from-failure
  pendingDraft: WizardDraft | null;
  resumeDraft: () => void;
  discardDraft: () => void;
}
