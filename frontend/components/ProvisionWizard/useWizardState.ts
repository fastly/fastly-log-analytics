"use client";

import React, { useEffect, useEffectEvent, useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { client, extractApiError, unwrapClient } from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";
import { useServiceStore, useTimezoneStore } from "@/stores/filterStore";
import { useSSE } from "@/hooks/useSSE";
import { SHIELD_MAP, showToast } from "@/lib/utils";
import type { Schema } from "@/types/api";
import {
  INITIAL_CONFIG, WIZARD_DRAFT_VERSION, ANALYST_PATH_A_UNSUPPORTED_REASON, getStepsForMode,
  type DomainStatus, type FosStatus, type JoinPhase, type LakeInfo, type PersistedConfig,
  type ProvisionConfig, type ProvisionService, type Step, type TokenInfo, type WizardDraft,
  type WizardMode, type WizardState,
} from "./ProvisionWizard";
import type { components } from "@/types/api.generated";
import {
  buildHandleModalClose, runAdminIngest, runDeploy, runExportTerraform, runFetchTerraformPreview, runJoin,
} from "./wizard-deploy";
import {
  estimateLogFieldBytesPerLine, isLogFieldPresetActive, toggleLogFieldGroup,
  toggleLogFieldOverride, toggleLogFieldPreset, updateLogFieldLimit,
} from "@/lib/query-keys";

export type { WizardState } from "./ProvisionWizard";

interface WizardEffectsArgs {
  open: boolean; step: Step; config: ProvisionConfig;
  setConfig: React.Dispatch<React.SetStateAction<ProvisionConfig>>;
  fosStatus: FosStatus; setFosStatus: (s: FosStatus) => void; setFosError: (s: string) => void;
  setStep: (s: Step) => void; setMode: (m: WizardMode) => void; setSearch: (s: string) => void;
  setSelectedService: (s: ProvisionService | null) => void; setIsDeploying: (b: boolean) => void;
  reset: () => void; resetConfig: () => void; joinPhase: JoinPhase; isAnalyzing: boolean;
  handleAnalyzeLake: () => void; selectedService: ProvisionService | null; token: string;
  setNgwafWorkspaces: (w: { id: string; name: string }[]) => void;
  setNgwafFetchError: (s: string) => void; setNgwafDebugRaw: (s: string) => void;
  setNgwafFetching: (b: boolean) => void; isDone: boolean;
  checkDomain: (prefix: string) => void; pendingDraft?: WizardDraft | null;
}

export function useWizardEffects(args: WizardEffectsArgs) {
  const {
    open, step, config, setConfig, fosStatus, setFosStatus, setFosError,
    setStep, setMode, setSearch, setSelectedService, setIsDeploying,
    reset, resetConfig, joinPhase, isAnalyzing, handleAnalyzeLake,
    selectedService, token, setNgwafWorkspaces, setNgwafFetchError,
    setNgwafDebugRaw, setNgwafFetching, isDone, checkDomain, pendingDraft,
  } = args;

  useEffect(() => {
    const shield = SHIELD_MAP[config.fos_region];
    if (shield && shield !== config.cdn_shield) setConfig((prev) => ({ ...prev, cdn_shield: shield }));
  }, [config.fos_region]);

  useEffect(() => {
    if (open && !pendingDraft) {
      setStep("mode"); setMode(null); setSearch(""); setSelectedService(null);
      setIsDeploying(false); setFosStatus("idle"); setFosError("");
      reset(); resetConfig();
    }
  }, [open, reset, pendingDraft]);

  useEffect(() => {
    if (step === "storage" && config.cdn_prefix) {
      const timer = setTimeout(() => checkDomain(config.cdn_prefix), 500);
      return () => clearTimeout(timer);
    }
  }, [config.cdn_prefix, step]);

  useEffect(() => {
    if (fosStatus !== "idle" && fosStatus !== "checking") {
      setFosStatus("idle"); setFosError("");
    }
  }, [config.fos_bucket_name, config.fos_region, config.fos_access_key, config.fos_secret_key]);

  const autoAnalyzeLake = useEffectEvent(() => {
    if (fosStatus === "success" && step === "join" && joinPhase === "form" && config.endpoint_name && config.cdn_service_name && !isAnalyzing) {
      handleAnalyzeLake();
    }
  });
  useEffect(() => { autoAnalyzeLake(); }, [fosStatus]);

  useEffect(() => {
    if (step !== "ngwaf" || !selectedService?.id) return;
    setNgwafWorkspaces([]); setNgwafFetchError(""); setNgwafDebugRaw(""); setNgwafFetching(true);
    client
      .GET("/api/provision/ngwaf-workspaces", { params: { query: { service_id: selectedService.id, token: token || undefined } } })
      .then((r) => {
        if (r.error) {
          setNgwafFetchError(extractApiError(r.error) || "Could not load workspaces");
        } else {
          const data = r.data;
          setNgwafWorkspaces((data?.workspaces || []).flatMap((w) => (w.id && w.name ? [{ id: w.id, name: w.name }] : [])));
          if (data?._debug_raw) setNgwafDebugRaw(data._debug_raw);
          if (data?.error_hint) setNgwafFetchError(data.error_hint);
        }
      })
      .catch((e: unknown) => setNgwafFetchError(extractApiError(e) || "Could not load workspaces"))
      .finally(() => setNgwafFetching(false));
  }, [step, selectedService?.id, token]);

  const persistNgwafWorkspace = useEffectEvent(() => {
    if (isDone && step === "execute" && config.ngwaf_workspace_id && selectedService?.id) {
      client
        .PATCH("/api/provision/services/{service_id}/ngwaf-workspace", {
          params: { path: { service_id: selectedService.id } },
          headers: { Authorization: `Bearer ${token}` },
          body: { ngwaf_workspace_id: config.ngwaf_workspace_id },
        })
        .then((r) => { if (r?.error) console.error("[wizard] failed to persist ngwaf_workspace_id:", r.error); })
        .catch((e: unknown) => console.error("[wizard] failed to persist ngwaf_workspace_id:", e));
    }
  });
  useEffect(() => { persistNgwafWorkspace(); }, [isDone]);
}

interface JoinCompletionEffectArgs {
  joinPhase: JoinPhase; status: string; config: ProvisionConfig;
  setIsDeploying: (b: boolean) => void; setJoinedServiceId: (id: string) => void;
  setActiveServiceId: (id: string) => void;
  services: { id: string; name: string; accessLevel?: string }[];
  setServices: (s: { id: string; name: string; accessLevel?: string }[]) => void;
  queryClient: Pick<QueryClient, "invalidateQueries">;
  setJoinPhase: (p: JoinPhase) => void; reset: () => void;
}

export function useJoinCompletionEffect(args: JoinCompletionEffectArgs) {
  const {
    joinPhase, status, config, setIsDeploying, setJoinedServiceId,
    setActiveServiceId, services, setServices, queryClient, setJoinPhase, reset,
  } = args;
  useEffect(() => {
    if (joinPhase !== "connecting") return;
    if (status === "done") {
      setIsDeploying(false);
      setJoinedServiceId(config.cdn_service_name);
      if (config.cdn_service_name) {
        setActiveServiceId(config.cdn_service_name);
        if (!services.some((s) => s.id === config.cdn_service_name)) {
          setServices([...services, { id: config.cdn_service_name, name: config.cdn_service_name, accessLevel: "read_only" }]);
        }
      }
      queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() });
      setJoinPhase("done");
    } else if (status === "error") {
      setIsDeploying(false); setJoinPhase("form"); reset();
    }
  }, [joinPhase, status, config.cdn_service_name, setActiveServiceId, setServices, services, queryClient, reset]);
}

type ValidateResponse = components["schemas"]["ProvisionValidateResponse"];

interface ValidateMutationDeps {
  token: string;
  setTokenInfo: (info: { id: string; name: string; type: "user" | "automation" }) => void;
  setConfig: (updater: (prev: ProvisionConfig) => ProvisionConfig) => void;
  setStep: (s: Step) => void;
  mode: string | null;
}

export const validateMutationFn = (token: string) => async (serviceId: string) =>
  await unwrapClient(client.POST("/api/provision/validate", { body: { token, service_id: serviceId } }));

export function buildValidateOnSuccess(deps: ValidateMutationDeps) {
  return (data: ValidateResponse | undefined) => {
    const tokenInfo = data?.token_info;
    if (tokenInfo?.id && tokenInfo.name && tokenInfo.type) {
      deps.setTokenInfo({ id: tokenInfo.id, name: tokenInfo.name, type: tokenInfo.type });
    }
    const defaults = data?.defaults;
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
      }));
    }
    deps.setStep(deps.mode === "join" || deps.mode === "ingest" ? "join" : "features");
  };
}

interface CheckConfigArgs {
  token: string; selectedService: ProvisionService | null; selectedCdnService: ProvisionService | null;
  config: ProvisionConfig; setIsCheckingConfig: (b: boolean) => void;
  setConfigStatus: (s: components["schemas"]["ProvisionCheckConfigResponse"] | null) => void;
}

export async function runCheckConfig(args: CheckConfigArgs) {
  const { token, selectedService, selectedCdnService, config, setIsCheckingConfig, setConfigStatus } = args;
  if (!selectedService || !selectedCdnService || !config.fos_bucket_name) return;
  setIsCheckingConfig(true);
  try {
    const data = await unwrapClient(
      client.GET("/api/provision/check-config", {
        params: { query: { token, service_id: selectedService.id, cdn_service_id: selectedCdnService.id, bucket: config.fos_bucket_name } },
      }),
    );
    if (data) setConfigStatus(data);
  } catch (e) {
    console.error("Failed to check config", e);
  } finally {
    setIsCheckingConfig(false);
  }
}

interface CheckFosArgs {
  vals?: { bucket?: string; region?: string; access_key?: string; secret_key?: string };
  config: ProvisionConfig; setFosStatus: (s: FosStatus) => void; setFosError: (s: string) => void;
}

export async function runCheckFos(args: CheckFosArgs) {
  const { vals, config, setFosStatus, setFosError } = args;
  const bucket = vals?.bucket ?? config.fos_bucket_name;
  const region = vals?.region ?? config.fos_region;
  const access_key = vals?.access_key ?? config.fos_access_key;
  const secret_key = vals?.secret_key ?? config.fos_secret_key;
  if (!bucket || !region || !access_key || !secret_key) return;
  setFosStatus("checking"); setFosError("");
  try {
    const data = await unwrapClient(client.POST("/api/provision/check-fos", { body: { bucket, region, access_key, secret_key } }));
    if (data?.ok) {
      setFosStatus("success");
    } else {
      setFosStatus("error"); setFosError(data?.error || "Failed to connect.");
    }
  } catch (err) {
    setFosStatus("error"); setFosError(err instanceof Error && err.message ? err.message : "An error occurred.");
  }
}

interface CheckDomainArgs {
  prefix: string;
  setDomainStatus: (s: "idle" | "checking" | "available" | "taken" | "error") => void;
  setDomainMessage: (m: string) => void;
}

export async function runCheckDomain(args: CheckDomainArgs) {
  const { prefix, setDomainStatus, setDomainMessage } = args;
  if (!prefix || prefix.length < 3) return;
  setDomainStatus("checking");
  try {
    const data = await unwrapClient(client.GET("/api/provision/check-domain", { params: { query: { prefix } } }));
    if (data?.available) {
      setDomainStatus("available"); setDomainMessage("Domain available!");
    } else {
      setDomainStatus("taken"); setDomainMessage("This domain prefix is already in use.");
    }
  } catch {
    setDomainStatus("error");
  }
}

interface AnalyzeLakeArgs {
  config: ProvisionConfig; icebergMetadataLocation: string; setIsAnalyzing: (b: boolean) => void;
  setLakeInfo: (l: LakeInfo) => void; setImportRange: (r: { start: string; end: string }) => void;
  setStep: (s: Step) => void; setFosStatus: (s: FosStatus) => void; setFosError: (s: string) => void;
}

export async function runAnalyzeLake(args: AnalyzeLakeArgs) {
  const { config, icebergMetadataLocation, setIsAnalyzing, setLakeInfo, setImportRange, setStep, setFosStatus, setFosError } = args;
  setIsAnalyzing(true);
  try {
    const data = await unwrapClient(
      client.POST("/api/provision/lake-info", {
        body: {
          bucket: config.fos_bucket_name, region: config.fos_region, access_key: config.fos_access_key,
          secret_key: config.fos_secret_key, prefix: config.fos_prefix,
          endpoint: config.fos_endpoint || undefined, iceberg_metadata_location: icebergMetadataLocation || undefined,
        },
      }),
    );
    if (data?.ok) {
      setLakeInfo(data);
      if (data.range?.start && data.range?.end) setImportRange({ start: data.range.start, end: data.range.end });
      setStep("analyze");
    } else {
      setFosStatus("error"); setFosError(data?.error || "Failed to analyze data lake.");
    }
  } catch (e) {
    setFosStatus("error"); setFosError(e instanceof Error ? e.message : String(e));
  } finally {
    setIsAnalyzing(false);
  }
}

export const WIZARD_DRAFT_KEY = "provision-wizard-draft-v1";

const SECRET_KEYS = ["fos_access_key", "fos_secret_key", "cdn_secret"] as const;

export function stripSecretsFromConfig(config: ProvisionConfig): PersistedConfig {
  const out: Partial<ProvisionConfig> = { ...config };
  for (const key of SECRET_KEYS) delete out[key];
  return out as PersistedConfig;
}

export function mergePersistedConfig(persisted: PersistedConfig): ProvisionConfig {
  return {
    ...INITIAL_CONFIG,
    ...persisted,
    fos_access_key: "",
    fos_secret_key: "",
    cdn_secret: "",
  };
}

function safeLocalStorage(): Storage | null {
  try {
    return typeof window === "undefined" ? null : window.localStorage;
  } catch {
    return null;
  }
}

const WizardDraftHeaderSchema = z.object({
  version: z.number(),
  mode: z.enum(["provision", "join", "ingest"]),
  step: z.string(),
});

export function loadDraft(): WizardDraft | null {
  const ls = safeLocalStorage();
  if (!ls) return null;
  try {
    const raw = ls.getItem(WIZARD_DRAFT_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as WizardDraft;
    if (!parsed || typeof parsed !== "object") return null;
    if (parsed.version !== WIZARD_DRAFT_VERSION) {
      try { ls.removeItem(WIZARD_DRAFT_KEY); } catch { /* ignore */ }
      return null;
    }
    const check = WizardDraftHeaderSchema.safeParse(parsed);
    if (!check.success || parsed.step === "mode") return null;
    return parsed;
  } catch {
    return null;
  }
}

export function saveDraft(draft: WizardDraft): void {
  const ls = safeLocalStorage();
  if (!ls) return;
  try {
    const sanitized: WizardDraft = {
      ...draft,
      config: stripSecretsFromConfig({ ...INITIAL_CONFIG, ...(draft.config as ProvisionConfig) }),
    };
    ls.setItem(WIZARD_DRAFT_KEY, JSON.stringify(sanitized));
  } catch {
    /* private-mode browsers throw; intentional no-op */
  }
}

export function clearDraft(): void {
  const ls = safeLocalStorage();
  if (!ls) return;
  try { ls.removeItem(WIZARD_DRAFT_KEY); } catch { /* ignore */ }
}

export function subscribeDraft(cb: (next: WizardDraft | null) => void): () => void {
  if (typeof window === "undefined") return () => undefined;
  const handler = (e: StorageEvent) => {
    if (e.key !== WIZARD_DRAFT_KEY) return;
    if (e.newValue === null) {
      cb(null);
      return;
    }
    try {
      const parsed = JSON.parse(e.newValue) as WizardDraft;
      cb(parsed && parsed.version === WIZARD_DRAFT_VERSION ? parsed : null);
    } catch {
      cb(null);
    }
  };
  window.addEventListener("storage", handler);
  return () => window.removeEventListener("storage", handler);
}

export function makeDraftId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `draft-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

export function useWizardState(
  open: boolean,
  onOpenChange: (open: boolean) => void,
): WizardState {
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId);
  const activeServiceId = useServiceStore((s) => s.activeServiceId);
  const setServices = useServiceStore((s) => s.setServices);
  const services = useServiceStore((s) => s.services);
  const { timezone } = useTimezoneStore();
  const queryClient = useQueryClient();
  const [step, setStep] = useState<Step>("mode");
  const [mode, setMode] = useState<WizardMode>(null);
  const [token, setToken] = useState("");
  const [submittedToken, setSubmittedToken] = useState("");

  const [prevOpenForToken, setPrevOpenForToken] = useState(open);
  if (open !== prevOpenForToken) {
    setPrevOpenForToken(open);
    if (!open) setSubmittedToken("");
  }

  const [tokenInfo, setTokenInfo] = useState<TokenInfo | null>(null);
  const [search, setSearch] = useState("");
  const [selectedService, setSelectedService] = useState<ProvisionService | null>(null);
  const [isDeploying, setIsDeploying] = useState(false);
  const [fosStatus, setFosStatus] = useState<FosStatus>("idle");
  const [fosError, setFosError] = useState("");
  const [terraformFiles, setTerraformFiles] = useState<Record<string, string>>({});
  const [selectedTfFile, setSelectedTfFile] = useState<string>("logging_service.tf");
  const [isFetchingTerraform, setIsFetchingTerraform] = useState(false);
  const [selectedCdnService, setSelectedCdnService] = useState<ProvisionService | null>(null);
  const [configStatus, setConfigStatus] = useState<Schema<"ProvisionCheckConfigResponse"> | null>(null);
  const [isCheckingConfig, setIsCheckingConfig] = useState(false);
  const [config, setConfig] = useState<ProvisionConfig>(INITIAL_CONFIG);

  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<{ id: string; name: string }[]>([]);
  const [ngwafFetching, setNgwafFetching] = useState(false);
  const [ngwafFetchError, setNgwafFetchError] = useState("");
  const [ngwafDebugRaw, setNgwafDebugRaw] = useState("");

  const [lakeInfo, setLakeInfo] = useState<LakeInfo | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [importMode, setImportMode] = useState<"all" | "range">("all");
  const [importRange, setImportRange] = useState<{ start: string; end: string }>({ start: "", end: "" });
  const [syncEnabled, setSyncEnabled] = useState(true);

  const capabilityService = React.useMemo(() => {
    if (!services.length) return null;
    if (!activeServiceId) return services[0] ?? null;
    return services.find((service) => service.id === activeServiceId) ?? null;
  }, [activeServiceId, services]);
  const analystPathASupported = capabilityService?.analystPathASupported ?? true;
  const analystPathAReason = analystPathASupported
    ? null
    : capabilityService?.analystPathAReason ?? ANALYST_PATH_A_UNSUPPORTED_REASON;

  const [pendingDraft, setPendingDraft] = useState<WizardDraft | null>(() => (open ? loadDraft() : null));
  const draftIdRef = useRef<string | null>(null);
  const draftCreatedAtRef = useRef<string | null>(null);

  const { lines, status, isDone, error: sseError, start, stop, reset } = useSSE();

  const handleModalClose = buildHandleModalClose({
    status, isDone, onOpenChange, selectedService, setActiveServiceId, queryClient,
    setStep, setMode, setSearch, setSelectedService, setIsDeploying, setFosStatus,
    setFosError, setLakeInfo, setIsAnalyzing, setImportMode, setSyncEnabled, reset,
    resetConfig: () => setConfig({ ...INITIAL_CONFIG }),
    setNgwafWorkspaces, setNgwafFetching, setNgwafFetchError,
  });

  const [domainStatus, setDomainStatus] = useState<DomainStatus>("idle");
  const [domainMessage, setDomainMessage] = useState("");

  const {
    data: servicesData,
    error: servicesError,
    isLoading: isLoadingServices,
  } = useQuery({
    queryKey: ["provision-services", submittedToken],
    queryFn: async () => {
      if (!submittedToken) return [];
      const { data, error } = await client.GET("/api/provision/services", {
        params: { query: { token: submittedToken } },
      });
      if (error) {
        const err = new Error(extractApiError(error)) as Error & { code?: string };
        err.code = (error as { detail?: { error?: string } })?.detail?.error;
        throw err;
      }
      return data ?? [];
    },
    enabled: !!submittedToken,
    retry: false,
  });

  const [prevServicesData, setPrevServicesData] = useState(servicesData);
  if (servicesData !== prevServicesData) {
    setPrevServicesData(servicesData);
    if (servicesData && Array.isArray(servicesData) && step === "token") {
      setStep("service");
    }
  }

  const { data: catalog, isLoading: isLoadingCatalog } = useQuery({
    queryKey: ["services", "catalog"],
    queryFn: async () => unwrapClient(client.GET("/api/log-fields/catalog")),
    enabled: step === "fields",
  });

  const toggleGroup = (groupId: string, checked: boolean) => {
    setConfig((prev) => ({
      ...prev,
      log_fields: toggleLogFieldGroup(prev.log_fields, catalog, groupId, checked),
    }));
  };

  const toggleField = (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => {
    setConfig((prev) => ({
      ...prev,
      log_fields: toggleLogFieldOverride(prev.log_fields, fieldId, checked, defaultEnabledByGroup),
    }));
  };

  const updateFieldLimit = (fieldId: string, limit?: number) => {
    setConfig((prev) => ({
      ...prev,
      log_fields: updateLogFieldLimit(prev.log_fields, fieldId, limit),
    }));
  };

  const isPresetActive = (groups: string[]) => isLogFieldPresetActive(config.log_fields, groups);

  const togglePreset = (presetGroups: string[]) => {
    setConfig((prev) => ({
      ...prev,
      log_fields: toggleLogFieldPreset(prev.log_fields, catalog, presetGroups, isPresetActive),
    }));
  };

  const estimatedBytes = React.useMemo(
    () => estimateLogFieldBytesPerLine(config.log_fields, catalog),
    [catalog, config.log_fields],
  );

  const handleTokenSubmit = async () => {
    if (token) setSubmittedToken(token);
  };

  const validateMutation = useMutation({
    mutationFn: validateMutationFn(token),
    onSuccess: buildValidateOnSuccess({ token, setTokenInfo, setConfig, setStep, mode }),
  });

  const handleServiceSelect = (service: ProvisionService) => {
    if (service.provisioned) return;
    setSelectedService(service);
    validateMutation.mutate(service.id);
  };

  const handleCheckConfig = () =>
    runCheckConfig({ token, selectedService, selectedCdnService, config, setIsCheckingConfig, setConfigStatus });

  const handleCheckFos = (vals?: { bucket?: string; region?: string; access_key?: string; secret_key?: string }) =>
    runCheckFos({ vals, config, setFosStatus, setFosError });

  const checkDomain = (prefix: string) => runCheckDomain({ prefix, setDomainStatus, setDomainMessage });

  const [joinPhase, setJoinPhase] = useState<JoinPhase>("form");
  const [joinedServiceId, setJoinedServiceId] = useState("");
  const [syncIntervalMins, setSyncIntervalMins] = useState("2");
  const [icebergMetadataLocation, setIcebergMetadataLocation] = useState("");

  const resumeDraft = () => {
    if (!pendingDraft) return;
    setMode(pendingDraft.mode);
    setConfig(mergePersistedConfig(pendingDraft.config));
    setSelectedService(
      pendingDraft.selectedServiceId
        ? ({ id: pendingDraft.selectedServiceId, name: pendingDraft.selectedServiceName || pendingDraft.selectedServiceId } as ProvisionService)
        : null,
    );
    setSelectedCdnService(
      pendingDraft.selectedCdnServiceId
        ? ({ id: pendingDraft.selectedCdnServiceId, name: pendingDraft.selectedCdnServiceName || pendingDraft.selectedCdnServiceId } as ProvisionService)
        : null,
    );
    setImportMode(pendingDraft.importMode);
    setImportRange(pendingDraft.importRange);
    setSyncEnabled(pendingDraft.syncEnabled);
    setSyncIntervalMins(pendingDraft.syncIntervalMins);
    setIcebergMetadataLocation(pendingDraft.icebergMetadataLocation);

    let next: Step = pendingDraft.currentStep === "execute" ? "fields" : pendingDraft.currentStep;
    const tokenGatedSteps: Step[] = ["service", "storage", "ngwaf", "fields", "execute"];
    if (tokenGatedSteps.includes(next)) next = "token";
    setStep(next);
    draftIdRef.current = pendingDraft.draftId;
    draftCreatedAtRef.current = pendingDraft.createdAt;
    setPendingDraft(null);
  };

  const discardDraft = () => {
    clearDraft();
    draftIdRef.current = null;
    draftCreatedAtRef.current = null;
    setPendingDraft(null);
  };

  const estimatedImportSize = React.useMemo(() => {
    if (!lakeInfo?.calendar) return 0;
    let total = 0;
    const { start, end } = importRange;
    for (const [dateStr, stats] of Object.entries(lakeInfo.calendar)) {
      if (dateStr === "unknown") continue;
      if (importMode === "range") {
        if (start && dateStr < start.split("T")[0]) continue;
        if (end && dateStr > end.split("T")[0]) continue;
      }
      total += stats.size_bytes;
    }
    return total;
  }, [lakeInfo, importMode, importRange]);

  const handleAnalyzeLake = () =>
    runAnalyzeLake({ config, icebergMetadataLocation, setIsAnalyzing, setLakeInfo, setImportRange, setStep, setFosStatus, setFosError });

  const handleJoin = () =>
    runJoin({
      config, analystPathASupported, syncIntervalMins, syncEnabled, icebergMetadataLocation,
      importMode, importRange, setIsDeploying, setJoinPhase, setStep, reset, start,
    });

  useJoinCompletionEffect({
    joinPhase, status, config, setIsDeploying, setJoinedServiceId,
    setActiveServiceId, services, setServices, queryClient, setJoinPhase, reset,
  });

  const handleFinishJoin = () => {
    onOpenChange(false);
    window.location.reload();
  };

  const STEPS = getStepsForMode(mode, config, step);

  useWizardEffects({
    open, step, config, setConfig, fosStatus, setFosStatus, setFosError, setStep, setMode, setSearch,
    setSelectedService, setIsDeploying, reset, resetConfig: () => setConfig({ ...INITIAL_CONFIG }),
    joinPhase, isAnalyzing, handleAnalyzeLake, selectedService, token, setNgwafWorkspaces,
    setNgwafFetchError, setNgwafDebugRaw, setNgwafFetching, isDone, checkDomain, pendingDraft,
  });

  const handleDeploy = () => runDeploy({ token, selectedService, config, setIsDeploying, start });
  const fetchTerraformPreview = () =>
    runFetchTerraformPreview({ token, selectedService, config, setIsFetchingTerraform, setTerraformFiles, setSelectedTfFile });
  const handleExportTerraform = () => runExportTerraform({ token, selectedService, config });
  const handleAdminIngest = () =>
    runAdminIngest({
      token, selectedService, selectedCdnService, config, services, setIsDeploying,
      setJoinedServiceId, setActiveServiceId, setServices, queryClient, setJoinPhase, setStep,
    });

  const [draftOpen, setDraftOpen] = useState(false);
  if (open !== draftOpen) {
    setDraftOpen(open);
    setPendingDraft(open ? loadDraft() : null);
  }

  useEffect(() => {
    if (!open || !mode || step === "mode" || pendingDraft) return;
    const timer = setTimeout(() => {
      if (!draftIdRef.current) draftIdRef.current = makeDraftId();
      if (!draftCreatedAtRef.current) draftCreatedAtRef.current = new Date().toISOString();
      const draft: WizardDraft = {
        version: WIZARD_DRAFT_VERSION, draftId: draftIdRef.current,
        mode: mode as Exclude<WizardMode, null>, step, currentStep: step,
        selectedServiceId: selectedService?.id ?? null, selectedServiceName: selectedService?.name ?? null,
        selectedCdnServiceId: selectedCdnService?.id ?? null, selectedCdnServiceName: selectedCdnService?.name ?? null,
        tokenInfo, config: stripSecretsFromConfig(config), importMode, importRange,
        syncEnabled, syncIntervalMins, icebergMetadataLocation,
        updatedAt: new Date().toISOString(), createdAt: draftCreatedAtRef.current,
      };
      saveDraft(draft);
    }, 500);
    return () => clearTimeout(timer);
  }, [
    open, pendingDraft, step, mode, selectedService?.id, selectedService?.name,
    selectedCdnService?.id, selectedCdnService?.name, tokenInfo, config,
    importMode, importRange, syncEnabled, syncIntervalMins, icebergMetadataLocation,
  ]);

  useEffect(() => {
    if (!open) return;
    return subscribeDraft((next) => {
      if (!next) return;
      if (draftIdRef.current && next.draftId !== draftIdRef.current) {
        showToast("Wizard updated in another tab. Close and reopen to load latest.", "warn");
      }
    });
  }, [open]);

  useEffect(() => {
    if (status === "done" || isDone) {
      clearDraft();
      draftIdRef.current = null;
      draftCreatedAtRef.current = null;
      queryClient.invalidateQueries({ queryKey: queryKeys.services() });
    }
  }, [status, isDone, queryClient]);

  const filteredServices = Array.isArray(servicesData)
    ? servicesData.filter((s) => s.name.toLowerCase().includes(search.toLowerCase()) || s.id.toLowerCase().includes(search.toLowerCase()))
    : [];

  return {
    setActiveServiceId, setServices, services, timezone, queryClient, step, setStep, mode, setMode,
    token, setToken, tokenInfo, search, setSearch, selectedService, setSelectedService,
    selectedCdnService, setSelectedCdnService, isDeploying, fosStatus, fosError, terraformFiles,
    selectedTfFile, setSelectedTfFile, isFetchingTerraform, configStatus, isCheckingConfig,
    handleCheckConfig, ngwafWorkspaces, ngwafFetching, ngwafFetchError, ngwafDebugRaw,
    lakeInfo, isAnalyzing, analystPathASupported, analystPathAReason, importMode, setImportMode,
    importRange, setImportRange, syncEnabled, setSyncEnabled, lines, status, isDone, sseError,
    stop, handleModalClose, onOpenChange, config, setConfig, catalog, isLoadingCatalog,
    toggleGroup, toggleField, updateFieldLimit, togglePreset, isPresetActive, estimatedBytes,
    servicesData, servicesError, isLoadingServices, filteredServices, handleTokenSubmit,
    validateMutation, handleServiceSelect, handleCheckFos, checkDomain, domainStatus,
    domainMessage, joinPhase, joinedServiceId, syncIntervalMins, setSyncIntervalMins,
    icebergMetadataLocation, setIcebergMetadataLocation, estimatedImportSize, handleAnalyzeLake,
    handleJoin, handleFinishJoin, handleDeploy, fetchTerraformPreview, handleExportTerraform,
    handleAdminIngest, STEPS, pendingDraft, resumeDraft, discardDraft,
  };
}
