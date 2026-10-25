"use client";

import React, { useEffect, useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { client, extractApiError } from "@/lib/api";
import { unwrapClient } from '@/lib/api';
import { queryKeys } from "@/lib/query-keys";
import { useServiceStore } from "@/stores/serviceStore";
import { useTimezoneStore } from "@/stores/timezoneStore";
import { useSSE } from "@/hooks/useSSE";
import { showToast } from '@/lib/utils';
import type { Schema } from "@/types/api";
import {
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  ANALYST_PATH_A_UNSUPPORTED_REASON,
  getStepsForMode,
  type DomainStatus,
  type FosStatus,
  type JoinPhase,
  type LakeInfo,
  type PersistedConfig,
  type ProvisionConfig,
  type ProvisionService,
  type Step,
  type TokenInfo,
  type WizardDraft,
  type WizardMode,
  type WizardState,
} from "./types";
import { useJoinCompletionEffect, useWizardEffects } from "./wizard-effects";
import {
  buildValidateOnSuccess,
  runAnalyzeLake,
  runCheckConfig,
  runCheckDomain,
  runCheckFos,
  validateMutationFn,
} from "./wizard-api";
import {
  buildHandleModalClose,
  runAdminIngest,
  runDeploy,
  runExportTerraform,
  runFetchTerraformPreview,
  runJoin,
} from "./wizard-deploy";
import {
  estimateLogFieldBytesPerLine,
  isLogFieldPresetActive,
  toggleLogFieldGroup,
  toggleLogFieldOverride,
  toggleLogFieldPreset,
  updateLogFieldLimit,
} from '@/lib/query-keys';

// Re-export so consumers (step components) can import WizardState from this hook module
export type { WizardState } from "./types";

export const WIZARD_DRAFT_KEY = "provision-wizard-draft-v1";

const SECRET_KEYS = ["fos_access_key", "fos_secret_key", "cdn_secret"] as const;

export function stripSecretsFromConfig(config: ProvisionConfig): PersistedConfig {
  const out: Partial<ProvisionConfig> = { ...config };
  for (const key of SECRET_KEYS) {
    delete out[key];
  }
  return out as PersistedConfig;
}

export function mergePersistedConfig(
  persisted: PersistedConfig,
): ProvisionConfig {
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
    if (typeof window === "undefined") return null;
    return window.localStorage;
  } catch {
    return null;
  }
}

export function loadDraft(): WizardDraft | null {
  const ls = safeLocalStorage();
  if (!ls) return null;
  try {
    const raw = ls.getItem(WIZARD_DRAFT_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as WizardDraft;
    if (!parsed || typeof parsed !== "object") return null;
    if (parsed.version !== WIZARD_DRAFT_VERSION) {
      try {
        ls.removeItem(WIZARD_DRAFT_KEY);
      } catch {
        /* ignore */
      }
      return null;
    }
    if (!parsed.mode || parsed.step === "mode") return null;
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
      config: stripSecretsFromConfig({
        ...INITIAL_CONFIG,
        ...(draft.config as ProvisionConfig),
      }),
    };
    ls.setItem(WIZARD_DRAFT_KEY, JSON.stringify(sanitized));
  } catch {
    /* private-mode browsers throw; intentional no-op */
  }
}

export function clearDraft(): void {
  const ls = safeLocalStorage();
  if (!ls) return;
  try {
    ls.removeItem(WIZARD_DRAFT_KEY);
  } catch {
    /* ignore */
  }
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
      if (parsed && parsed.version === WIZARD_DRAFT_VERSION) {
        cb(parsed);
      } else {
        cb(null);
      }
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
  const setActiveServiceId = useServiceStore(s => s.setActiveServiceId)
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const setServices = useServiceStore(s => s.setServices)
  const services = useServiceStore(s => s.services);
  const { timezone } = useTimezoneStore();
  const queryClient = useQueryClient();
  const [step, setStep] = useState<Step>("mode");
  const [mode, setMode] = useState<WizardMode>(null);
  const [token, setToken] = useState("");
  const [submittedToken, setSubmittedToken] = useState("");

  // Clear the submitted token when the wizard closes. Derived during render
  // (rather than in a useEffect) to avoid the extra setState-in-effect render
  // pass — see https://react.dev/learn/you-might-not-need-an-effect.
  const [prevOpenForToken, setPrevOpenForToken] = useState(open);
  if (open !== prevOpenForToken) {
    setPrevOpenForToken(open);
    if (!open) {
      setSubmittedToken("");
    }
  }

  const [tokenInfo, setTokenInfo] = useState<TokenInfo | null>(null);
  const [search, setSearch] = useState("");
  const [selectedService, setSelectedService] =
    useState<ProvisionService | null>(null);
  const [isDeploying, setIsDeploying] = useState(false);
  const [fosStatus, setFosStatus] = useState<FosStatus>("idle");
  const [fosError, setFosError] = useState("");
  const [terraformFiles, setTerraformFiles] = useState<Record<string, string>>(
    {},
  );
  const [selectedTfFile, setSelectedTfFile] = useState<string>(
    "logging_service.tf",
  );
  const [isFetchingTerraform, setIsFetchingTerraform] = useState(false);
  const [selectedCdnService, setSelectedCdnService] =
    useState<ProvisionService | null>(null);
  const [configStatus, setConfigStatus] =
    useState<Schema<"ProvisionCheckConfigResponse"> | null>(null);
  const [isCheckingConfig, setIsCheckingConfig] = useState(false);

  const [config, setConfig] = useState<ProvisionConfig>(INITIAL_CONFIG);

  // NGWAF step state
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<
    { id: string; name: string }[]
  >([]);
  const [ngwafFetching, setNgwafFetching] = useState(false);
  const [ngwafFetchError, setNgwafFetchError] = useState("");
  const [ngwafDebugRaw, setNgwafDebugRaw] = useState("");

  // Analyst Flow state
  const [lakeInfo, setLakeInfo] = useState<LakeInfo | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [importMode, setImportMode] = useState<"all" | "range">("all");
  const [importRange, setImportRange] = useState<{
    start: string;
    end: string;
  }>({ start: "", end: "" });
  const [syncEnabled, setSyncEnabled] = useState(true);
  const capabilityService = React.useMemo(() => {
    if (!services.length) return null;
    if (!activeServiceId) return services[0] ?? null;
    return services.find((service) => service.id === activeServiceId) ?? null;
  }, [activeServiceId, services]);
  const analystPathASupported =
    capabilityService?.analystPathASupported ?? true;
  const analystPathAReason =
    analystPathASupported
      ? null
      : capabilityService?.analystPathAReason ??
        ANALYST_PATH_A_UNSUPPORTED_REASON;

  const [pendingDraft, setPendingDraft] = useState<WizardDraft | null>(() =>
    open ? loadDraft() : null,
  );
  const draftIdRef = useRef<string | null>(null);
  const draftCreatedAtRef = useRef<string | null>(null);

  const {
    lines,
    status,
    isDone,
    error: sseError,
    start,
    stop,
    reset,
  } = useSSE();

  const handleModalClose = buildHandleModalClose({
    status,
    isDone,
    onOpenChange,
    selectedService,
    setActiveServiceId,
    queryClient,
    setStep,
    setMode,
    setSearch,
    setSelectedService,
    setIsDeploying,
    setFosStatus,
    setFosError,
    setLakeInfo,
    setIsAnalyzing,
    setImportMode,
    setSyncEnabled,
    reset,
    resetConfig: () => setConfig({ ...INITIAL_CONFIG }),
    setNgwafWorkspaces,
    setNgwafFetching,
    setNgwafFetchError,
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
      // Surface backend errors (e.g. object_storage_not_enabled, invalid token)
      // as the query error so TokenStep renders the actionable message instead
      // of silently returning undefined and stalling on "Fetch Services".
      // Preserve the machine-readable code so the UI can render a richer message
      // (e.g. an "Enable Object Storage" link) for specific cases.
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

  // Advance to the service step once services arrive. Derived during render
  // (rather than in a useEffect) to avoid the setState-in-effect cascade —
  // see https://react.dev/learn/you-might-not-need-an-effect.
  const [prevServicesData, setPrevServicesData] = useState(servicesData);
  if (servicesData !== prevServicesData) {
    setPrevServicesData(servicesData);
    if (servicesData && Array.isArray(servicesData) && step === "token") {
      setStep("service");
    }
  }

  // ── Step 4: Catalog ──
  const { data: catalog, isLoading: isLoadingCatalog } = useQuery({
    queryKey: ["services", "catalog"],
    queryFn: async () => {
      return await unwrapClient(client.GET("/api/log-fields/catalog"));
    },
    enabled: step === "fields",
  });

  // ── Field handlers ──
  const toggleGroup = (groupId: string, checked: boolean) => {
    setConfig((prev) => ({
      ...prev,
      log_fields: toggleLogFieldGroup(prev.log_fields, catalog, groupId, checked),
    }));
  };

  const toggleField = (
    fieldId: string,
    checked: boolean,
    defaultEnabledByGroup: boolean,
  ) => {
    setConfig((prev) => ({
      ...prev,
      log_fields: toggleLogFieldOverride(
        prev.log_fields,
        fieldId,
        checked,
        defaultEnabledByGroup,
      ),
    }));
  };

  const updateFieldLimit = (fieldId: string, limit?: number) => {
    setConfig((prev) => ({
      ...prev,
      log_fields: updateLogFieldLimit(prev.log_fields, fieldId, limit),
    }));
  };

  const isPresetActive = (groups: string[]) =>
    isLogFieldPresetActive(config.log_fields, groups);

  const togglePreset = (presetGroups: string[]) => {
    setConfig((prev) => ({
      ...prev,
      log_fields: toggleLogFieldPreset(
        prev.log_fields,
        catalog,
        presetGroups,
        isPresetActive,
      ),
    }));
  };

  const estimatedBytes = React.useMemo(
    () => estimateLogFieldBytesPerLine(config.log_fields, catalog),
    [catalog, config.log_fields],
  );

  const handleTokenSubmit = async () => {
    if (token) {
      setSubmittedToken(token);
    }
  };

  // ── Step 2: Service ──
  const validateMutation = useMutation({
    mutationFn: validateMutationFn(token),
    onSuccess: buildValidateOnSuccess({
      token,
      setTokenInfo,
      setConfig,
      setStep,
      mode,
    }),
  });

  const handleServiceSelect = (service: ProvisionService) => {
    if (service.provisioned) return;
    setSelectedService(service);
    validateMutation.mutate(service.id);
  };

  const handleCheckConfig = () =>
    runCheckConfig({
      token,
      selectedService,
      selectedCdnService,
      config,
      setIsCheckingConfig,
      setConfigStatus,
    });

  const handleCheckFos = (vals?: {
    bucket?: string;
    region?: string;
    access_key?: string;
    secret_key?: string;
  }) =>
    runCheckFos({
      vals,
      config,
      setFosStatus,
      setFosError,
    });

  const checkDomain = (prefix: string) =>
    runCheckDomain({ prefix, setDomainStatus, setDomainMessage });

  // join flow phases: form → connecting (SSE) → importing (SSE) → done
  const [joinPhase, setJoinPhase] = useState<JoinPhase>("form");
  const [joinedServiceId, setJoinedServiceId] = useState("");
  const [syncIntervalMins, setSyncIntervalMins] = useState("2");
  const [icebergMetadataLocation, setIcebergMetadataLocation] = useState("");

  const resumeDraft = () => {
    if (!pendingDraft) return;
    const merged = mergePersistedConfig(pendingDraft.config);
    setMode(pendingDraft.mode);
    setConfig(merged);
    setSelectedService(
      pendingDraft.selectedServiceId
        ? ({
            id: pendingDraft.selectedServiceId,
            name:
              pendingDraft.selectedServiceName ||
              pendingDraft.selectedServiceId,
          } as ProvisionService)
        : null,
    );
    setSelectedCdnService(
      pendingDraft.selectedCdnServiceId
        ? ({
            id: pendingDraft.selectedCdnServiceId,
            name:
              pendingDraft.selectedCdnServiceName ||
              pendingDraft.selectedCdnServiceId,
          } as ProvisionService)
        : null,
    );
    setImportMode(pendingDraft.importMode);
    setImportRange(pendingDraft.importRange);
    setSyncEnabled(pendingDraft.syncEnabled);
    setSyncIntervalMins(pendingDraft.syncIntervalMins);
    setIcebergMetadataLocation(pendingDraft.icebergMetadataLocation);

    let next: Step = pendingDraft.currentStep;
    if (next === "execute") next = "fields";
    const tokenGatedSteps: Step[] = [
      "service",
      "storage",
      "ngwaf",
      "fields",
      "execute",
    ];
    if (tokenGatedSteps.includes(next)) {
      next = "token";
    }
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
    const start = importRange.start;
    const end = importRange.end;

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
    runAnalyzeLake({
      config,
      icebergMetadataLocation,
      setIsAnalyzing,
      setLakeInfo,
      setImportRange,
      setStep,
      setFosStatus,
      setFosError,
    });

  const handleJoin = () =>
    runJoin({
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
    });

  useJoinCompletionEffect({
    joinPhase,
    status,
    config,
    setIsDeploying,
    setJoinedServiceId,
    setActiveServiceId,
    services,
    setServices,
    queryClient,
    setJoinPhase,
    reset,
  });

  const handleFinishJoin = () => {
    onOpenChange(false);
    window.location.reload();
  };

  const STEPS = getStepsForMode(mode, config, step);

  useWizardEffects({
    open,
    step,
    config,
    setConfig,
    fosStatus,
    setFosStatus,
    setFosError,
    setStep,
    setMode,
    setSearch,
    setSelectedService,
    setIsDeploying,
    reset,
    resetConfig: () => setConfig({ ...INITIAL_CONFIG }),
    joinPhase,
    isAnalyzing,
    handleAnalyzeLake,
    selectedService,
    token,
    setNgwafWorkspaces,
    setNgwafFetchError,
    setNgwafDebugRaw,
    setNgwafFetching,
    isDone,
    checkDomain,
    pendingDraft,
  });

  const handleDeploy = () =>
    runDeploy({ token, selectedService, config, setIsDeploying, start });

  const fetchTerraformPreview = () =>
    runFetchTerraformPreview({
      token,
      selectedService,
      config,
      setIsFetchingTerraform,
      setTerraformFiles,
      setSelectedTfFile,
    });

  const handleExportTerraform = () =>
    runExportTerraform({ token, selectedService, config });

  const handleAdminIngest = () =>
    runAdminIngest({
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
    });

  // The saved draft is loaded when the wizard opens and dropped when it closes (adjusted during
  // render, keyed on `open`).
  const [draftOpen, setDraftOpen] = useState(false);
  if (open !== draftOpen) {
    setDraftOpen(open);
    setPendingDraft(open ? loadDraft() : null);
  }

  useEffect(() => {
    if (!open) return;
    if (!mode || step === "mode") return;
    if (pendingDraft) return;
    const timer = setTimeout(() => {
      if (!draftIdRef.current) draftIdRef.current = makeDraftId();
      if (!draftCreatedAtRef.current) {
        draftCreatedAtRef.current = new Date().toISOString();
      }
      const draft: WizardDraft = {
        version: WIZARD_DRAFT_VERSION,
        draftId: draftIdRef.current,
        mode: mode as Exclude<WizardMode, null>,
        step,
        currentStep: step,
        selectedServiceId: selectedService?.id ?? null,
        selectedServiceName: selectedService?.name ?? null,
        selectedCdnServiceId: selectedCdnService?.id ?? null,
        selectedCdnServiceName: selectedCdnService?.name ?? null,
        tokenInfo,
        config: stripSecretsFromConfig(config),
        importMode,
        importRange,
        syncEnabled,
        syncIntervalMins,
        icebergMetadataLocation,
        updatedAt: new Date().toISOString(),
        createdAt: draftCreatedAtRef.current,
      };
      saveDraft(draft);
    }, 500);
    return () => clearTimeout(timer);
  }, [
    open,
    pendingDraft,
    step,
    mode,
    selectedService?.id,
    selectedService?.name,
    selectedCdnService?.id,
    selectedCdnService?.name,
    tokenInfo,
    config,
    importMode,
    importRange,
    syncEnabled,
    syncIntervalMins,
    icebergMetadataLocation,
  ]);

  useEffect(() => {
    if (!open) return;
    return subscribeDraft((next) => {
      if (!next) return;
      if (draftIdRef.current && next.draftId !== draftIdRef.current) {
        showToast(
          "Wizard updated in another tab. Close and reopen to load latest.",
          "warn",
        );
      }
    });
  }, [open]);

  useEffect(() => {
    if (status === "done" || isDone) {
      clearDraft();
      draftIdRef.current = null;
      draftCreatedAtRef.current = null;
      queryClient.invalidateQueries({ queryKey: queryKeys.services() })
    }
  }, [status, isDone, queryClient]);

  const filteredServices = Array.isArray(servicesData)
    ? servicesData.filter(
        (s) =>
          s.name.toLowerCase().includes(search.toLowerCase()) ||
          s.id.toLowerCase().includes(search.toLowerCase()),
      )
    : [];

  return {
    setActiveServiceId,
    setServices,
    services,
    timezone,
    queryClient,
    step,
    setStep,
    mode,
    setMode,
    token,
    setToken,
    tokenInfo,
    search,
    setSearch,
    selectedService,
    setSelectedService,
    selectedCdnService,
    setSelectedCdnService,
    isDeploying,
    fosStatus,
    fosError,
    terraformFiles,
    selectedTfFile,
    setSelectedTfFile,
    isFetchingTerraform,
    configStatus,
    isCheckingConfig,
    handleCheckConfig,
    ngwafWorkspaces,
    ngwafFetching,
    ngwafFetchError,
    ngwafDebugRaw,
    lakeInfo,
    isAnalyzing,
    analystPathASupported,
    analystPathAReason,
    importMode,
    setImportMode,
    importRange,
    setImportRange,
    syncEnabled,
    setSyncEnabled,
    lines,
    status,
    isDone,
    sseError,
    stop,
    handleModalClose,
    onOpenChange,
    config,
    setConfig,
    catalog,
    isLoadingCatalog,
    toggleGroup,
    toggleField,
    updateFieldLimit,
    togglePreset,
    isPresetActive,
    estimatedBytes,
    servicesData,
    servicesError,
    isLoadingServices,
    filteredServices,
    handleTokenSubmit,
    validateMutation,
    handleServiceSelect,
    handleCheckFos,
    checkDomain,
    domainStatus,
    domainMessage,
    joinPhase,
    joinedServiceId,
    syncIntervalMins,
    setSyncIntervalMins,
    icebergMetadataLocation,
    setIcebergMetadataLocation,
    estimatedImportSize,
    handleAnalyzeLake,
    handleJoin,
    handleFinishJoin,
    handleDeploy,
    fetchTerraformPreview,
    handleExportTerraform,
    handleAdminIngest,
    STEPS,
    pendingDraft,
    resumeDraft,
    discardDraft,
  };
}
