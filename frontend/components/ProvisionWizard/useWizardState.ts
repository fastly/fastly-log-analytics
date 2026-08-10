"use client";

import React, { useEffect, useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { client, extractApiError } from "@/lib/api";
import { useServiceStore } from "@/stores/serviceStore";
import { useTimezoneStore } from "@/stores/timezoneStore";
import { useSSE } from "@/hooks/useSSE";
import { showToast } from "@/lib/toast";
import {
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  getStepsForMode,
  type DomainStatus,
  type FosStatus,
  type JoinPhase,
  type ProvisionConfig,
  type ProvisionService,
  type Step,
  type TokenInfo,
  type WizardDraft,
  type WizardMode,
  type WizardState,
} from "./types";
import {
  clearDraft,
  loadDraft,
  makeDraftId,
  mergePersistedConfig,
  saveDraft,
  stripSecretsFromConfig,
  subscribeDraft,
} from "./wizard-draft";
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
  applyToggleField,
  applyUpdateFieldLimit,
  buildToggleGroup,
  buildTogglePreset,
} from "./wizard-config-helpers";

// Re-export so consumers (step components) can import WizardState from this hook module
export type { WizardState } from "./types";

export function useWizardState(
  open: boolean,
  onOpenChange: (open: boolean) => void,
): WizardState {
  const setActiveServiceId = useServiceStore(s => s.setActiveServiceId)
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
  const [configStatus, setConfigStatus] = useState<{
    logging_service: { ok: boolean; details: string };
    cdn_service: { ok: boolean; details: string };
  } | null>(null);
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
  const [lakeInfo, setLakeInfo] = useState<any>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [importMode, setImportMode] = useState<"all" | "range">("all");
  const [importRange, setImportRange] = useState<{
    start: string;
    end: string;
  }>({ start: "", end: "" });
  const [syncEnabled, setSyncEnabled] = useState(true);

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
      return data as any;
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
      const { data } = await client.GET("/api/log-fields/catalog");
      return data as any;
    },
    enabled: step === "fields",
  });

  // ── Field handlers (built from pure transforms in wizard-config-helpers) ──
  const toggleGroup = (groupId: string, checked: boolean) => {
    setConfig((prev) => buildToggleGroup(catalog)(prev, groupId, checked));
  };

  const toggleField = (
    fieldId: string,
    checked: boolean,
    defaultEnabledByGroup: boolean,
  ) => {
    setConfig((prev) =>
      applyToggleField(prev, fieldId, checked, defaultEnabledByGroup),
    );
  };

  const updateFieldLimit = (fieldId: string, limit?: number) => {
    setConfig((prev) => applyUpdateFieldLimit(prev, fieldId, limit));
  };

  const isPresetActive = (groups: string[]) => {
    if (!groups.length) return false;
    const currentGroups = new Set(config.log_fields.groups || []);
    return groups.every((g) => currentGroups.has(g));
  };

  const togglePreset = (presetGroups: string[]) => {
    setConfig((prev) =>
      buildTogglePreset(catalog, isPresetActive)(prev, presetGroups),
    );
  };

  const estimatedBytes = React.useMemo(() => {
    if (!catalog?.fields) return 0;
    let total = 0;
    const enabledGroups = new Set(config.log_fields.groups || []);
    const overrides = config.log_fields.field_overrides || {};
    for (const field of catalog.fields) {
      const inGroup = field.group === null || enabledGroups.has(field.group);
      const override = overrides[field.id];
      if (override === true) {
        total += field.typical_bytes || 0;
        continue;
      }
      if (override === false) continue;
      if (inGroup) total += field.typical_bytes || 0;
    }
    return total;
  }, [catalog, config.log_fields]);

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

      total += (stats as any).size_bytes || 0;
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

  const STEPS = getStepsForMode(mode, config);

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

  useEffect(() => {
    if (!open) {
      return;
    }
    setPendingDraft(loadDraft());
    return () => {
      setPendingDraft(null);
    };
  }, [open]);

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
      queryClient.invalidateQueries({ queryKey: ['services'] })
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
