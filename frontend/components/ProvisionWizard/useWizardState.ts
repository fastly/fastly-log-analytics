"use client";

import React, { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { client } from "@/lib/api";
import { useServiceStore } from "@/stores/serviceStore";
import { useTimezoneStore } from "@/stores/timezoneStore";
import { useSSE } from "@/hooks/useSSE";
import {
  INITIAL_CONFIG,
  getStepsForMode,
  type DomainStatus,
  type FosStatus,
  type JoinPhase,
  type ProvisionConfig,
  type ProvisionService,
  type Step,
  type TokenInfo,
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
  const { setActiveServiceId, setServices, services } = useServiceStore();
  const { timezone } = useTimezoneStore();
  const queryClient = useQueryClient();
  const [step, setStep] = useState<Step>("mode");
  const [mode, setMode] = useState<WizardMode>(null);
  const [token, setToken] = useState("");
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

  // ── Step 1: Token ──
  const {
    data: servicesData,
    error: servicesError,
    isLoading: isLoadingServices,
    refetch: fetchServices,
  } = useQuery({
    queryKey: ["provision-services"],
    queryFn: async () => {
      const { data } = await client.GET("/api/provision/services", {
        params: { query: { token } },
      });
      return data as any;
    },
    enabled: false,
    retry: false,
  });

  // ── Step 4: Catalog ──
  const { data: catalog, isLoading: isLoadingCatalog } = useQuery({
    queryKey: ["services", "catalog"],
    queryFn: async () => {
      const { data } = await client.GET("/api/log-fields/catalog");
      return data as any;
    },
    enabled: step === "fields",
  });

  // ── Field handlers (built from pure transforms in wizard-handlers) ──
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
    const res = await fetchServices();
    if (res.data && Array.isArray(res.data)) {
      setStep("service");
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

  const STEPS = getStepsForMode(mode);

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
  };
}
