"use client"

import React, { useState, useEffect, useEffectEvent, useRef } from 'react'
import { useMutation, useQueryClient, useQuery, type QueryClient } from '@tanstack/react-query'
import {
  client,
  unwrapClient,
  queryKeys,
  extractApiError,
  estimateLogFieldBytesPerLine,
  isLogFieldPresetActive,
  toggleLogFieldGroup,
  toggleLogFieldOverride,
  toggleLogFieldPreset,
  updateLogFieldLimit,
} from '@/lib/api'
import { SHIELD_MAP, showToast } from '@/lib/utils'
import { useServiceStore, useTimezoneStore } from '@/stores/filterStore'
import { useSSE } from '@/hooks/useSSE'
import type { Schema } from '@/types/api'
import {
  ANALYST_PATH_A_UNSUPPORTED_REASON,
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  getStepsForMode,
  type DomainStatus,
  type FosStatus,
  type JoinPhase,
  type LakeInfo,
  type ProvisionConfig,
  type ProvisionService,
  type Step,
  type TokenInfo,
  type WizardDraft,
  type WizardMode,
  type WizardState,
} from './types'
import {
  buildHandleModalClose,
  buildValidateOnSuccess,
  clearDraft,
  loadDraft,
  makeDraftId,
  mergePersistedConfig,
  registerServiceAndInvalidate,
  runAdminIngest,
  runAnalyzeLake,
  runCheckConfig,
  runCheckDomain,
  runCheckFos,
  runDeploy,
  runExportTerraform,
  runFetchTerraformPreview,
  runJoin,
  saveDraft,
  stripSecretsFromConfig,
  subscribeDraft,
  validateMutationFn,
} from './wizardActions'

export * from './types'
export * from './wizardActions'

export interface WizardEffectsArgs {
  open: boolean
  step: Step
  config: ProvisionConfig
  setConfig: React.Dispatch<React.SetStateAction<ProvisionConfig>>
  fosStatus: FosStatus
  setFosStatus: (s: FosStatus) => void
  setFosError: (s: string) => void
  setStep: (s: Step) => void
  setMode: (m: WizardMode) => void
  setSearch: (s: string) => void
  setSelectedService: (s: ProvisionService | null) => void
  setIsDeploying: (b: boolean) => void
  reset: () => void
  resetConfig: () => void
  joinPhase: JoinPhase
  isAnalyzing: boolean
  handleAnalyzeLake: () => void
  selectedService: ProvisionService | null
  token: string
  setNgwafWorkspaces: (w: { id: string; name: string }[]) => void
  setNgwafFetchError: (s: string) => void
  setNgwafDebugRaw: (s: string) => void
  setNgwafFetching: (b: boolean) => void
  isDone: boolean
  checkDomain: (prefix: string) => void
  pendingDraft?: WizardDraft | null
}

export function useWizardEffects(args: WizardEffectsArgs) {
  const {
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
    resetConfig,
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
  } = args

  useEffect(() => {
    const shield = SHIELD_MAP[config.fos_region]
    if (shield && shield !== config.cdn_shield) {
      setConfig((prev) => ({ ...prev, cdn_shield: shield }))
    }
  }, [config.fos_region])

  useEffect(() => {
    if (open && !pendingDraft) {
      setStep("mode")
      setMode(null)
      setSearch("")
      setSelectedService(null)
      setIsDeploying(false)
      setFosStatus("idle")
      setFosError("")
      reset()
      resetConfig()
    }
  }, [open, reset, pendingDraft])

  useEffect(() => {
    if (step === "storage" && config.cdn_prefix) {
      const timer = setTimeout(() => checkDomain(config.cdn_prefix), 500)
      return () => clearTimeout(timer)
    }
  }, [config.cdn_prefix, step])

  useEffect(() => {
    if (fosStatus !== "idle" && fosStatus !== "checking") {
      setFosStatus("idle")
      setFosError("")
    }
  }, [config.fos_bucket_name, config.fos_region, config.fos_access_key, config.fos_secret_key])

  const autoAnalyzeLake = useEffectEvent(() => {
    if (
      fosStatus === "success" &&
      step === "join" &&
      joinPhase === "form" &&
      config.endpoint_name &&
      config.cdn_service_name &&
      !isAnalyzing
    ) {
      handleAnalyzeLake()
    }
  })
  useEffect(() => {
    autoAnalyzeLake()
  }, [fosStatus])

  useEffect(() => {
    if (step !== "ngwaf" || !selectedService?.id) return
    setNgwafWorkspaces([])
    setNgwafFetchError("")
    setNgwafDebugRaw("")
    setNgwafFetching(true)
    client
      .GET("/api/provision/ngwaf-workspaces", {
        params: { query: { service_id: selectedService.id, token: token || undefined } },
      })
      .then((r) => {
        if (r.error) {
          setNgwafFetchError(extractApiError(r.error) || "Could not load workspaces")
        } else {
          const data = r.data
          setNgwafWorkspaces(
            (data?.workspaces || []).flatMap((w) => (w.id && w.name ? [{ id: w.id, name: w.name }] : [])),
          )
          if (data?._debug_raw) setNgwafDebugRaw(data._debug_raw)
          if (data?.error_hint) setNgwafFetchError(data.error_hint)
        }
      })
      .catch((e: unknown) => setNgwafFetchError(extractApiError(e) || "Could not load workspaces"))
      .finally(() => setNgwafFetching(false))
  }, [step, selectedService?.id, token])

  const persistNgwafWorkspace = useEffectEvent(() => {
    if (isDone && step === "execute" && config.ngwaf_workspace_id && selectedService?.id) {
      client
        .PATCH("/api/provision/services/{service_id}/ngwaf-workspace", {
          params: { path: { service_id: selectedService.id } },
          headers: { Authorization: `Bearer ${token}` },
          body: { ngwaf_workspace_id: config.ngwaf_workspace_id },
        })
        .then((r) => {
          if (r?.error) console.error("[wizard] failed to persist ngwaf_workspace_id:", r.error)
        })
        .catch((e: unknown) => console.error("[wizard] failed to persist ngwaf_workspace_id:", e))
    }
  })
  useEffect(() => {
    persistNgwafWorkspace()
  }, [isDone])
}

export interface JoinCompletionEffectArgs {
  joinPhase: JoinPhase
  status: string
  config: ProvisionConfig
  setIsDeploying: (b: boolean) => void
  setJoinedServiceId: (id: string) => void
  setActiveServiceId: (id: string) => void
  services: { id: string; name: string; accessLevel?: string }[]
  setServices: (s: { id: string; name: string; accessLevel?: string }[]) => void
  queryClient: Pick<QueryClient, "invalidateQueries">
  setJoinPhase: (p: JoinPhase) => void
  reset: () => void
}

export function useJoinCompletionEffect(args: JoinCompletionEffectArgs) {
  const {
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
  } = args
  useEffect(() => {
    if (joinPhase !== "connecting") return
    if (status === "done") {
      setIsDeploying(false)
      registerServiceAndInvalidate(
        config.cdn_service_name,
        config.cdn_service_name,
        "read_only",
        services,
        setServices,
        setActiveServiceId,
        setJoinedServiceId,
        queryClient,
      )
      setJoinPhase("done")
    } else if (status === "error") {
      setIsDeploying(false)
      setJoinPhase("form")
      reset()
    }
  }, [joinPhase, status, config.cdn_service_name, setActiveServiceId, setServices, services, queryClient, reset])
}

export function useWizardState(open: boolean, onOpenChange: (open: boolean) => void): WizardState {
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId)
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const setServices = useServiceStore((s) => s.setServices)
  const services = useServiceStore((s) => s.services)
  const { timezone } = useTimezoneStore()
  const queryClient = useQueryClient()
  const [step, setStep] = useState<Step>("mode")
  const [mode, setMode] = useState<WizardMode>(null)
  const [token, setToken] = useState("")
  const [submittedToken, setSubmittedToken] = useState("")
  const [prevOpenForToken, setPrevOpenForToken] = useState(open)
  if (open !== prevOpenForToken) {
    setPrevOpenForToken(open)
    if (!open) setSubmittedToken("")
  }

  const [tokenInfo, setTokenInfo] = useState<TokenInfo | null>(null)
  const [search, setSearch] = useState("")
  const [selectedService, setSelectedService] = useState<ProvisionService | null>(null)
  const [isDeploying, setIsDeploying] = useState(false)
  const [fosStatus, setFosStatus] = useState<FosStatus>("idle")
  const [fosError, setFosError] = useState("")
  const [terraformFiles, setTerraformFiles] = useState<Record<string, string>>({})
  const [selectedTfFile, setSelectedTfFile] = useState<string>("logging_service.tf")
  const [isFetchingTerraform, setIsFetchingTerraform] = useState(false)
  const [selectedCdnService, setSelectedCdnService] = useState<ProvisionService | null>(null)
  const [configStatus, setConfigStatus] = useState<Schema<"ProvisionCheckConfigResponse"> | null>(null)
  const [isCheckingConfig, setIsCheckingConfig] = useState(false)
  const [config, setConfig] = useState<ProvisionConfig>(INITIAL_CONFIG)
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<{ id: string; name: string }[]>([])
  const [ngwafFetching, setNgwafFetching] = useState(false)
  const [ngwafFetchError, setNgwafFetchError] = useState("")
  const [ngwafDebugRaw, setNgwafDebugRaw] = useState("")
  const [lakeInfo, setLakeInfo] = useState<LakeInfo | null>(null)
  const [isAnalyzing, setIsAnalyzing] = useState(false)
  const [importMode, setImportMode] = useState<"all" | "range">("all")
  const [importRange, setImportRange] = useState<{ start: string; end: string }>({ start: "", end: "" })
  const [syncEnabled, setSyncEnabled] = useState(true)

  const capabilityService = React.useMemo(() => {
    if (!services.length) return null
    return !activeServiceId
      ? (services[0] ?? null)
      : (services.find((service) => service.id === activeServiceId) ?? null)
  }, [activeServiceId, services])
  const analystPathASupported = capabilityService?.analystPathASupported ?? true
  const analystPathAReason = analystPathASupported
    ? null
    : (capabilityService?.analystPathAReason ?? ANALYST_PATH_A_UNSUPPORTED_REASON)

  const [pendingDraft, setPendingDraft] = useState<WizardDraft | null>(() => (open ? loadDraft() : null))
  const draftIdRef = useRef<string | null>(null)
  const draftCreatedAtRef = useRef<string | null>(null)
  const { lines, status, isDone, error: sseError, start, stop, reset } = useSSE()

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
  })

  const [domainStatus, setDomainStatus] = useState<DomainStatus>("idle")
  const [domainMessage, setDomainMessage] = useState("")

  const {
    data: servicesData,
    error: servicesError,
    isLoading: isLoadingServices,
  } = useQuery({
    queryKey: ["provision-services", submittedToken],
    queryFn: async () => {
      if (!submittedToken) return []
      const { data, error } = await client.GET("/api/provision/services", {
        params: { query: { token: submittedToken } },
      })
      if (error) {
        const err = new Error(extractApiError(error)) as Error & { code?: string }
        err.code = (error as { detail?: { error?: string } })?.detail?.error
        throw err
      }
      return data ?? []
    },
    enabled: !!submittedToken,
    retry: false,
  })

  const [prevServicesData, setPrevServicesData] = useState(servicesData)
  if (servicesData !== prevServicesData) {
    setPrevServicesData(servicesData)
    if (servicesData && Array.isArray(servicesData) && step === "token") {
      setStep("service")
    }
  }

  const { data: catalog, isLoading: isLoadingCatalog } = useQuery({
    queryKey: ["services", "catalog"],
    queryFn: async () => unwrapClient(client.GET("/api/log-fields/catalog")),
    enabled: step === "fields",
  })

  const updateLogFields = (fn: (lf: ProvisionConfig["log_fields"]) => ProvisionConfig["log_fields"]) =>
    setConfig((prev) => ({ ...prev, log_fields: fn(prev.log_fields) }))
  const toggleGroup = (groupId: string, checked: boolean) =>
    updateLogFields((lf) => toggleLogFieldGroup(lf, catalog, groupId, checked))
  const toggleField = (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) =>
    updateLogFields((lf) => toggleLogFieldOverride(lf, fieldId, checked, defaultEnabledByGroup))
  const updateFieldLimit = (fieldId: string, limit?: number) =>
    updateLogFields((lf) => updateLogFieldLimit(lf, fieldId, limit))
  const isPresetActive = (groups: string[]) => isLogFieldPresetActive(config.log_fields, groups)
  const togglePreset = (presetGroups: string[]) =>
    updateLogFields((lf) => toggleLogFieldPreset(lf, catalog, presetGroups, isPresetActive))

  const estimatedBytes = React.useMemo(
    () => estimateLogFieldBytesPerLine(config.log_fields, catalog),
    [catalog, config.log_fields],
  )
  const handleTokenSubmit = async () => {
    if (token) setSubmittedToken(token)
  }

  const validateMutation = useMutation({
    mutationFn: validateMutationFn(token),
    onSuccess: buildValidateOnSuccess({ token, setTokenInfo, setConfig, setStep, mode }),
  })

  const handleServiceSelect = (service: ProvisionService) => {
    if (service.provisioned) return
    setSelectedService(service)
    validateMutation.mutate(service.id)
  }

  const handleCheckConfig = () =>
    runCheckConfig({ token, selectedService, selectedCdnService, config, setIsCheckingConfig, setConfigStatus })
  const handleCheckFos = (vals?: { bucket?: string; region?: string; access_key?: string; secret_key?: string }) =>
    runCheckFos({ vals, config, setFosStatus, setFosError })
  const checkDomain = (prefix: string) => runCheckDomain({ prefix, setDomainStatus, setDomainMessage })

  const [joinPhase, setJoinPhase] = useState<JoinPhase>("form")
  const [joinedServiceId, setJoinedServiceId] = useState("")
  const [syncIntervalMins, setSyncIntervalMins] = useState("2")
  const [icebergMetadataLocation, setIcebergMetadataLocation] = useState("")

  const resumeDraft = () => {
    if (!pendingDraft) return
    setMode(pendingDraft.mode)
    setConfig(mergePersistedConfig(pendingDraft.config))
    const toSvc = (id: string | null, name: string | null) =>
      id ? ({ id, name: name || id } as ProvisionService) : null
    setSelectedService(toSvc(pendingDraft.selectedServiceId, pendingDraft.selectedServiceName))
    setSelectedCdnService(toSvc(pendingDraft.selectedCdnServiceId, pendingDraft.selectedCdnServiceName))
    setImportMode(pendingDraft.importMode)
    setImportRange(pendingDraft.importRange)
    setSyncEnabled(pendingDraft.syncEnabled)
    setSyncIntervalMins(pendingDraft.syncIntervalMins)
    setIcebergMetadataLocation(pendingDraft.icebergMetadataLocation)
    let next: Step = pendingDraft.currentStep === "execute" ? "fields" : pendingDraft.currentStep
    if ((["service", "storage", "ngwaf", "fields", "execute"] as Step[]).includes(next)) {
      next = "token"
    }
    setStep(next)
    draftIdRef.current = pendingDraft.draftId
    draftCreatedAtRef.current = pendingDraft.createdAt
    setPendingDraft(null)
  }

  const discardDraft = () => {
    clearDraft()
    draftIdRef.current = null
    draftCreatedAtRef.current = null
    setPendingDraft(null)
  }

  const estimatedImportSize = React.useMemo(() => {
    if (!lakeInfo?.calendar) return 0
    let total = 0
    const { start, end } = importRange
    for (const [dateStr, stats] of Object.entries(lakeInfo.calendar)) {
      if (dateStr === "unknown") continue
      if (
        importMode === "range" &&
        ((start && dateStr < start.split("T")[0]) || (end && dateStr > end.split("T")[0]))
      ) {
        continue
      }
      total += stats.size_bytes
    }
    return total
  }, [lakeInfo, importMode, importRange])

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
    })
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
    })

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
  })

  const handleFinishJoin = () => {
    onOpenChange(false)
    window.location.reload()
  }
  const STEPS = getStepsForMode(mode, config, step)

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
  })

  const handleDeploy = () => runDeploy({ token, selectedService, config, setIsDeploying, start })
  const fetchTerraformPreview = () =>
    runFetchTerraformPreview({
      token,
      selectedService,
      config,
      setIsFetchingTerraform,
      setTerraformFiles,
      setSelectedTfFile,
    })
  const handleExportTerraform = () => runExportTerraform({ token, selectedService, config })
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
    })

  const [draftOpen, setDraftOpen] = useState(false)
  if (open !== draftOpen) {
    setDraftOpen(open)
    setPendingDraft(open ? loadDraft() : null)
  }

  useEffect(() => {
    if (!open || !mode || step === "mode" || pendingDraft) return
    const timer = setTimeout(() => {
      if (!draftIdRef.current) draftIdRef.current = makeDraftId()
      if (!draftCreatedAtRef.current) draftCreatedAtRef.current = new Date().toISOString()
      saveDraft({
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
      })
    }, 500)
    return () => clearTimeout(timer)
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
  ])

  useEffect(() => {
    if (!open) return
    return subscribeDraft((next) => {
      if (next && draftIdRef.current && next.draftId !== draftIdRef.current) {
        showToast("Wizard updated in another tab. Close and reopen to load latest.", "warn")
      }
    })
  }, [open])

  useEffect(() => {
    if (status === "done" || isDone) {
      clearDraft()
      draftIdRef.current = null
      draftCreatedAtRef.current = null
      queryClient.invalidateQueries({ queryKey: queryKeys.services() })
    }
  }, [status, isDone, queryClient])

  const filteredServices = Array.isArray(servicesData)
    ? servicesData.filter(
        (s) =>
          s.name.toLowerCase().includes(search.toLowerCase()) ||
          s.id.toLowerCase().includes(search.toLowerCase()),
      )
    : []

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
  }
}
