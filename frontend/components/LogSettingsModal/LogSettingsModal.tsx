'use client'

import React, { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useOpenGeneration } from '@/hooks/useDerivedState'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Loader2, Settings2, AlertTriangle, ChevronRight, FileJson } from 'lucide-react'
import { cn, panelDialogContent, panelDialogFooter } from '@/lib/utils';
import { formatBytes } from '@/lib/format'
import { useSSE } from '@/hooks/useSSE'
import { SSEDialogFooter, SSEExecutionSection } from '@/components/SSEModal'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { CollapsibleGroup, StandardFieldsStep } from './FieldGroups'
import { CustomFieldsManager } from '@/components/CustomFields/CustomFieldsManager'
import {
  estimateLogFieldBytesPerLine,
  evaluateLogFieldPresetSummary,
  isLogFieldPresetActive,
  resolveEnabledGroupsWithDeps,
  toggleLogFieldGroup,
  toggleLogFieldOverride,
  toggleLogFieldPreset,
  updateLogFieldLimit,
} from '@/lib/log-field-config-ops'
import type { ServiceConfig, LogFieldsConfig, components } from '@/types/api'

interface ReviewStepProps {
  service: ServiceConfig
  catalog: components['schemas']['LogFieldsCatalogResponse'] | undefined
  config: LogFieldsConfig
  period: string
  sampleRate: number
  edgeOnly: boolean
  customCondition: string
  estimatedBytes: number
  cmcdEnabled?: boolean
  cmcdMode?: string
  cmcdVersion?: number
}

export function ReviewStep({
  service,
  catalog,
  config,
  period,
  sampleRate,
  edgeOnly,
  customCondition,
  estimatedBytes,
  cmcdEnabled,
  cmcdMode,
  cmcdVersion,
}: ReviewStepProps) {
  return (
    <div className="space-y-6">
      <h3 className="text-lg font-semibold border-b pb-2">Review Log Configuration Changes</h3>
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div className="p-4 border rounded-lg bg-muted/20 space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">General Settings</p>
            <p className="text-sm font-medium">Log Period: {period} seconds</p>
            <p className="text-sm font-medium">Sample Rate: {sampleRate}%</p>
            <p className="text-sm font-medium">Edge Only: {edgeOnly ? "Yes" : "No"}</p>
            {customCondition && (
              <p className="text-sm font-medium truncate" title={customCondition}>
                Custom Condition: <code className="text-[10px] bg-background px-1 rounded border">{customCondition}</code>
              </p>
            )}
            <p className="text-sm font-medium">
              CMCD: {cmcdEnabled
                ? `Enabled (v${cmcdVersion ?? 1}, ${cmcdMode === 'headers' ? 'Headers' : 'Query String'})`
                : 'Disabled'}
            </p>
          </div>

          <div className="p-4 border rounded-lg bg-muted/20 space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider flex items-center justify-between">
              Log Configuration
              <span className="font-mono text-[10px] bg-background px-1.5 py-0.5 rounded border normal-case font-medium">
                ~{formatBytes(estimatedBytes)} / line
              </span>
            </p>
            <div className="flex flex-wrap gap-1.5 pt-2">
              {(() => {
                const { enabledGroupsSet, bestPresetName, disabledCount } =
                  evaluateLogFieldPresetSummary(config, catalog);

                if (bestPresetName) {
                  return (
                    <>
                      <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                        {bestPresetName} Preset
                      </div>
                      {disabledCount > 0 && (
                        <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                          +{disabledCount} disabled
                        </div>
                      )}
                    </>
                  );
                }

                return (
                  <>
                    <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                      Custom Configuration
                    </div>
                    {(catalog?.groups || []).map((g) => {
                      const isEnabled = g.locked || (g.id != null && enabledGroupsSet.has(g.id));
                      if (!isEnabled) return null;
                      return (
                        <div key={g.id || "core"} className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-primary/10 text-primary border border-primary/20">
                          {g.label}
                        </div>
                      );
                    })}
                    {disabledCount > 0 && (
                      <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                        +{disabledCount} disabled
                      </div>
                    )}
                  </>
                );
              })()}
            </div>
          </div>
        </div>

        {/* Custom Fields Summary */}
        {(() => {
           const customFields = (catalog?.fields || []).filter((f) => f.is_custom);
           if (customFields.length === 0) return null;
           return (
             <div className="p-4 border rounded-lg bg-muted/20 space-y-3">
               <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Custom Fields ({customFields.length})</p>
               <div className="flex flex-wrap gap-2">
                 {customFields.map((cf) => (
                    <div key={cf.id} className="flex items-center gap-1.5 px-2.5 py-1 rounded bg-background border text-xs shadow-sm">
                       <span className="font-medium">{cf.label}</span>
                       <span className="text-[10px] text-muted-foreground font-mono">({cf.id})</span>
                    </div>
                 ))}
               </div>
             </div>
           );
        })()}

        {/* Insights Section */}
        <div className="p-4 border rounded-lg bg-muted/20 space-y-3">
          <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider flex justify-between items-center">
            Automated Insights
            <span className="text-[10px] normal-case font-medium">Derived from logs</span>
          </p>
          <div className="grid grid-cols-2 gap-3">
            {catalog?.insights?.map((insight) => {
              const enabledGroups = resolveEnabledGroupsWithDeps(config.groups, catalog);
              const isEnabled = insight.required_groups.every((rg) => enabledGroups.has(rg));
              return (
                <div key={insight.id} className={cn("flex items-start gap-3 border rounded-lg p-2.5 bg-background shadow-sm transition-all", isEnabled ? "border-primary/20 bg-primary/5" : "opacity-50 grayscale bg-muted/50")}>
                  <div className={cn("w-2 h-2 mt-1.5 rounded-full shrink-0", isEnabled ? "bg-primary" : "bg-muted-foreground")} />
                  <div className="space-y-1 overflow-hidden">
                    <h4 className="text-xs font-semibold tracking-tight truncate" title={insight.title}>{insight.title}</h4>
                    <p className="text-[10px] text-muted-foreground line-clamp-2 leading-snug">{insight.description}</p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-900/50 p-4 rounded-lg flex gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-600 shrink-0 mt-0.5" />
          <div className="text-sm text-amber-800 dark:text-amber-300 leading-normal">
            {service.storage_mode === "terraform" ? (
              <>
                <strong>Note:</strong> This service is managed via Terraform. Direct deployment is disabled. Please view and export the updated Terraform code to apply these changes.
              </>
            ) : (
              <>
                <strong>Important:</strong> Deploying this configuration will clone your active Fastly service version, update the logging endpoints, and activate the new version. Data collected before this change will have <code>null</code> for any newly added fields.
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

interface LogSettingsModalProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function LogSettingsModal(props: LogSettingsModalProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <LogSettingsModalBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function LogSettingsModalBody({ service, open, onOpenChange }: LogSettingsModalProps) {
  const queryClient = useQueryClient()

  const [config, setConfig] = useState<LogFieldsConfig>({ groups: [], field_overrides: {} })
  const [period, setPeriod] = useState<string>('60')
  const [sampleRate, setSampleRate] = useState<number>(100)
  const [edgeOnly, setEdgeOnly] = useState<boolean>(true)
  const [customCondition, setCustomCondition] = useState<string>('')
  const [cmcdEnabled, setCmcdEnabled] = useState<boolean>(false)
  const [cmcdMode, setCmcdMode] = useState<string>('query_string')
  const [cmcdVersion, setCmcdVersion] = useState<number>(1)
  const [step, setStep] = useState<number>(1)

  const { data: catalog } = useLogFieldsCatalog(service?.service_id)

  const { data: lfResponse, isLoading: isLoadingFields, isError: isFieldsError, error: fieldsError } = useQuery({
    queryKey: ['services', service?.service_id, 'fields'],
    queryFn: async () =>
      unwrapClient(
        client.GET('/api/services/{service_id}/log-fields', {
          params: { path: { service_id: service!.service_id } },
        }),
      ),
    enabled: !!service && open,
    retry: false,
  })

  const { data: loggingSettings, isLoading: isLoadingSettings, isError: isSettingsError, error: settingsError } = useQuery({
    queryKey: ['services', service?.service_id, 'logging-settings'],
    queryFn: async () =>
      unwrapClient(
        client.GET('/api/services/{service_id}/logging-settings', {
          params: { path: { service_id: service!.service_id } },
        }),
      ),
    enabled: !!service && open,
    retry: false,
  })

  const { lines, status, error, start, stop } = useSSE()

  const fieldsMutation = useMutation({
    mutationFn: async (config: LogFieldsConfig) =>
      unwrapClient(
        client.POST('/api/services/{service_id}/log-fields', {
          params: { path: { service_id: service!.service_id } },
          body: { log_fields: config },
        }),
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.services() })
      // Trigger Fastly deployment with all settings.
      // Security: endpoint was previously a state-changing GET (CSRF
      // surface). Now POST — useSSE's `start(url, body)` overload sends a
      // POST and streams the response body via ReadableStream, preserving
      // the SSE-style UX without the CSRF risk. Query params stay where
      // they are because the backend reads them via Query(...).
      const encodedCond = encodeURIComponent(customCondition)
      const endpoint = `/api/services/${service!.service_id}/logging-settings/update?update_format=true&period=${period}&sample_rate=${sampleRate}&edge_only=${edgeOnly}&custom_condition=${encodedCond}&cmcd_enabled=${cmcdEnabled}&cmcd_mode=${cmcdMode}&cmcd_version=${cmcdVersion}`
      start(endpoint, {})
    }
  })

  // Load the saved settings into the form whenever the server data or the open state changes
  // (adjusted during render; the conditions are read as of that change, as the effect did).
  const [syncedFrom, setSyncedFrom] = useState<{
    lfResponse: typeof lfResponse
    loggingSettings: typeof loggingSettings
    open: boolean
  }>({ lfResponse: undefined, loggingSettings: undefined, open: false })
  if (syncedFrom.lfResponse !== lfResponse || syncedFrom.loggingSettings !== loggingSettings || syncedFrom.open !== open) {
    setSyncedFrom({ lfResponse, loggingSettings, open })
    if (open && status === 'idle' && !fieldsMutation.isPending && !fieldsMutation.isSuccess) {
      if (lfResponse?.log_fields) {
        setConfig({
          groups: lfResponse.log_fields.groups || [],
          field_overrides: lfResponse.log_fields.field_overrides || {},
          field_limits: lfResponse.log_fields.field_limits || {},
        })
      }
      if (loggingSettings) {
        setPeriod(String(loggingSettings.period || 60))
        setSampleRate(loggingSettings.sample_rate ?? 100)
        setEdgeOnly(loggingSettings.edge_only ?? true)
        setCustomCondition(loggingSettings.custom_condition || '')
        setCmcdEnabled(loggingSettings.cmcd?.enabled ?? false)
        setCmcdMode(loggingSettings.cmcd?.mode ?? 'query_string')
        setCmcdVersion(loggingSettings.cmcd?.version ?? 1)
      }
    }
  }

  const toggleGroup = (groupId: string, checked: boolean) => {
    setConfig(prev => toggleLogFieldGroup(prev, catalog, groupId, checked))
  }

  const toggleField = (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => {
    setConfig(prev => toggleLogFieldOverride(prev, fieldId, checked, defaultEnabledByGroup))
  }

  const updateFieldLimit = (fieldId: string, limit?: number) => {
    setConfig(prev => updateLogFieldLimit(prev, fieldId, limit))
  }

  const isPresetActive = (groups: string[]) => isLogFieldPresetActive(config, groups)

  const togglePreset = (presetGroups: string[]) => {
    setConfig(prev => toggleLogFieldPreset(prev, catalog, presetGroups, isPresetActive))
  }

  const handleSave = () => {
    fieldsMutation.mutate(config)
  }

  const handleOpenChange = (newOpen: boolean) => {
    if (fieldsMutation.isPending || status === 'streaming') return
    onOpenChange(newOpen)
  }

  // Calculate estimated bytes based on current selections
  const estimatedBytes = useMemo(
    () => estimateLogFieldBytesPerLine(config, catalog),
    [catalog, config],
  )

  if (!service) return null

  const isPending = fieldsMutation.isPending || status === 'streaming'
  const isSuccess = fieldsMutation.isPending || fieldsMutation.isSuccess || status === 'done' || status === 'error' || status === 'streaming'
  const isLoading = isLoadingFields || isLoadingSettings
  const hasInitError = isFieldsError || isSettingsError

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className={cn("sm:max-w-4xl", panelDialogContent)} showCloseButton={status !== 'streaming'}>
        <DialogHeader className="px-6 pt-6 pb-2 border-b bg-muted/10">
          <div className="flex items-center justify-between">
            <DialogTitle className="flex items-center gap-2">
              <Settings2 className="h-5 w-5 text-primary" />
              Configure Log Settings
            </DialogTitle>
          </div>
          <div className="flex items-center justify-between mt-1 mb-2">
            <div className="text-sm text-muted-foreground">
              Service: <span className="font-medium text-foreground">{service.name}</span>
            </div>
            {!isLoading && !isSuccess && step === 1 && (
              <div className="text-xs font-mono text-muted-foreground bg-muted/50 px-2 py-0.5 rounded">
                Est. ~{formatBytes(estimatedBytes)} / line
              </div>
            )}
          </div>
          <div className="w-full flex items-center justify-between px-6 pb-4">
            <div className="flex items-center gap-2">
              <div className={cn("px-3 py-1 rounded-full text-xs font-bold", step === 1 ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground")}>1. Standard Fields</div>
              <ChevronRight className="h-4 w-4 text-muted-foreground" />
              <div className={cn("px-3 py-1 rounded-full text-xs font-bold", step === 2 ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground")}>2. Custom Fields</div>
              <ChevronRight className="h-4 w-4 text-muted-foreground" />
              <div className={cn("px-3 py-1 rounded-full text-xs font-bold", step === 3 ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground")}>3. Review</div>
            </div>
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {isSuccess ? (
            <SSEExecutionSection
              title="Updating Log Configuration"
              subtitle="Deploying new configuration to Fastly network..."
              lines={fieldsMutation.isPending ? [{ type: 'info', message: 'Saving configuration locally...' }] : lines}
              status={fieldsMutation.isPending ? 'streaming' : status}
              error={error}
              heightClassName="h-[400px]"
              progressLabel="Step"
              doneMessage="Log configuration updated successfully! You may now close this window."
            />
          ) : (
            <div className="p-6">
              {isLoading ? (
                <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
                  <Loader2 className="h-8 w-8 animate-spin mb-4" />
                  <p className="text-sm">Loading active settings...</p>
                </div>
              ) : hasInitError ? (
                <div className="flex flex-col items-center justify-center py-12 text-muted-foreground gap-3">
                  <AlertTriangle className="h-8 w-8 text-destructive" />
                  <p className="text-sm font-medium text-foreground">Failed to load log settings</p>
                  <p className="text-xs text-center max-w-md">
                    {(fieldsError || settingsError) instanceof Error
                      ? (fieldsError || settingsError)!.message
                      : 'The backend returned an error. Check the service configuration and try again.'}
                  </p>
                  <Button variant="outline" size="sm" onClick={() => onOpenChange(false)} className="mt-2">Close</Button>
                </div>
              ) : (
                <div className="w-full h-full">
                  {step === 1 && (
                    <StandardFieldsStep
                      catalog={catalog}
                      config={config}
                      setConfig={setConfig}
                      period={period}
                      setPeriod={setPeriod}
                      sampleRate={sampleRate}
                      setSampleRate={setSampleRate}
                      edgeOnly={edgeOnly}
                      setEdgeOnly={setEdgeOnly}
                      customCondition={customCondition}
                      setCustomCondition={setCustomCondition}
                      cmcdEnabled={cmcdEnabled}
                      setCmcdEnabled={setCmcdEnabled}
                      cmcdMode={cmcdMode}
                      setCmcdMode={setCmcdMode}
                      cmcdVersion={cmcdVersion}
                      setCmcdVersion={setCmcdVersion}
                      toggleGroup={toggleGroup}
                      toggleField={toggleField}
                      updateFieldLimit={updateFieldLimit}
                      togglePreset={togglePreset}
                      isPresetActive={isPresetActive}
                    />
                  )}

                  {step === 2 && (
                    <div className="m-0 border-none p-0 outline-none">
                      <CustomFieldsManager serviceId={service.service_id} />
                    </div>
                  )}

                  {step === 3 && (
                    <ReviewStep
                      service={service}
                      catalog={catalog}
                      config={config}
                      period={period}
                      sampleRate={sampleRate}
                      edgeOnly={edgeOnly}
                      customCondition={customCondition}
                      estimatedBytes={estimatedBytes}
                      cmcdEnabled={cmcdEnabled}
                      cmcdMode={cmcdMode}
                      cmcdVersion={cmcdVersion}
                    />
                  )}
                </div>
              )}
            </div>
                    )}
                    </div>

                    <DialogFooter className={panelDialogFooter}>
          {!isSuccess ? (
            <div className="flex flex-col w-full">
              {fieldsMutation.isError && (
                <div className="mb-4 text-xs text-destructive flex items-center gap-1.5">
                  <AlertTriangle className="h-4 w-4" />
                  {fieldsMutation.error instanceof Error ? fieldsMutation.error.message : 'Failed to update log fields. Please try again.'}
                </div>
              )}
              <div className="flex items-center justify-between gap-2 w-full">
                <Button variant="outline" onClick={() => onOpenChange(false)} className="h-10 px-6">Cancel</Button>
                <div className="flex items-center gap-2">
                  {step > 1 && (
                    <Button variant="outline" onClick={() => setStep(step - 1)} className="h-10 px-6">Back</Button>
                  )}
                  {step < 3 ? (
                    <Button onClick={() => setStep(step + 1)} className="h-10 px-6 font-bold">Next Step</Button>
                  ) : service.storage_mode === "terraform" ? (
                    <Button
                      onClick={() => {
                        alert("Update your Terraform configuration with the new log format and snippets generated by the 'Connect Terraform' tool.");
                      }}
                      className="h-10 px-6 font-bold"
                    >
                      <FileJson className="mr-2 h-4 w-4" />
                      View Terraform
                    </Button>
                  ) : (
                    <Button onClick={handleSave} disabled={isPending} className="h-10 px-6 font-bold">
                      {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                      Deploy to Fastly
                    </Button>
                  )}
                </div>
              </div>
            </div>
          ) : (
            <div className="flex flex-col items-end w-full">
              <div className="flex gap-2">
                <SSEDialogFooter
                  status={fieldsMutation.isPending ? 'streaming' : status}
                  onClose={() => onOpenChange(false)}
                  onStop={stop}
                  idleOrErrorLabel="Close"
                />
              </div>
            </div>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
