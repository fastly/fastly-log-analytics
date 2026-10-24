'use client'

import React, { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
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
import { cn } from '@/lib/utils';
import { formatBytes } from '@/lib/format'
import { useSSE } from '@/hooks/useSSE'
import { SSEDialogFooter, SSEExecutionSection } from '@/components/SSEModal'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import {
  panelDialogContent,
  panelDialogFooter,
} from '@/lib/panel-dialog'
import { CollapsibleGroup, StandardFieldsStep } from './FieldGroups'
import { CustomFieldsManager } from '@/components/CustomFields/CustomFieldsManager'
import { ReviewStep } from './Preview'
import {
  estimateLogFieldBytesPerLine,
  isLogFieldPresetActive,
  toggleLogFieldGroup,
  toggleLogFieldOverride,
  toggleLogFieldPreset,
  updateLogFieldLimit,
} from '@/lib/log-field-config-ops'
import type { ServiceConfig, LogFieldsConfig } from '@/types/api'

// Re-export CollapsibleGroup so existing imports from this module keep working
// (e.g. ProvisionWizard imports it from this path).
export { CollapsibleGroup }


interface LogSettingsModalProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function LogSettingsModal(props: LogSettingsModalProps) {
  const { service, open } = props
  const [generation, setGeneration] = useState(0)
  const [wasOpen, setWasOpen] = useState(open)
  if (open !== wasOpen) {
    setWasOpen(open)
    if (open) setGeneration((g) => g + 1)
  }
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
