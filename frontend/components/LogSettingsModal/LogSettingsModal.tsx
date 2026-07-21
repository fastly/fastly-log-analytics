'use client'

import React, { useState, useEffect, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { components } from '@/types/api.generated'
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
import { SSEProgressView } from '@/components/SSEModal'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import {
  panelDialogContent,
  panelDialogFooter,
} from '@/lib/panel-dialog'
import { CollapsibleGroup, StandardFieldsStep } from './FieldGroups'
import { CustomFieldsStep } from './CustomFields'
import { ReviewStep } from './Preview'

// Re-export CollapsibleGroup so existing imports from this module keep working
// (e.g. ProvisionWizard imports it from this path).
export { CollapsibleGroup }

type ServiceConfig = components['schemas']['ServiceConfig']
type LogFieldsConfig = components['schemas']['LogFieldsConfig']

interface LogSettingsModalProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function LogSettingsModal({ service, open, onOpenChange }: LogSettingsModalProps) {
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
    queryFn: async () => {
      const { data } = await client.GET('/api/services/{service_id}/log-fields', {
        params: { path: { service_id: service!.service_id } },
      })
      return data
    },
    enabled: !!service && open,
    retry: false,
  })

  const { data: loggingSettings, isLoading: isLoadingSettings, isError: isSettingsError, error: settingsError } = useQuery({
    queryKey: ['services', service?.service_id, 'logging-settings'],
    queryFn: async () => {
      const { data } = await client.GET('/api/services/{service_id}/logging-settings', {
        params: { path: { service_id: service!.service_id } },
      })
      return data
    },
    enabled: !!service && open,
    retry: false,
  })

  const { lines, status, isDone, error, start, stop, reset } = useSSE()

  const fieldsMutation = useMutation({
    mutationFn: async (config: LogFieldsConfig) => {
      const { data } = await client.POST('/api/services/{service_id}/log-fields', {
        params: { path: { service_id: service!.service_id } },
        body: { log_fields: config },
      })
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['services'] })
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

  useEffect(() => {
    if (open && status === 'idle' && !fieldsMutation.isPending && !fieldsMutation.isSuccess) {
      if (lfResponse?.log_fields) {
        setConfig({
          groups: lfResponse.log_fields.groups || [],
          field_overrides: lfResponse.log_fields.field_overrides || {}
        })
      }
      if (loggingSettings) {
        setPeriod(String(loggingSettings.period || 60))
        setSampleRate(loggingSettings.sample_rate ?? 100)
        setEdgeOnly(loggingSettings.edge_only ?? true)
        setCustomCondition(loggingSettings.custom_condition || '')
        setCmcdEnabled(loggingSettings.cmcd_enabled ?? false)
        setCmcdMode(loggingSettings.cmcd_mode ?? 'query_string')
        setCmcdVersion(loggingSettings.cmcd_version ?? 1)
      }
    }
  }, [lfResponse, loggingSettings, open]) // Using fieldsMutation.reset directly inside breaks exhaustive-deps since it's an object, we suppress or omit it, but wait! The issue says `react-hooks/set-state-in-effect`. That's usually fine, just a warning.

  // Reset state only when opening the modal freshly
  useEffect(() => {
    if (open) {
      fieldsMutation.reset()
      reset()
    }
  }, [open])

  const toggleGroup = (groupId: string, checked: boolean) => {
    setConfig(prev => {
      const nextGroups = new Set(prev.groups)
      if (checked) {
        nextGroups.add(groupId)
        // Auto-enable required dependencies
        let changed = true
        while (changed) {
           changed = false;
           (catalog?.groups || []).forEach((g: { id: string; requires?: string }) => {
             if (nextGroups.has(g.id) && g.requires && !nextGroups.has(g.requires)) {
               nextGroups.add(g.requires)
               changed = true
             }
           })
        }
      } else {
        nextGroups.delete(groupId)
      }
      return { ...prev, groups: Array.from(nextGroups) }
    })
  }

  const toggleField = (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => {
    setConfig(prev => {
      const overrides = { ...prev.field_overrides }
      if (checked === defaultEnabledByGroup) {
        delete overrides[fieldId]
      } else {
        overrides[fieldId] = checked
      }
      return { ...prev, field_overrides: overrides }
    })
  }

  const updateFieldLimit = (fieldId: string, limit?: number) => {
    setConfig(prev => {
      const field_limits = { ...(prev.field_limits || {}) }
      if (limit === undefined) {
        delete field_limits[fieldId]
      } else {
        field_limits[fieldId] = limit
      }
      return { ...prev, field_limits }
    })
  }

  const togglePreset = (presetGroups: string[]) => {
    setConfig(prev => {
      const currentGroups = new Set(prev.groups || [])
      const allActive = presetGroups.every(g => currentGroups.has(g))

      const nextGroups = new Set(prev.groups || [])

      if (allActive) {
        const otherActivePresetsGroups = new Set<string>()
        if (catalog?.presets) {
          Object.entries(catalog.presets).forEach(([key, preset]: [string, any]) => {
             if (preset.groups.length !== presetGroups.length || !preset.groups.every((g: string) => presetGroups.includes(g))) {
               if (isPresetActive(preset.groups)) {
                 preset.groups.forEach((g: string) => otherActivePresetsGroups.add(g))
               }
             }
          })
        }

        presetGroups.forEach(g => {
          if (!otherActivePresetsGroups.has(g)) {
            nextGroups.delete(g);
            (catalog?.groups || []).forEach((cg: any) => {
               if (cg.requires === g && !otherActivePresetsGroups.has(cg.id)) {
                 nextGroups.delete(cg.id)
               }
            })
          }
        })
      } else {
        presetGroups.forEach(g => nextGroups.add(g))

        let changed = true
        while (changed) {
           changed = false;
           (catalog?.groups || []).forEach((cg: any) => {
             if (nextGroups.has(cg.id) && cg.requires && !nextGroups.has(cg.requires)) {
               nextGroups.add(cg.requires)
               changed = true
             }
           })
        }
      }

      return { ...prev, groups: Array.from(nextGroups) }
    })
  }

  const isPresetActive = (groups: string[]) => {
    if (!groups.length) return false
    const currentGroups = new Set(config.groups || [])
    return groups.every(g => currentGroups.has(g))
  }

  const handleSave = () => {
    fieldsMutation.mutate(config)
  }

  const handleOpenChange = (newOpen: boolean) => {
    if (fieldsMutation.isPending || status === 'streaming') return
    onOpenChange(newOpen)
  }

  // Calculate estimated bytes based on current selections
  const estimatedBytes = useMemo(() => {
    if (!catalog?.fields) return 0
    let total = 0
    const enabledGroups = new Set(config.groups)
    const overrides = config.field_overrides || {}

    for (const field of catalog.fields) {
      const inGroup = field.group === null || enabledGroups.has(field.group)
      const override = overrides[field.id]
      if (override === true) { total += (field.typical_bytes || 0); continue; }
      if (override === false) continue;
      if (inGroup) total += (field.typical_bytes || 0);
    }
    return total
  }, [catalog, config])

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
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
               <div className="text-center space-y-2">
                  <h3 className="text-lg font-semibold tracking-tight">Updating Log Configuration</h3>
                  <p className="text-sm text-muted-foreground">Deploying new configuration to Fastly network...</p>
               </div>

               <SSEProgressView
                 lines={fieldsMutation.isPending ? [{ type: 'info', message: 'Saving configuration locally...' }] : lines}
                 status={fieldsMutation.isPending ? 'streaming' : status}
                 error={error}
                 className="h-[400px]"
                 progressLabel="Step"
                 doneMessage="Log configuration updated successfully! You may now close this window."
               />
            </div>
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
                    <CustomFieldsStep serviceId={service.service_id} />
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
                {status !== 'streaming' && (
                  <Button variant="outline" onClick={() => onOpenChange(false)} className="h-10 px-6">Close</Button>
                )}
                {status === 'streaming' && (
                  <Button variant="outline" onClick={stop} className="h-10 px-6">Stop</Button>
                )}
              </div>
            </div>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
