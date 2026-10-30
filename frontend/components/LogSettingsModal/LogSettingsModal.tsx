'use client'

import React, { useState, useMemo } from 'react'
import { useForm, useWatch, type PathValue } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useOpenGeneration, useSSE } from '@/hooks/useSSE'
import {
  client, unwrapClient, queryKeys, estimateLogFieldBytesPerLine,
  isLogFieldPresetActive, toggleLogFieldGroup, toggleLogFieldOverride,
  toggleLogFieldPreset, updateLogFieldLimit,
} from '@/lib/api'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Loader2, Settings2, AlertTriangle, ChevronRight, FileJson } from 'lucide-react'
import { cn, panelDialogContent, panelDialogFooter, formatBytes } from '@/lib/utils'
import { SSEDialogFooter, SSEExecutionSection } from '@/components/SSEModal'
import { useLogFieldsCatalog } from '@/hooks/useBootstrap'
import { CustomFieldsManager } from '@/components/CustomFields/CustomFieldDrawer'
import type { ServiceConfig, LogFieldsConfig } from '@/types/api'
import {
  CmcdConfigSection,
  CollapsibleGroup,
  StandardFieldsStep,
  ReviewStep,
} from './LogSettingsSteps'

export {
  CmcdConfigSection,
  CollapsibleGroup,
  StandardFieldsStep,
  ReviewStep,
}

const logSettingsSchema = z.object({
  config: z.custom<LogFieldsConfig>(), period: z.string(), sampleRate: z.number().min(1).max(100),
  edgeOnly: z.boolean(), customCondition: z.string(), cmcdEnabled: z.boolean(), cmcdMode: z.string(), cmcdVersion: z.number(),
})
type LogSettingsFormValues = z.infer<typeof logSettingsSchema>

const STEP_LABELS = ['1. Standard Fields', '2. Custom Fields', '3. Review'] as const

interface LogSettingsModalProps {
  service: ServiceConfig | null; open: boolean; onOpenChange: (open: boolean) => void
}

export function LogSettingsModal(props: LogSettingsModalProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <LogSettingsModalBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function LogSettingsModalBody({ service, open, onOpenChange }: LogSettingsModalProps) {
  const queryClient = useQueryClient()
  const [step, setStep] = useState<number>(1)

  const { control, setValue, getValues } = useForm<LogSettingsFormValues>({
    resolver: zodResolver(logSettingsSchema),
    defaultValues: {
      config: { groups: [], field_overrides: {} },
      period: '60', sampleRate: 100, edgeOnly: true, customCondition: '',
      cmcdEnabled: false, cmcdMode: 'query_string', cmcdVersion: 1,
    },
  })
  const { config, period, sampleRate, edgeOnly, customCondition, cmcdEnabled, cmcdMode, cmcdVersion } =
    useWatch({ control }) as LogSettingsFormValues
  const setConfig = (next: LogFieldsConfig | ((prev: LogFieldsConfig) => LogFieldsConfig)) =>
    setValue('config', typeof next === 'function' ? next(getValues('config')) : next, { shouldDirty: true })
  const bind = <K extends keyof LogSettingsFormValues>(key: K) => (v: LogSettingsFormValues[K]) =>
    setValue(key, v as PathValue<LogSettingsFormValues, K>, { shouldDirty: true })

  const { data: catalog } = useLogFieldsCatalog(service?.service_id)

  const { data: lfResponse, isLoading: isLoadingFields, isError: isFieldsError, error: fieldsError } = useQuery({
    queryKey: ['services', service?.service_id, 'fields'],
    queryFn: async () => unwrapClient(client.GET('/api/services/{service_id}/log-fields', { params: { path: { service_id: service!.service_id } } })),
    enabled: !!service && open, retry: false,
  })

  const { data: loggingSettings, isLoading: isLoadingSettings, isError: isSettingsError, error: settingsError } = useQuery({
    queryKey: ['services', service?.service_id, 'logging-settings'],
    queryFn: async () => unwrapClient(client.GET('/api/services/{service_id}/logging-settings', { params: { path: { service_id: service!.service_id } } })),
    enabled: !!service && open, retry: false,
  })

  const { lines, status, error, start, stop } = useSSE()

  const fieldsMutation = useMutation({
    mutationFn: async (cfg: LogFieldsConfig) =>
      unwrapClient(client.POST('/api/services/{service_id}/log-fields', { params: { path: { service_id: service!.service_id } }, body: { log_fields: cfg } })),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.services() })
      const encodedCond = encodeURIComponent(customCondition)
      start(`/api/services/${service!.service_id}/logging-settings/update?update_format=true&period=${period}&sample_rate=${sampleRate}&edge_only=${edgeOnly}&custom_condition=${encodedCond}&cmcd_enabled=${cmcdEnabled}&cmcd_mode=${cmcdMode}&cmcd_version=${cmcdVersion}`, {})
    },
  })

  const syncServerData = React.useEffectEvent((lf: typeof lfResponse, ls: typeof loggingSettings, isOpen: boolean) => {
    if (!isOpen || status !== 'idle' || fieldsMutation.isPending || fieldsMutation.isSuccess) return
    if (lf?.log_fields) {
      setValue('config', {
        groups: lf.log_fields.groups || [],
        field_overrides: lf.log_fields.field_overrides || {},
        field_limits: lf.log_fields.field_limits || {},
      })
    }
    if (ls) {
      setValue('period', String(ls.period || 60))
      setValue('sampleRate', ls.sample_rate ?? 100)
      setValue('edgeOnly', ls.edge_only ?? true)
      setValue('customCondition', ls.custom_condition || '')
      setValue('cmcdEnabled', ls.cmcd?.enabled ?? false)
      setValue('cmcdMode', ls.cmcd?.mode ?? 'query_string')
      setValue('cmcdVersion', ls.cmcd?.version ?? 1)
    }
  })
  React.useEffect(() => { syncServerData(lfResponse, loggingSettings, open) }, [lfResponse, loggingSettings, open])

  const toggleGroup = (groupId: string, checked: boolean) => setConfig((prev) => toggleLogFieldGroup(prev, catalog, groupId, checked))
  const toggleField = (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => setConfig((prev) => toggleLogFieldOverride(prev, fieldId, checked, defaultEnabledByGroup))
  const updateFieldLimit = (fieldId: string, limit?: number) => setConfig((prev) => updateLogFieldLimit(prev, fieldId, limit))
  const isPresetActive = (groups: string[]) => isLogFieldPresetActive(config, groups)
  const togglePreset = (presetGroups: string[]) => setConfig((prev) => toggleLogFieldPreset(prev, catalog, presetGroups, isPresetActive))
  const handleSave = () => fieldsMutation.mutate(config)
  const handleOpenChange = (newOpen: boolean) => {
    if (fieldsMutation.isPending || status === 'streaming') return
    onOpenChange(newOpen)
  }

  const estimatedBytes = useMemo(() => estimateLogFieldBytesPerLine(config, catalog), [catalog, config])
  if (!service) return null

  const isPending = fieldsMutation.isPending || status === 'streaming'
  const isSuccess = fieldsMutation.isPending || fieldsMutation.isSuccess || status === 'done' || status === 'error' || status === 'streaming'
  const isLoading = isLoadingFields || isLoadingSettings
  const hasInitError = isFieldsError || isSettingsError
  const initErr = fieldsError || settingsError

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className={cn('sm:max-w-4xl', panelDialogContent)} showCloseButton={status !== 'streaming'}>
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
              {STEP_LABELS.map((label, idx) => (
                <React.Fragment key={label}>
                  {idx > 0 && <ChevronRight className="h-4 w-4 text-muted-foreground" />}
                  <div className={cn('px-3 py-1 rounded-full text-xs font-bold', step === idx + 1 ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground')}>
                    {label}
                  </div>
                </React.Fragment>
              ))}
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
                    {initErr instanceof Error ? initErr.message : 'The backend returned an error. Check the service configuration and try again.'}
                  </p>
                  <Button variant="outline" size="sm" onClick={() => onOpenChange(false)} className="mt-2">Close</Button>
                </div>
              ) : (
                <div className="w-full h-full">
                  {step === 1 && (
                    <StandardFieldsStep
                      catalog={catalog} config={config} setConfig={setConfig}
                      period={period} setPeriod={bind('period')}
                      sampleRate={sampleRate} setSampleRate={bind('sampleRate')}
                      edgeOnly={edgeOnly} setEdgeOnly={bind('edgeOnly')}
                      customCondition={customCondition} setCustomCondition={bind('customCondition')}
                      cmcdEnabled={cmcdEnabled} setCmcdEnabled={bind('cmcdEnabled')}
                      cmcdMode={cmcdMode} setCmcdMode={bind('cmcdMode')}
                      cmcdVersion={cmcdVersion} setCmcdVersion={bind('cmcdVersion')}
                      toggleGroup={toggleGroup} toggleField={toggleField}
                      updateFieldLimit={updateFieldLimit} togglePreset={togglePreset} isPresetActive={isPresetActive}
                    />
                  )}
                  {step === 2 && (
                    <div className="m-0 border-none p-0 outline-none">
                      <CustomFieldsManager serviceId={service.service_id} />
                    </div>
                  )}
                  {step === 3 && (
                    <ReviewStep
                      service={service} catalog={catalog} config={config}
                      period={period} sampleRate={sampleRate} edgeOnly={edgeOnly}
                      customCondition={customCondition} estimatedBytes={estimatedBytes}
                      cmcdEnabled={cmcdEnabled} cmcdMode={cmcdMode} cmcdVersion={cmcdVersion}
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
                  {step > 1 && <Button variant="outline" onClick={() => setStep(step - 1)} className="h-10 px-6">Back</Button>}
                  {step < 3 ? (
                    <Button onClick={() => setStep(step + 1)} className="h-10 px-6 font-bold">Next Step</Button>
                  ) : service.storage_mode === 'terraform' ? (
                    <Button
                      onClick={() => alert("Update your Terraform configuration with the new log format and snippets generated by the 'Connect Terraform' tool.")}
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
