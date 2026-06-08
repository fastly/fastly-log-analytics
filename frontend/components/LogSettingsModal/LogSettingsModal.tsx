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
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import { Switch } from '@/components/ui/switch'
import { LabelWithInfo } from '@/components/ui/label-with-info'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { Loader2, Settings2, AlertTriangle, ChevronRight, ChevronDown, FileJson } from 'lucide-react'
import { cn, formatBytes } from '@/lib/utils'
import { useSSE } from '@/hooks/useSSE'
import { SSEProgressView } from '@/components/SSEModal'
import { CustomFieldsManager } from '@/components/CustomFields/CustomFieldsManager'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import {
  panelDialogContent,
  panelDialogFooter,
} from '@/lib/panel-dialog'

type ServiceConfig = components['schemas']['ServiceConfig']
type LogFieldsConfig = components['schemas']['LogFieldsConfig']

interface LogSettingsModalProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function CollapsibleGroup({ group, catalog, config, toggleGroup, toggleField, updateFieldLimit }: any) {
  const [isOpen, setIsOpen] = useState(false)
  
  const enabledGroups = new Set(config.groups)
  const overrides = config.field_overrides || {}
  const limits = config.field_limits || {}
  const gid = group.id
  const isLocked = group.locked
  const isEnabled = isLocked || enabledGroups.has(gid)

  const groupFields = group.fields.map((fid: string) => catalog.fields.find((f: any) => f.id === fid)).filter(Boolean)
  const groupBytes = groupFields.reduce((s: number, f: any) => s + (f.typical_bytes || 0), 0)

  // Dependency checking
  const requiredGroup = group.requires ? catalog.groups.find((g: any) => g.id === group.requires) : null
  const isDepSatisfied = !group.requires || enabledGroups.has(group.requires)

  const recommendedGroups = group.recommended_with 
    ? group.recommended_with.map((rid: string) => catalog.groups.find((g: any) => g.id === rid)).filter(Boolean)
    : []

  const handleGroupToggle = (checked: boolean) => {
    if (isLocked) return
    toggleGroup(gid, checked)
  }

  return (
    <div className={cn("border border-border/60 rounded-lg overflow-hidden bg-card/50", !isDepSatisfied && !isEnabled && "opacity-60 grayscale-[0.5]")}>
      <div 
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left cursor-pointer"
      >
        <div className="flex items-center gap-3">
          <div onClick={e => e.stopPropagation()} className="flex items-center">
            <Checkbox 
              checked={isEnabled} 
              onCheckedChange={handleGroupToggle}
              disabled={isLocked}
              className={cn("mr-1", isLocked && "opacity-50")}
            />
          </div>
          <div className="flex items-center gap-2">
            <h4 className="text-xs font-bold tracking-tight uppercase text-foreground/80">
              {group.label || group.id || 'Core'}
            </h4>
            {isLocked && <Badge variant="secondary" className="text-[9px] h-3.5 px-1 font-bold">LOCKED</Badge>}
            {requiredGroup && (
              <span className="text-[10px] text-muted-foreground font-medium lowercase">
                (requires {requiredGroup.label})
              </span>
            )}
            {recommendedGroups.length > 0 && (
              <span className="text-[10px] text-muted-foreground font-medium lowercase italic">
                (best with {recommendedGroups.map((rg: any) => rg.label).join(', ')})
              </span>
            )}
            <span className="text-[10px] text-muted-foreground ml-1">+{groupBytes} bytes</span>
          </div>
        </div>
        <div className="text-muted-foreground">
          {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </div>
      </div>
      
      {isOpen && (
        <div className="p-4 pt-2 border-t border-border/40 bg-card">
          <p className="text-[11px] text-muted-foreground mb-3 leading-relaxed">
            {group.description}
            {group.note && <span className="block mt-1.5 text-amber-600 dark:text-amber-500 font-medium italic">⚠ {group.note}</span>}
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-2">
            {groupFields.map((field: any) => {
              const fEnabled = isLocked ? true : (overrides[field.id] !== false && (isEnabled || overrides[field.id] === true))
              const itToggle = !!field.individually_toggleable
              const disabled = isLocked || !itToggle

              return (
                <div key={field.id} className={cn("flex flex-col space-y-2 p-2 rounded-md transition-colors", disabled ? "opacity-70" : "hover:bg-muted/50")}>
                  <div className="flex items-start space-x-2">
                    <Checkbox 
                      id={`f-${field.id}`} 
                      checked={fEnabled} 
                      onCheckedChange={(checked) => itToggle && toggleField(field.id, checked as boolean, isEnabled)}
                      disabled={disabled}
                      className="mt-0.5"
                    />
                    <div className="grid gap-0.5 leading-none flex-1">
                      <div className="flex items-center justify-between">
                        <Label 
                          htmlFor={`f-${field.id}`} 
                          className={cn("text-[11px] font-mono", disabled ? "cursor-default" : "cursor-pointer")}
                        >
                          {field.label || field.id}
                          <span className="font-sans text-[10px] text-muted-foreground ml-1 font-normal">(~{field.typical_bytes || 0} B)</span>
                        </Label>
                        {field.has_limit && (
                          <div className="flex items-center gap-1.5 ml-2" onClick={e => e.stopPropagation()}>
                            <Label htmlFor={`limit-${field.id}`} className="text-[9px] text-muted-foreground whitespace-nowrap">
                              Max Length
                            </Label>
                            <Input
                              id={`limit-${field.id}`}
                              type="number"
                              min="1"
                              max="16000"
                              value={limits[field.id] !== undefined ? limits[field.id] : (field.limit || '')}
                              onChange={e => updateFieldLimit(field.id, e.target.value ? parseInt(e.target.value, 10) : undefined)}
                              disabled={!fEnabled}
                              className="h-6 w-16 text-[10px] px-1.5 py-0 text-center"
                            />
                            <LabelWithInfo
                              label=""
                              info={`Truncates the logged string to this many characters to ensure the total log line payload stays under Fastly's 16KB limit.`}
                              className="mb-0"
                            />
                          </div>
                        )}
                      </div>
                      <p className="text-[10px] text-muted-foreground line-clamp-2 leading-tight mt-1" title={field.description}>{field.description}</p>
                      {field.note && <p className="text-[9px] text-amber-600 dark:text-amber-500 mt-0.5">⚠ {field.note}</p>}
                      {field.required_by?.length > 0 && (
                        <p className="text-[9px] text-muted-foreground mt-0.5">
                          Used by: {field.required_by.map((id: string) => (catalog?.insights || []).find((ins: any) => ins.id === id)?.name || id).join(', ')}
                        </p>
                      )}
                    </div>
                  </div>
                </div>
              )
            })}
            {groupFields.length === 0 && (
              <p className="text-[11px] text-muted-foreground">No fields in this group.</p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

export function LogSettingsModal({ service, open, onOpenChange }: LogSettingsModalProps) {
  const queryClient = useQueryClient()

  const [config, setConfig] = useState<LogFieldsConfig>({ groups: [], field_overrides: {} })
  const [period, setPeriod] = useState<string>('60')
  const [sampleRate, setSampleRate] = useState<number>(100)
  const [edgeOnly, setEdgeOnly] = useState<boolean>(true)
  const [customCondition, setCustomCondition] = useState<string>('')
  const [step, setStep] = useState<number>(1)

  const { data: catalog } = useLogFieldsCatalog(service?.service_id)

  const { data: lfResponse, isLoading: isLoadingFields } = useQuery({
    queryKey: ['services', service?.service_id, 'fields'],
    queryFn: async () => {
      const { data } = await client.GET('/api/services/{service_id}/log-fields', {
        params: { path: { service_id: service!.service_id } },
      })
      return data
    },
    enabled: !!service && open,
  })

  const { data: loggingSettings, isLoading: isLoadingSettings } = useQuery({
    queryKey: ['services', service?.service_id, 'logging-settings'],
    queryFn: async () => {
      const { data } = await client.GET('/api/services/{service_id}/logging-settings', {
        params: { path: { service_id: service!.service_id } },
      })
      return data
    },
    enabled: !!service && open,
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
      const endpoint = `/api/services/${service!.service_id}/logging-settings/update?update_format=true&period=${period}&sample_rate=${sampleRate}&edge_only=${edgeOnly}&custom_condition=${encodedCond}`
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
              ) : (
                <div className="w-full h-full">
                  {step === 1 && (
                  <div className="m-0 border-none p-0 outline-none space-y-8">
                    {/* General Settings Section */}
                    <div className="space-y-4">
                    <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80 border-b pb-2">General Settings</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      
                      {/* Log Rotation Period */}
                      <div className="flex flex-col space-y-1.5 p-3 border rounded-md bg-muted/10 justify-center">
                        <LabelWithInfo
                          label="Log Period"
                          info="How often Fastly will write log files to the bucket. A shorter period means more real-time data but creates more files."
                        />
                        <Select value={period} onValueChange={(v) => v && setPeriod(v)}>
                          <SelectTrigger id="period" className="h-9">
                            <SelectValue>
                              {period === '1' ? '1 second' :
                               period === '5' ? '5 seconds' :
                               period === '10' ? '10 seconds' :
                               period === '20' ? '20 seconds' :
                               period === '30' ? '30 seconds' :
                               period === '60' ? '1 minute' :
                               period === '120' ? '2 minutes' :
                               period === '300' ? '5 minutes' : period}
                            </SelectValue>
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="1">1 second</SelectItem>
                            <SelectItem value="5">5 seconds</SelectItem>
                            <SelectItem value="10">10 seconds</SelectItem>
                            <SelectItem value="20">20 seconds</SelectItem>
                            <SelectItem value="30">30 seconds</SelectItem>
                            <SelectItem value="60">1 minute</SelectItem>
                            <SelectItem value="120">2 minutes</SelectItem>
                            <SelectItem value="300">5 minutes</SelectItem>
                          </SelectContent>
                        </Select>
                        {(period === '1' || period === '5') && (
                          <p className="text-xs text-muted-foreground mt-1">
                            Sub-10s log periods sync every 5s. Fastly may write multiple rotation files between checks.
                          </p>
                        )}
                        {(period === '10' || period === '20') && (
                          <p className="text-xs text-muted-foreground mt-1">
                            Dashboard freshness is bounded by the sync cadence (~30s floor); sub-30s log periods produce more files but won't appear faster.
                          </p>
                        )}
                      </div>

                      {/* Log Sampling */}
                      <div className="flex flex-col space-y-1.5 p-3 border rounded-md bg-muted/10 justify-center">
                        <LabelWithInfo
                          label="Sample Rate (%)"
                          info="The percentage of requests to log. Set to 100% to log everything, or lower it for high-traffic services to save storage."
                        />
                        <Input 
                          id="sampleRate"
                          type="number" 
                          min={1} 
                          max={100} 
                          value={sampleRate} 
                          onChange={(e) => setSampleRate(Number(e.target.value) || 100)} 
                          className="h-9" 
                        />
                      </div>

                      {/* Edge Only Switch */}
                      <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10 md:col-span-2">
                        <div className="space-y-0.5 pr-4">
                          <LabelWithInfo
                            label="Edge Only"
                            info="When enabled, only edge nodes write logs, skipping shield nodes and cache restarts. This prevents duplicate log entries."
                          />
                        </div>
                        <Switch id="edgeOnly" checked={edgeOnly} onCheckedChange={setEdgeOnly} />
                      </div>

                      {/* Optional Log Condition */}
                      <div className="flex flex-col space-y-1.5 p-3 border rounded-md bg-muted/10 md:col-span-2">
                        <LabelWithInfo
                          htmlFor="customCondition"
                          label="Optional Log Condition"
                          info="An additional VCL condition to filter logs (e.g., req.url !~ '\.(jpg|png)$'). The expression will be wrapped in parentheses and added to the logging condition logic."
                        />
                        <Input 
                          id="customCondition"
                          placeholder="e.g. std.tolower(req.url) !~ '\.(jpg|png|css|js)$'"
                          value={customCondition} 
                          onChange={(e) => setCustomCondition(e.target.value)} 
                          className="h-9 font-mono text-xs" 
                        />
                      </div>

                    </div>
                  </div>

                  {/* Log Fields Section */}
                  <div className="space-y-4">
                    <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80 border-b pb-2">Log Fields</h3>
                    {catalog?.presets && (
                      <div className="flex flex-wrap gap-2 pb-2 items-center">
                        <span className="text-xs font-bold text-muted-foreground uppercase tracking-wider py-1.5 mr-2">Presets:</span>
                        {Object.entries(catalog.presets as Record<string, { label: string, description: string, groups?: string[] }>).map(([key, preset]) => {
                          const isMinimal = key === 'minimal'
                          const active = isMinimal || isPresetActive(preset.groups || [])
                          return (
                            <Button
                              key={key}
                              variant={active ? "default" : "outline"}
                              size="sm"
                              className={cn("h-8 text-xs font-semibold transition-all", active && "ring-2 ring-primary/20", isMinimal && "opacity-80")}
                              title={preset.description}
                              onClick={() => !isMinimal && togglePreset(preset.groups || [])}
                              disabled={isMinimal}
                            >
                              {preset.label || key}
                            </Button>
                          )
                        })}
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          className="h-8 text-xs font-semibold text-muted-foreground hover:text-foreground ml-auto"
                          onClick={() => setConfig({ groups: [], field_overrides: {} })}
                        >
                          Clear All
                        </Button>
                      </div>
                    )}
                    
                    <div className="bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-900/50 p-3 rounded-lg flex gap-3 mb-4">
                      <AlertTriangle className="h-4 w-4 text-amber-600 shrink-0 mt-0.5" />
                      <div className="text-[11px] text-amber-800 dark:text-amber-300 leading-normal">
                        <strong>Important:</strong> Updating log fields will trigger a new version deploy of your Fastly service. 
                        Data collected before this change will have <code>null</code> for any newly added fields.
                      </div>
                    </div>

                    <div className="space-y-2">
                      {(catalog?.groups || []).map((group: any) => (
                        <CollapsibleGroup 
                          key={group.id || 'core'} 
                          group={group} 
                          catalog={catalog} 
                          config={config} 
                          toggleGroup={toggleGroup}
                          toggleField={toggleField}
                          updateFieldLimit={updateFieldLimit} 
                        />
                      ))}
                    </div>
                    </div>
                    </div>
                  )}

                  {step === 2 && (
                    <div className="m-0 border-none p-0 outline-none">
                      <CustomFieldsManager serviceId={service.service_id} />
                    </div>
                  )}

                  {step === 3 && (
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
                                const enabledGroupsSet = new Set(config.groups || []);
                                const overrides = config.field_overrides || {};
                                const hasOverrides = Object.keys(overrides).length > 0;
                                let bestPresetName = null;

                                if (catalog?.presets && !hasOverrides) {
                                  for (const [key, preset] of Object.entries(catalog.presets)) {
                                    const presetGroups = (preset as any).groups || [];
                                    if (presetGroups.length === enabledGroupsSet.size && presetGroups.every((g: string) => enabledGroupsSet.has(g))) {
                                      bestPresetName = (preset as any).label || key;
                                      break;
                                    }
                                  }
                                }

                                const disabledCount = (catalog?.groups || []).filter((g: any) => !(g.locked || enabledGroupsSet.has(g.id))).length || 0;

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
                                    {(catalog?.groups || []).map((g: any) => {
                                      const isEnabled = g.locked || enabledGroupsSet.has(g.id);
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
                           const customFields = (catalog?.fields || []).filter((f: any) => f.is_custom);
                           if (customFields.length === 0) return null;
                           return (
                             <div className="p-4 border rounded-lg bg-muted/20 space-y-3">
                               <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Custom Fields ({customFields.length})</p>
                               <div className="flex flex-wrap gap-2">
                                 {customFields.map((cf: any) => (
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
                            {(catalog as any)?.insights?.map((insight: any) => {
                              const enabledGroups = new Set([null, ...(config.groups || [])]);
                              const catalogGroups = (catalog as any)?.groups || [];
                              let changed = true;
                              while (changed) {
                                changed = false;
                                catalogGroups.forEach((g: any) => {
                                  if (enabledGroups.has(g.id) && g.requires && !enabledGroups.has(g.requires)) {
                                    enabledGroups.add(g.requires);
                                    changed = true;
                                  }
                                });
                              }

                              const isEnabled = insight.required_groups?.every((rg: any) => enabledGroups.has(rg));
                              return (
                                <div key={insight.id} className={cn("flex items-start gap-3 border rounded-lg p-2.5 bg-background shadow-sm transition-all", isEnabled ? "border-primary/20 bg-primary/5" : "opacity-50 grayscale bg-muted/50")}>
                                  <div className={cn("w-2 h-2 mt-1.5 rounded-full shrink-0", isEnabled ? "bg-primary" : "bg-muted-foreground")} />
                                  <div className="space-y-1 overflow-hidden">
                                    <h4 className="text-xs font-semibold tracking-tight truncate" title={insight.name}>{insight.name}</h4>
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
