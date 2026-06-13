'use client'

import React, { useState } from 'react'
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
import { AlertTriangle, ChevronRight, ChevronDown } from 'lucide-react'
import { cn } from '@/lib/utils'

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
      <div className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left">
        <div className="flex items-center gap-3">
          <Checkbox
            checked={isEnabled}
            onCheckedChange={handleGroupToggle}
            disabled={isLocked}
            className={cn("mr-1", isLocked && "opacity-50")}
          />
          <button
            type="button"
            onClick={() => setIsOpen(!isOpen)}
            aria-expanded={isOpen}
            className="flex items-center gap-2 text-left cursor-pointer bg-transparent border-0 p-0"
          >
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
          </button>
        </div>
        <button
          type="button"
          onClick={() => setIsOpen(!isOpen)}
          aria-label={isOpen ? "Collapse group" : "Expand group"}
          className="text-muted-foreground bg-transparent border-0 p-0 cursor-pointer"
        >
          {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </button>
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

interface StandardFieldsStepProps {
  catalog: any
  config: any
  setConfig: (cfg: any) => void
  period: string
  setPeriod: (v: string) => void
  sampleRate: number
  setSampleRate: (n: number) => void
  edgeOnly: boolean
  setEdgeOnly: (v: boolean) => void
  customCondition: string
  setCustomCondition: (v: string) => void
  toggleGroup: (groupId: string, checked: boolean) => void
  toggleField: (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => void
  updateFieldLimit: (fieldId: string, limit?: number) => void
  togglePreset: (presetGroups: string[]) => void
  isPresetActive: (groups: string[]) => boolean
}

export function StandardFieldsStep({
  catalog,
  config,
  setConfig,
  period,
  setPeriod,
  sampleRate,
  setSampleRate,
  edgeOnly,
  setEdgeOnly,
  customCondition,
  setCustomCondition,
  toggleGroup,
  toggleField,
  updateFieldLimit,
  togglePreset,
  isPresetActive,
}: StandardFieldsStepProps) {
  return (
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
  )
}
