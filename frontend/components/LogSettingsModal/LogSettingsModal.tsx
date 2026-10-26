'use client'

import React, { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useOpenGeneration } from '@/hooks/useSSE'
import { client, unwrapClient, queryKeys, estimateLogFieldBytesPerLine, evaluateLogFieldPresetSummary, isLogFieldPresetActive, resolveEnabledGroupsWithDeps, toggleLogFieldGroup, toggleLogFieldOverride, toggleLogFieldPreset, updateLogFieldLimit } from "@/lib/api";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Loader2, Settings2, AlertTriangle, ChevronRight, ChevronDown, FileJson } from 'lucide-react'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import { Switch } from '@/components/ui/switch'
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { cn, panelDialogContent, panelDialogFooter, formatBytes, PERIOD_LABELS } from "@/lib/utils";
import { useSSE } from '@/hooks/useSSE'
import { SSEDialogFooter, SSEExecutionSection } from '@/components/SSEModal'
import { useLogFieldsCatalog } from '@/hooks/useBootstrap'
import { CustomFieldsManager } from '@/components/CustomFields/CustomFieldDrawer'
import type { ServiceConfig, LogFieldsConfig, components } from '@/types/api'

type Catalog = components['schemas']['LogFieldsCatalogResponse']
type CatalogGroup = components['schemas']['LogFieldCatalogGroup']

interface CmcdConfigSectionProps {
  enabled: boolean
  onEnabledChange: (enabled: boolean) => void
  mode: string
  onModeChange: (mode: string) => void
  version: number
  onVersionChange: (version: number) => void
  disabled?: boolean
}

export function CmcdConfigSection({
  enabled,
  onEnabledChange,
  mode,
  onModeChange,
  version,
  onVersionChange,
  disabled,
}: CmcdConfigSectionProps) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <div className="border border-border/60 rounded-lg overflow-hidden bg-card/50">
      <div className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left">
        <div className="flex items-center gap-3">
          <Checkbox
            checked={enabled}
            onCheckedChange={(checked) => onEnabledChange(checked as boolean)}
            disabled={disabled}
            className="mr-1"
          />
          <button
            type="button"
            onClick={() => setIsOpen(!isOpen)}
            aria-expanded={isOpen}
            className="flex items-center gap-2 text-left cursor-pointer bg-transparent border-0 p-0"
          >
            <h4 className="text-xs font-bold tracking-tight uppercase text-foreground/80">
              CMCD Streaming Metrics
            </h4>
            <span className="text-[10px] text-muted-foreground ml-1">+~348 bytes</span>
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
            Capture Common Media Client Data (CTA-5004) fields from streaming video players.
            Extracts session, bitrate, buffer, and quality metrics at the edge.
            <span className="block mt-1.5 text-amber-600 dark:text-amber-500 font-medium italic">
              ⚠ Enabling this group deploys a VCL snippet to your service to extract CMCD fields from query strings or HTTP headers.
            </span>
          </p>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label htmlFor="cmcd-version" className="text-[11px] font-medium">CMCD Version</Label>
              <Select
                value={String(version)}
                onValueChange={(v) => v && onVersionChange(Number(v))}
                disabled={disabled || !enabled}
              >
                <SelectTrigger id="cmcd-version" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1">v1 (CTA-5004)</SelectItem>
                  <SelectItem value="2">v2 (CTA-5004-2)</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="cmcd-mode" className="text-[11px] font-medium">Transport Mode</Label>
              <Select
                value={mode}
                onValueChange={(v) => v && onModeChange(v)}
                disabled={disabled || !enabled}
              >
                <SelectTrigger id="cmcd-mode" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="query_string">Query String (?CMCD=...)</SelectItem>
                  <SelectItem value="headers">HTTP Headers (CMCD-*)</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}


interface CollapsibleGroupProps {
  group: CatalogGroup
  catalog: Catalog
  config: LogFieldsConfig
  toggleGroup: (groupId: string, checked: boolean) => void
  toggleField: (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => void
  updateFieldLimit: (fieldId: string, limit?: number) => void
}

export function CollapsibleGroup({ group, catalog, config, toggleGroup, toggleField, updateFieldLimit }: CollapsibleGroupProps) {
  const [isOpen, setIsOpen] = useState(false)

  // O(1) id lookups instead of repeated O(n) Array.find scans over the
  // catalog. Field ids are unique, so .get returns the same object .find did.
  const fieldsById = React.useMemo(
    () => new Map((catalog.fields || []).map((f) => [f.id, f] as const)),
    [catalog.fields],
  )
  const groupsById = React.useMemo(
    () => new Map((catalog.groups || []).flatMap((g) => (g.id != null ? [[g.id, g] as const] : []))),
    [catalog.groups],
  )
  const insightsById = React.useMemo(
    () => new Map((catalog.insights || []).map((ins) => [ins.id, ins] as const)),
    [catalog.insights],
  )

  const enabledGroups = new Set(config.groups)
  const overrides = config.field_overrides || {}
  const limits = config.field_limits || {}
  const gid = group.id
  const isLocked = group.locked
  const isEnabled = isLocked || (gid != null && enabledGroups.has(gid))

  const groupFields = group.fields.flatMap((fid) => fieldsById.get(fid) ?? [])
  const groupBytes = groupFields.reduce((s, f) => s + (f.typical_bytes || 0), 0)

  // Dependency checking
  const requiredGroup = group.requires ? groupsById.get(group.requires) : null
  const isDepSatisfied = !group.requires || enabledGroups.has(group.requires)

  const recommendedGroups = group.recommended_with
    ? group.recommended_with.flatMap((rid) => groupsById.get(rid) ?? [])
    : []

  const handleGroupToggle = (checked: boolean) => {
    if (isLocked || gid == null) return
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
                (best with {recommendedGroups.map((rg) => rg.label).join(', ')})
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
            {groupFields.map((field) => {
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
                          <div className="flex items-center gap-1.5 ml-2">
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
                      {field.required_by && field.required_by.length > 0 && (
                        <p className="text-[9px] text-muted-foreground mt-0.5">
                          Used by: {field.required_by.map((id) => insightsById.get(id)?.title || id).join(', ')}
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
  catalog: Catalog | undefined
  config: LogFieldsConfig
  setConfig: (cfg: LogFieldsConfig) => void
  period: string
  setPeriod: (v: string) => void
  sampleRate: number
  setSampleRate: (n: number) => void
  edgeOnly: boolean
  setEdgeOnly: (v: boolean) => void
  customCondition: string
  setCustomCondition: (v: string) => void
  cmcdEnabled?: boolean
  setCmcdEnabled?: (v: boolean) => void
  cmcdMode?: string
  setCmcdMode?: (v: string) => void
  cmcdVersion?: number
  setCmcdVersion?: (v: number) => void
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
  cmcdEnabled,
  setCmcdEnabled,
  cmcdMode,
  setCmcdMode,
  cmcdVersion,
  setCmcdVersion,
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
                {PERIOD_LABELS[period] ?? period}
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
              Dashboard freshness is bounded by the sync cadence (~30s floor); sub-30s log periods produce more files but won&apos;t appear faster.
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
          <Switch id="edgeOnly" aria-label="Edge Only" checked={edgeOnly} onCheckedChange={setEdgeOnly} />
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
          {Object.entries(catalog.presets).map(([key, preset]) => {
            const isMinimal = key === 'minimal'
            const active = isMinimal || isPresetActive(preset.groups)
            return (
              <Button
                key={key}
                variant={active ? "default" : "outline"}
                size="sm"
                className={cn("h-8 text-xs font-semibold transition-all", active && "ring-2 ring-primary/20", isMinimal && "opacity-80")}
                title={preset.description}
                onClick={() => !isMinimal && togglePreset(preset.groups)}
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
            onClick={() => setConfig({ ...config, groups: [], field_overrides: {}, field_limits: config.field_limits })}
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
        {catalog && (catalog.groups || []).map((group) => (
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
        {setCmcdEnabled && setCmcdMode && setCmcdVersion && (
          <CmcdConfigSection
            enabled={cmcdEnabled ?? false}
            onEnabledChange={setCmcdEnabled}
            mode={cmcdMode ?? 'query_string'}
            onModeChange={setCmcdMode}
            version={cmcdVersion ?? 1}
            onVersionChange={setCmcdVersion}
          />
        )}
      </div>
      </div>
      </div>
  )
}

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
