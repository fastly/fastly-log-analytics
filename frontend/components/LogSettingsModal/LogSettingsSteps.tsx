'use client'

import React, { useState, useMemo } from 'react'
import { evaluateLogFieldPresetSummary, resolveEnabledGroupsWithDeps } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { AlertTriangle, ChevronRight, ChevronDown } from 'lucide-react'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Checkbox } from '@/components/ui/checkbox'
import { Switch } from '@/components/ui/switch'
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { cn, formatBytes, PERIOD_LABELS } from '@/lib/utils'
import type { ServiceConfig, LogFieldsConfig, components } from '@/types/api'

type Catalog = components['schemas']['LogFieldsCatalogResponse']
type CatalogGroup = components['schemas']['LogFieldCatalogGroup']

export const PERIOD_OPTIONS = ['1', '5', '10', '20', '30', '60', '120', '300'] as const

export function CollapsibleSectionShell({
  checked, onCheckedChange, disabled, dimmed, headerContent, children,
}: {
  checked: boolean; onCheckedChange: (checked: boolean) => void
  disabled?: boolean; dimmed?: boolean; headerContent: React.ReactNode; children: React.ReactNode
}) {
  const [isOpen, setIsOpen] = useState(false)
  return (
    <div className={cn('border border-border/60 rounded-lg overflow-hidden bg-card/50', dimmed && 'opacity-60 grayscale-[0.5]')}>
      <div className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left">
        <div className="flex items-center gap-3">
          <Checkbox checked={checked} onCheckedChange={(c) => onCheckedChange(c as boolean)} disabled={disabled} className={cn('mr-1', disabled && 'opacity-50')} />
          <button type="button" onClick={() => setIsOpen(!isOpen)} aria-expanded={isOpen} className="flex items-center gap-2 text-left cursor-pointer bg-transparent border-0 p-0">
            {headerContent}
          </button>
        </div>
        <button type="button" onClick={() => setIsOpen(!isOpen)} aria-label={isOpen ? 'Collapse group' : 'Expand group'} className="text-muted-foreground bg-transparent border-0 p-0 cursor-pointer">
          {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </button>
      </div>
      {isOpen && <div className="p-4 pt-2 border-t border-border/40 bg-card">{children}</div>}
    </div>
  )
}

interface CmcdConfigSectionProps {
  enabled: boolean; onEnabledChange: (enabled: boolean) => void
  mode: string; onModeChange: (mode: string) => void
  version: number; onVersionChange: (version: number) => void
  disabled?: boolean
}

export function CmcdConfigSection({
  enabled, onEnabledChange, mode, onModeChange, version, onVersionChange, disabled,
}: CmcdConfigSectionProps) {
  const selects = [
    {
      id: 'cmcd-version', label: 'CMCD Version', value: String(version),
      onChange: (v: string) => onVersionChange(Number(v)),
      items: [['1', 'v1 (CTA-5004)'], ['2', 'v2 (CTA-5004-2)']] as const,
    },
    {
      id: 'cmcd-mode', label: 'Transport Mode', value: mode,
      onChange: (v: string) => onModeChange(v),
      items: [['query_string', 'Query String (?CMCD=...)'], ['headers', 'HTTP Headers (CMCD-*)']] as const,
    },
  ]
  return (
    <CollapsibleSectionShell
      checked={enabled}
      onCheckedChange={onEnabledChange}
      disabled={disabled}
      headerContent={
        <>
          <h4 className="text-xs font-bold tracking-tight uppercase text-foreground/80">CMCD Streaming Metrics</h4>
          <span className="text-[10px] text-muted-foreground ml-1">+~348 bytes</span>
        </>
      }
    >
      <p className="text-[11px] text-muted-foreground mb-3 leading-relaxed">
        Capture Common Media Client Data (CTA-5004) fields from streaming video players.
        Extracts session, bitrate, buffer, and quality metrics at the edge.
        <span className="block mt-1.5 text-amber-600 dark:text-amber-500 font-medium italic">
          ⚠ Enabling this group deploys a VCL snippet to your service to extract CMCD fields from query strings or HTTP headers.
        </span>
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {selects.map((s) => (
          <div key={s.id} className="space-y-1.5">
            <Label htmlFor={s.id} className="text-[11px] font-medium">{s.label}</Label>
            <Select value={s.value} onValueChange={(v) => v && s.onChange(v)} disabled={disabled || !enabled}>
              <SelectTrigger id={s.id} className="w-full"><SelectValue /></SelectTrigger>
              <SelectContent>
                {s.items.map(([val, text]) => <SelectItem key={val} value={val}>{text}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
        ))}
      </div>
    </CollapsibleSectionShell>
  )
}

interface CollapsibleGroupProps {
  group: CatalogGroup; catalog: Catalog; config: LogFieldsConfig
  toggleGroup: (groupId: string, checked: boolean) => void
  toggleField: (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => void
  updateFieldLimit: (fieldId: string, limit?: number) => void
}

export function CollapsibleGroup({ group, catalog, config, toggleGroup, toggleField, updateFieldLimit }: CollapsibleGroupProps) {
  const fieldsById = useMemo(() => new Map((catalog.fields || []).map((f) => [f.id, f] as const)), [catalog.fields])
  const groupsById = useMemo(
    () => new Map((catalog.groups || []).flatMap((g) => (g.id != null ? [[g.id, g] as const] : []))),
    [catalog.groups],
  )
  const insightsById = useMemo(() => new Map((catalog.insights || []).map((ins) => [ins.id, ins] as const)), [catalog.insights])

  const enabledGroups = new Set(config.groups)
  const overrides = config.field_overrides || {}
  const limits = config.field_limits || {}
  const gid = group.id
  const isLocked = group.locked
  const isEnabled = isLocked || (gid != null && enabledGroups.has(gid))

  const groupFields = group.fields.flatMap((fid) => fieldsById.get(fid) ?? [])
  const groupBytes = groupFields.reduce((s, f) => s + (f.typical_bytes || 0), 0)
  const requiredGroup = group.requires ? groupsById.get(group.requires) : null
  const isDepSatisfied = !group.requires || enabledGroups.has(group.requires)
  const recommendedGroups = group.recommended_with ? group.recommended_with.flatMap((rid) => groupsById.get(rid) ?? []) : []

  return (
    <CollapsibleSectionShell
      checked={isEnabled}
      onCheckedChange={(checked) => { if (!isLocked && gid != null) toggleGroup(gid, checked) }}
      disabled={isLocked}
      dimmed={!isDepSatisfied && !isEnabled}
      headerContent={
        <>
          <h4 className="text-xs font-bold tracking-tight uppercase text-foreground/80">{group.label || group.id || 'Core'}</h4>
          {isLocked && <Badge variant="secondary" className="text-[9px] h-3.5 px-1 font-bold">LOCKED</Badge>}
          {requiredGroup && <span className="text-[10px] text-muted-foreground font-medium lowercase">(requires {requiredGroup.label})</span>}
          {recommendedGroups.length > 0 && (
            <span className="text-[10px] text-muted-foreground font-medium lowercase italic">
              (best with {recommendedGroups.map((rg) => rg.label).join(', ')})
            </span>
          )}
          <span className="text-[10px] text-muted-foreground ml-1">+{groupBytes} bytes</span>
        </>
      }
    >
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
            <div key={field.id} className={cn('flex flex-col space-y-2 p-2 rounded-md transition-colors', disabled ? 'opacity-70' : 'hover:bg-muted/50')}>
              <div className="flex items-start space-x-2">
                <Checkbox id={`f-${field.id}`} checked={fEnabled} onCheckedChange={(checked) => itToggle && toggleField(field.id, checked as boolean, isEnabled)} disabled={disabled} className="mt-0.5" />
                <div className="grid gap-0.5 leading-none flex-1">
                  <div className="flex items-center justify-between">
                    <Label htmlFor={`f-${field.id}`} className={cn('text-[11px] font-mono', disabled ? 'cursor-default' : 'cursor-pointer')}>
                      {field.label || field.id}
                      <span className="font-sans text-[10px] text-muted-foreground ml-1 font-normal">(~{field.typical_bytes || 0} B)</span>
                    </Label>
                    {field.has_limit && (
                      <div className="flex items-center gap-1.5 ml-2">
                        <Label htmlFor={`limit-${field.id}`} className="text-[9px] text-muted-foreground whitespace-nowrap">Max Length</Label>
                        <Input
                          id={`limit-${field.id}`} type="number" min="1" max="16000"
                          value={limits[field.id] !== undefined ? limits[field.id] : (field.limit || '')}
                          onChange={(e) => updateFieldLimit(field.id, e.target.value ? parseInt(e.target.value, 10) : undefined)}
                          disabled={!fEnabled} className="h-6 w-16 text-[10px] px-1.5 py-0 text-center"
                        />
                        <LabelWithInfo label="" info="Truncates the logged string to this many characters to ensure the total log line payload stays under Fastly's 16KB limit." className="mb-0" />
                      </div>
                    )}
                  </div>
                  <p className="text-[10px] text-muted-foreground line-clamp-2 leading-tight mt-1" title={field.description}>{field.description}</p>
                  {field.note && <p className="text-[9px] text-amber-600 dark:text-amber-500 mt-0.5">⚠ {field.note}</p>}
                  {field.required_by && field.required_by.length > 0 && (
                    <p className="text-[9px] text-muted-foreground mt-0.5">Used by: {field.required_by.map((id) => insightsById.get(id)?.title || id).join(', ')}</p>
                  )}
                </div>
              </div>
            </div>
          )
        })}
        {groupFields.length === 0 && <p className="text-[11px] text-muted-foreground">No fields in this group.</p>}
      </div>
    </CollapsibleSectionShell>
  )
}

interface StandardFieldsStepProps {
  catalog: Catalog | undefined; config: LogFieldsConfig; setConfig: (cfg: LogFieldsConfig) => void
  period: string; setPeriod: (v: string) => void; sampleRate: number; setSampleRate: (n: number) => void
  edgeOnly: boolean; setEdgeOnly: (v: boolean) => void; customCondition: string; setCustomCondition: (v: string) => void
  cmcdEnabled?: boolean; setCmcdEnabled?: (v: boolean) => void; cmcdMode?: string; setCmcdMode?: (v: string) => void
  cmcdVersion?: number; setCmcdVersion?: (v: number) => void
  toggleGroup: (groupId: string, checked: boolean) => void
  toggleField: (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => void
  updateFieldLimit: (fieldId: string, limit?: number) => void
  togglePreset: (presetGroups: string[]) => void; isPresetActive: (groups: string[]) => boolean
}

export function StandardFieldsStep({
  catalog, config, setConfig, period, setPeriod, sampleRate, setSampleRate,
  edgeOnly, setEdgeOnly, customCondition, setCustomCondition,
  cmcdEnabled, setCmcdEnabled, cmcdMode, setCmcdMode, cmcdVersion, setCmcdVersion,
  toggleGroup, toggleField, updateFieldLimit, togglePreset, isPresetActive,
}: StandardFieldsStepProps) {
  return (
    <div className="m-0 border-none p-0 outline-none space-y-8">
      <div className="space-y-4">
        <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80 border-b pb-2">General Settings</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="flex flex-col space-y-1.5 p-3 border rounded-md bg-muted/10 justify-center">
            <LabelWithInfo
              label="Log Period"
              info="How often Fastly will write log files to the bucket. A shorter period means more real-time data but creates more files."
            />
            <Select value={period} onValueChange={(v) => v && setPeriod(v)}>
              <SelectTrigger id="period" className="h-9">
                <SelectValue>{PERIOD_LABELS[period] ?? period}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                {PERIOD_OPTIONS.map((v) => <SelectItem key={v} value={v}>{PERIOD_LABELS[v]}</SelectItem>)}
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

          <div className="flex flex-col space-y-1.5 p-3 border rounded-md bg-muted/10 justify-center">
            <LabelWithInfo
              label="Sample Rate (%)"
              info="The percentage of requests to log. Set to 100% to log everything, or lower it for high-traffic services to save storage."
            />
            <Input id="sampleRate" type="number" min={1} max={100} value={sampleRate} onChange={(e) => setSampleRate(Number(e.target.value) || 100)} className="h-9" />
          </div>

          <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10 md:col-span-2">
            <div className="space-y-0.5 pr-4">
              <LabelWithInfo
                label="Edge Only"
                info="When enabled, only edge nodes write logs, skipping shield nodes and cache restarts. This prevents duplicate log entries."
              />
            </div>
            <Switch id="edgeOnly" aria-label="Edge Only" checked={edgeOnly} onCheckedChange={setEdgeOnly} />
          </div>

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
                  variant={active ? 'default' : 'outline'}
                  size="sm"
                  className={cn('h-8 text-xs font-semibold transition-all', active && 'ring-2 ring-primary/20', isMinimal && 'opacity-80')}
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
  service: ServiceConfig; catalog: components['schemas']['LogFieldsCatalogResponse'] | undefined
  config: LogFieldsConfig; period: string; sampleRate: number; edgeOnly: boolean
  customCondition: string; estimatedBytes: number
  cmcdEnabled?: boolean; cmcdMode?: string; cmcdVersion?: number
}

export function ReviewStep({
  service, catalog, config, period, sampleRate, edgeOnly,
  customCondition, estimatedBytes, cmcdEnabled, cmcdMode, cmcdVersion,
}: ReviewStepProps) {
  const { enabledGroupsSet, bestPresetName, disabledCount } = evaluateLogFieldPresetSummary(config, catalog)
  const customFields = (catalog?.fields || []).filter((f) => f.is_custom)
  const enabledGroupsWithDeps = resolveEnabledGroupsWithDeps(config.groups, catalog)

  return (
    <div className="space-y-6">
      <h3 className="text-lg font-semibold border-b pb-2">Review Log Configuration Changes</h3>
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div className="p-4 border rounded-lg bg-muted/20 space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">General Settings</p>
            <p className="text-sm font-medium">Log Period: {period} seconds</p>
            <p className="text-sm font-medium">Sample Rate: {sampleRate}%</p>
            <p className="text-sm font-medium">Edge Only: {edgeOnly ? 'Yes' : 'No'}</p>
            {customCondition && (
              <p className="text-sm font-medium truncate" title={customCondition}>
                Custom Condition: <code className="text-[10px] bg-background px-1 rounded border">{customCondition}</code>
              </p>
            )}
            <p className="text-sm font-medium">
              CMCD: {cmcdEnabled ? `Enabled (v${cmcdVersion ?? 1}, ${cmcdMode === 'headers' ? 'Headers' : 'Query String'})` : 'Disabled'}
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
              <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                {bestPresetName ? `${bestPresetName} Preset` : 'Custom Configuration'}
              </div>
              {!bestPresetName && (catalog?.groups || []).map((g) => {
                if (!(g.locked || (g.id != null && enabledGroupsSet.has(g.id)))) return null
                return (
                  <div key={g.id || 'core'} className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-primary/10 text-primary border border-primary/20">
                    {g.label}
                  </div>
                )
              })}
              {disabledCount > 0 && (
                <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                  +{disabledCount} disabled
                </div>
              )}
            </div>
          </div>
        </div>

        {customFields.length > 0 && (
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
        )}

        <div className="p-4 border rounded-lg bg-muted/20 space-y-3">
          <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider flex justify-between items-center">
            Automated Insights
            <span className="text-[10px] normal-case font-medium">Derived from logs</span>
          </p>
          <div className="grid grid-cols-2 gap-3">
            {catalog?.insights?.map((insight) => {
              const isEnabled = insight.required_groups.every((rg) => enabledGroupsWithDeps.has(rg))
              return (
                <div key={insight.id} className={cn('flex items-start gap-3 border rounded-lg p-2.5 bg-background shadow-sm transition-all', isEnabled ? 'border-primary/20 bg-primary/5' : 'opacity-50 grayscale bg-muted/50')}>
                  <div className={cn('w-2 h-2 mt-1.5 rounded-full shrink-0', isEnabled ? 'bg-primary' : 'bg-muted-foreground')} />
                  <div className="space-y-1 overflow-hidden">
                    <h4 className="text-xs font-semibold tracking-tight truncate" title={insight.title}>{insight.title}</h4>
                    <p className="text-[10px] text-muted-foreground line-clamp-2 leading-snug">{insight.description}</p>
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        <div className="bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-900/50 p-4 rounded-lg flex gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-600 shrink-0 mt-0.5" />
          <div className="text-sm text-amber-800 dark:text-amber-300 leading-normal">
            {service.storage_mode === 'terraform' ? (
              <><strong>Note:</strong> This service is managed via Terraform. Direct deployment is disabled. Please view and export the updated Terraform code to apply these changes.</>
            ) : (
              <><strong>Important:</strong> Deploying this configuration will clone your active Fastly service version, update the logging endpoints, and activate the new version. Data collected before this change will have <code>null</code> for any newly added fields.</>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
