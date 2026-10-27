'use client'
import React, { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useRouter } from 'next/navigation'
import { AlertCircle, CheckCircle2, DollarSign, Loader2, MapPin, Pencil, RefreshCw, Save } from 'lucide-react'
import type { components } from '@/types/api.generated'
import { client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { buildServiceHref, cn, panelDialogContent, panelDialogFooter, panelDialogHeaderMuted } from '@/lib/utils'
import { useBootstrap, useMounted } from '@/hooks/useBootstrap'
import { useDerivedState } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/filterStore'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { BotSourcesPanel } from '@/app/control-room/_sections/ControlRoomClient'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'

type PopLocation = components['schemas']['PopLocation']
function popCoord(pop: PopLocation, axis: 'latitude' | 'longitude'): number | null {
  const v = pop[axis] ?? pop.coordinates?.[axis] ?? pop.attributes?.[axis]
  return typeof v === 'number' ? v : null
}

function PopLocationsModal({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const [apiKey, setApiKey] = useState('')
  const queryClient = useQueryClient()
  const { data, isLoading } = useQuery({ queryKey: ['pop-locations'], queryFn: async () => unwrapClient(client.GET('/api/admin/pop-locations')), enabled: open })
  const mutation = useMutation({
    mutationFn: async () => unwrapClient(client.POST('/api/admin/pop-locations/refresh', { body: { token: apiKey } })),
    onSuccess: (result) => { queryClient.setQueryData(['pop-locations'], { pops: result?.pops || [] }); setApiKey('') },
  })
  const pops = data?.pops || []
  const withCoords = pops.filter(p => popCoord(p, 'latitude') != null)
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn('sm:max-w-2xl', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderMuted}>
          <DialogTitle className="flex items-center gap-2"><MapPin className="h-4 w-4" />POP Location Data</DialogTitle>
          <p className="text-sm text-muted-foreground mt-1">POP coordinates power the Impossible Distance insight — they detect when a client&apos;s claimed geo location is physically impossible given their TCP RTT. Refresh when Fastly adds new POPs.</p>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium">Cached POPs</span>
            <div className="flex items-center gap-2">
              <Badge variant={withCoords.length > 0 ? 'success' : 'secondary'} className="text-[10px] px-1.5">{withCoords.length} with coordinates</Badge>
              {pops.length > 0 && <Badge variant="outline" className="text-[10px] px-1.5">{pops.length} total</Badge>}
            </div>
          </div>
          {isLoading ? (
            <div className="h-48 flex items-center justify-center text-muted-foreground text-sm animate-pulse border rounded-lg">Loading...</div>
          ) : pops.length === 0 ? (
            <div className="h-48 flex flex-col items-center justify-center gap-2 border rounded-lg bg-muted/20">
              <AlertCircle className="h-5 w-5 text-muted-foreground" />
              <p className="text-sm text-muted-foreground">No POP data cached yet.</p>
              <p className="text-xs text-muted-foreground">Enter your Fastly API key below to fetch it.</p>
            </div>
          ) : (
            <ScrollArea className="h-52 border rounded-lg">
              <div className="divide-y">
                {pops.map((pop) => {
                  const code = pop.code || '?'
                  const lat = popCoord(pop, 'latitude'), lon = popCoord(pop, 'longitude')
                  return (
                    <div key={code} className="flex items-center gap-3 px-3 py-1.5 hover:bg-muted/30">
                      <span className="font-mono text-xs font-bold w-10 shrink-0 text-foreground">{code}</span>
                      <span className="text-xs text-muted-foreground flex-1 truncate">{pop.name}</span>
                      {lat != null && lon != null ? <span className="text-[10px] font-mono text-muted-foreground/70 shrink-0 tabular-nums">{lat.toFixed(2)}, {lon.toFixed(2)}</span> : <span className="text-[10px] text-destructive/70 shrink-0">no coords</span>}
                    </div>
                  )
                })}
              </div>
            </ScrollArea>
          )}
          <div className="space-y-2 pt-2 border-t">
            <Label htmlFor="pop-api-key" className="text-sm font-medium">Update from Fastly API</Label>
            <p className="text-xs text-muted-foreground">A read-only Fastly API token is sufficient. The key is used once to call <code className="font-mono">/datacenters</code> and is not stored.</p>
            <div className="flex gap-2">
              <input id="pop-api-key" type="password" placeholder="Fastly API key..." value={apiKey} onChange={e => setApiKey(e.target.value)} onKeyDown={e => e.key === 'Enter' && apiKey.trim() && !mutation.isPending && mutation.mutate()} className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 font-mono" />
              <Button onClick={() => mutation.mutate()} disabled={!apiKey.trim() || mutation.isPending} className="shrink-0">
                <RefreshCw className={`h-4 w-4 mr-2 ${mutation.isPending ? 'animate-spin' : ''}`} />{mutation.isPending ? 'Fetching...' : 'Update'}
              </Button>
            </div>
            {mutation.isSuccess && <div className="flex items-center gap-2 text-sm text-green-600 dark:text-green-400"><CheckCircle2 className="h-4 w-4" />Updated — {withCoords.length} POPs with coordinates cached.</div>}
            {mutation.isError && <div className="flex items-center gap-2 text-sm text-destructive"><AlertCircle className="h-4 w-4" />{mutation.error?.message || 'Update failed. Check your API key.'}</div>}
          </div>
        </div>
        <DialogFooter className={panelDialogFooter} />
      </DialogContent>
    </Dialog>
  )
}

export function DiagnosticsPanel() {
  const queryClient = useQueryClient()
  const { data: settings, isLoading: isSettingsLoading } = useQuery({
    queryKey: queryKeys.admin.debugSettings(),
    queryFn: async () => {
      const { data, error } = await client.GET('/api/admin/debug-settings')
      if (error) throw new Error(extractApiError(error) || 'Failed to fetch debug settings')
      return data
    },
  })
  const updateSettingsMutation = useMutation({
    mutationFn: async (body: { query_debug_visibility?: string; api_call_debug_visibility?: string }) => {
      const { data, error } = await client.PATCH('/api/admin/debug-settings', { body })
      if (error) throw new Error(extractApiError(error) || 'Failed to update debug settings')
      return data
    },
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: queryKeys.admin.debugSettings() }); queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() }) },
  })
  const { data: bootstrapData } = useBootstrap()
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const services = useServiceStore((s) => s.services)
  const activeService = services.find((s) => s.id === activeServiceId) ?? services[0]
  const queryEngineLabel = activeService?.is_high_scale ? 'ClickHouse' : 'DuckDB'
  const debugBackendOn = (bootstrapData as { debug_state?: { debug_responses_enabled?: boolean } } | undefined)?.debug_state?.debug_responses_enabled !== false
  const debugDisabledTooltip = !debugBackendOn ? 'Backend debug responses are disabled — set DEBUG_RESPONSES=true in the server env (or .env file) and restart to see data here.' : undefined
  const panels = [
    { id: 'diag-query-debug-label', title: 'Query debugging panel', desc: `Bottom-of-screen panel with ${queryEngineLabel} SQL queries and execution times.`, value: settings?.query_debug_visibility ?? 'disabled', onChange: (val: string | null) => val && updateSettingsMutation.mutate({ query_debug_visibility: val }) },
    { id: 'diag-api-panel-label', title: 'API call panel', desc: 'Bottom-of-screen panel with all Fastly API calls and FOS operations per request.', value: settings?.api_call_debug_visibility ?? 'disabled', onChange: (val: string | null) => val && updateSettingsMutation.mutate({ api_call_debug_visibility: val }) },
  ]
  return (
    <>
      {panels.map((p) => (
        <div key={p.id} className={`flex flex-col p-3 border rounded-lg gap-3 ${!debugBackendOn ? 'opacity-60' : ''}`}>
          <div className="min-w-0 space-y-0.5">
            <Label id={p.id} className="text-sm font-medium">{p.title}</Label>
            <p className="text-xs text-muted-foreground">{p.desc}</p>
            {!debugBackendOn && <p className="text-[11px] text-amber-500" title={debugDisabledTooltip}>Disabled — backend ``DEBUG_RESPONSES`` env is off.</p>}
          </div>
          <div className="flex items-center justify-end mt-auto" title={debugDisabledTooltip}>
            <Select value={p.value} onValueChange={p.onChange} disabled={!debugBackendOn || isSettingsLoading || updateSettingsMutation.isPending}>
              <SelectTrigger className="h-8 text-xs w-[180px]"><SelectValue placeholder="Select visibility..." /></SelectTrigger>
              <SelectContent>
                <SelectItem value="disabled" className="text-xs">Off / Disabled</SelectItem>
                <SelectItem value="admins" className="text-xs">Admins Only</SelectItem>
                <SelectItem value="analysts" className="text-xs">Analysts Only</SelectItem>
                <SelectItem value="both" className="text-xs">Both Admins & Analysts</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      ))}
    </>
  )
}

function UsageLogRetentionInput({ initial, onSave }: { initial: number; onSave: (days: number) => void }) {
  const [value, setValue] = useDerivedState(String(initial))
  return (
    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
      <span>Keep for</span>
      <Input type="number" min={1} aria-label="Usage-log retention in days" className="h-7 w-14 text-xs text-right" value={value} onChange={(e) => setValue(e.target.value)}
        onBlur={() => { const n = parseInt(value); if (Number.isFinite(n) && n >= 1) onSave(n); else setValue(String(initial)) }}
        onKeyDown={(e) => { if (e.key === 'Enter') (e.target as HTMLInputElement).blur() }}
      />
      <span>days</span>
    </div>
  )
}

const _PRICING_DEFAULTS = { class_a_rate_per_1k: 0.005, class_b_rate_per_10k: 0.01, cdn_egress_rate_per_gb: 0.12, storage_rate_per_gb_month: 0.02, min_billed_days: 30 }

export const PricingSettings = () => {
  const queryClient = useQueryClient()
  const mounted = useMounted()
  const [editing, setEditing] = useState(false)
  const { data: settings, isLoading } = useQuery({ queryKey: ['usage-logging-settings'], queryFn: async () => (await unwrapClient(client.GET('/api/admin/usage-logging'))) ?? null })
  const serverForm = useMemo(() => ({
    rateA: String(settings?.class_a_rate_per_1k ?? _PRICING_DEFAULTS.class_a_rate_per_1k),
    rateB: String(settings?.class_b_rate_per_10k ?? _PRICING_DEFAULTS.class_b_rate_per_10k),
    rateCdn: String(settings?.cdn_egress_rate_per_gb ?? _PRICING_DEFAULTS.cdn_egress_rate_per_gb),
    rateStorage: String(settings?.storage_rate_per_gb_month ?? _PRICING_DEFAULTS.storage_rate_per_gb_month),
    minBilledDays: String(settings?.min_billed_days ?? _PRICING_DEFAULTS.min_billed_days),
  }), [settings])
  const [form, setForm] = useDerivedState(serverForm)
  const { rateA, rateB, rateCdn, rateStorage, minBilledDays } = form
  const fieldSetter = (field: keyof typeof form) => (value: string) => setForm((f) => ({ ...f, [field]: value }))
  const saveMutation = useMutation({
    mutationFn: async () => {
      await unwrapClient(client.PATCH('/api/admin/usage-logging', { body: {
        class_a_rate_per_1k: parseFloat(rateA), class_b_rate_per_10k: parseFloat(rateB), cdn_egress_rate_per_gb: parseFloat(rateCdn),
        storage_rate_per_gb_month: parseFloat(rateStorage), min_billed_days: parseInt(minBilledDays),
      } }))
    },
    onSuccess: () => {
      for (const k of [['usage-logging-settings'], ['usage'], ['usage-log']]) queryClient.invalidateQueries({ queryKey: k })
      setEditing(false)
    },
  })
  const saving = saveMutation.isPending
  if (!mounted || isLoading) return <AnalyticsCard title="FOS Pricing Defaults" isLoading>{null}</AnalyticsCard>
  const fields = [
    { label: 'Class A Ops ($/1k)', value: rateA, setValue: fieldSetter('rateA') },
    { label: 'Class B Ops ($/10k)', value: rateB, setValue: fieldSetter('rateB') },
    { label: 'CDN Egress ($/GB)', value: rateCdn, setValue: fieldSetter('rateCdn') },
    { label: 'Storage ($/GB/mo)', value: rateStorage, setValue: fieldSetter('rateStorage') },
    { label: 'Min. Days Billed/Object', value: minBilledDays, setValue: fieldSetter('minBilledDays') },
  ]
  return (
    <AnalyticsCard
      title="Pricing & Retention Defaults" description="Global rates used for cost estimation across all services. Changes apply to all historical views." icon={<DollarSign className="h-4 w-4" />}
      headerAction={!editing ? <Button size="sm" variant="outline" onClick={() => setEditing(true)} className="h-8 font-bold uppercase tracking-tight"><Pencil className="h-3 w-3 mr-1.5" />Edit</Button> : null}
    >
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {fields.map((f) => {
          const inputId = `gs-${f.label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`
          return (
            <div key={f.label} className="space-y-1.5">
              <Label htmlFor={inputId} className="text-[10px] uppercase font-bold text-muted-foreground">{f.label}</Label>
              {editing ? <Input id={inputId} className="h-8 font-mono text-xs" value={f.value} onChange={(e) => f.setValue(e.target.value)} /> : <div className="h-8 flex items-center font-mono text-xs px-3 rounded-md bg-muted/40 border border-transparent">{f.value}</div>}
            </div>
          )
        })}
      </div>
      {editing && (
        <div className="flex justify-end gap-2 mt-4">
          <Button size="sm" variant="outline" onClick={() => { if (settings) setForm(serverForm); setEditing(false) }} disabled={saving} className="h-8 font-bold uppercase tracking-tight">Cancel</Button>
          <Button size="sm" onClick={() => saveMutation.mutate()} disabled={saving} className="h-8 font-bold uppercase tracking-tight">
            {saving ? <Loader2 className="h-3 w-3 mr-1.5 animate-spin" /> : <Save className="h-3 w-3 mr-1.5" />}Save Changes
          </Button>
        </div>
      )}
    </AnalyticsCard>
  )
}

export function GlobalSettings() {
  const queryClient = useQueryClient()
  const router = useRouter()
  const mounted = useMounted()
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const [popLocationsOpen, setPopLocationsOpen] = useState(false)
  const { data: queryUsageLoggingSettings } = useQuery({ queryKey: ['usage-logging-settings'], queryFn: async () => unwrapClient(client.GET('/api/admin/usage-logging')), staleTime: 60_000 })
  const usageLoggingSettings = mounted ? queryUsageLoggingSettings : undefined
  const usageLoggingEnabled = usageLoggingSettings?.enabled ?? false
  const usageLogRetention = usageLoggingSettings?.retention_days ?? 30
  const retentionMutation = useMutation({
    mutationFn: async (days: number) => { await unwrapClient(client.PATCH('/api/admin/usage-logging', { body: { retention_days: days } })) },
    onMutate: (days) => queryClient.setQueryData(['usage-logging-settings'], (old: typeof usageLoggingSettings) => ({ ...old, retention_days: days })),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] }),
  })
  const toggleMutation = useMutation({
    mutationFn: async (enabled: boolean) => { await unwrapClient(client.PATCH('/api/admin/usage-logging', { body: { enabled } })) },
    onMutate: (enabled) => queryClient.setQueryData(['usage-logging-settings'], (old: typeof usageLoggingSettings) => ({ ...old, enabled })),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] }),
  })
  return (
    <>
      <AnalyticsCard title="Overall Settings" description="Global preferences for the application.">
        <div className="flex flex-col gap-3">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
            <DiagnosticsPanel />
            <div className="flex flex-col p-3 border rounded-lg gap-3">
              <div className="min-w-0 space-y-0.5">
                <Label id="gs-usage-log-label" className="text-sm font-medium">Log FOS / CDN usage</Label>
                <p className="text-xs text-muted-foreground">Records every Class A/B operation and CDN download with function + process context for cost analysis.</p>
              </div>
              <div className="flex items-center justify-end gap-2 flex-wrap mt-auto">
                {usageLoggingEnabled && (
                  <>
                    <UsageLogRetentionInput initial={usageLogRetention} onSave={(days) => { if (Number.isFinite(days) && days >= 1 && days !== usageLogRetention) retentionMutation.mutate(days) }} />
                    <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => router.push(buildServiceHref('/admin/usage-log', activeServiceId))}>View Usage Log</Button>
                  </>
                )}
                <Switch aria-labelledby="gs-usage-log-label" checked={usageLoggingEnabled} onCheckedChange={(enabled) => toggleMutation.mutate(enabled)} disabled={toggleMutation.isPending} />
              </div>
            </div>
            <div className="flex flex-col p-3 border rounded-lg gap-3">
              <div className="min-w-0 space-y-0.5">
                <Label className="text-sm font-medium">POP location data</Label>
                <p className="text-xs text-muted-foreground">Fastly PoP coordinates used by the Impossible Distance insight for geo/RTT spoofing detection.</p>
              </div>
              <div className="flex items-center justify-end mt-auto">
                <Button variant="outline" size="sm" onClick={() => setPopLocationsOpen(true)}><MapPin className="h-3.5 w-3.5 mr-1.5" /> Update POP Info</Button>
              </div>
            </div>
          </div>
          <BotSourcesPanel />
        </div>
      </AnalyticsCard>
      <PopLocationsModal open={popLocationsOpen} onOpenChange={setPopLocationsOpen} />
    </>
  )
}
