'use client'
import React, { useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useRouter } from 'next/navigation'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { useDerivedState } from '@/hooks/useDerivedState'
import { useMounted } from '@/hooks/useMounted'
import { useServiceStore } from '@/stores/serviceStore'
import { buildServiceHref } from '@/lib/navigation'
import { AnalyticsCard } from "@/components/AnalyticsCard"
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { PopLocationsModal } from '@/components/PopLocationsModal/PopLocationsModal'
import {
  MapPin,
  DollarSign,
  Save,
  Loader2,
  Pencil,
} from 'lucide-react'

import { DiagnosticsPanel } from './DiagnosticsPanel'
import { BotSourcesPanel } from './BotSourcesPanel'

function UsageLogRetentionInput({ initial, onSave }: { initial: number; onSave: (days: number) => void }) {
  const [value, setValue] = useDerivedState(String(initial))
  return (
    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
      <span>Keep for</span>
      <Input
        type="number"
        min={1}
        aria-label="Usage-log retention in days"
        className="h-7 w-14 text-xs text-right"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onBlur={() => {
          const n = parseInt(value)
          if (Number.isFinite(n) && n >= 1) onSave(n)
          else setValue(String(initial))
        }}
        onKeyDown={(e) => { if (e.key === 'Enter') (e.target as HTMLInputElement).blur() }}
      />
      <span>days</span>
    </div>
  )
}

// N-9: hard defaults must match backend ``_USAGE_LOGGING_DEFAULTS`` so the
// fields render with sensible values even before /api/admin/usage-logging
// resolves — the prior implementation set state inside the queryFn body
// and rendered empty strings when openapi-fetch returned ``data`` as
// undefined (route has no declared response_model in the OpenAPI spec).
const _PRICING_DEFAULTS = {
  class_a_rate_per_1k: 0.005,
  class_b_rate_per_10k: 0.01,
  cdn_egress_rate_per_gb: 0.12,
  storage_rate_per_gb_month: 0.02,
  min_billed_days: 30,
}

export const PricingSettings = () => {
  const queryClient = useQueryClient()
  const mounted = useMounted()
  const [saving, setSaving] = useState(false)
  const [editing, setEditing] = useState(false)

  const { data: settings, isLoading } = useQuery({
    queryKey: ['usage-logging-settings'],
    queryFn: async () => {
      const data = await unwrapClient(client.GET('/api/admin/usage-logging'))
      return data ?? null
    },
  })

  // Inputs follow the server values whenever the query resolves (audit finding N-9,
  // 2026-06-10: setting state inside the queryFn raced React's batching and left them empty).
  const serverForm = useMemo(
    () => ({
      rateA: String(settings?.class_a_rate_per_1k ?? _PRICING_DEFAULTS.class_a_rate_per_1k),
      rateB: String(settings?.class_b_rate_per_10k ?? _PRICING_DEFAULTS.class_b_rate_per_10k),
      rateCdn: String(settings?.cdn_egress_rate_per_gb ?? _PRICING_DEFAULTS.cdn_egress_rate_per_gb),
      rateStorage: String(settings?.storage_rate_per_gb_month ?? _PRICING_DEFAULTS.storage_rate_per_gb_month),
      minBilledDays: String(settings?.min_billed_days ?? _PRICING_DEFAULTS.min_billed_days),
    }),
    [settings],
  )
  const [form, setForm] = useDerivedState(serverForm)
  const { rateA, rateB, rateCdn, rateStorage, minBilledDays } = form
  const fieldSetter = (field: keyof typeof form) => (value: string) => setForm((f) => ({ ...f, [field]: value }))
  const setRateA = fieldSetter('rateA')
  const setRateB = fieldSetter('rateB')
  const setRateCdn = fieldSetter('rateCdn')
  const setRateStorage = fieldSetter('rateStorage')
  const setMinBilledDays = fieldSetter('minBilledDays')

  function handleCancel() {
    if (settings) setForm(serverForm)
    setEditing(false)
  }

  async function handleSave() {
    setSaving(true)
    try {
      await unwrapClient(client.PATCH('/api/admin/usage-logging', {
        body: {
          class_a_rate_per_1k: parseFloat(rateA),
          class_b_rate_per_10k: parseFloat(rateB),
          cdn_egress_rate_per_gb: parseFloat(rateCdn),
          storage_rate_per_gb_month: parseFloat(rateStorage),
          min_billed_days: parseInt(minBilledDays),
        },
      }))
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
      queryClient.invalidateQueries({ queryKey: ['usage'] })
      queryClient.invalidateQueries({ queryKey: ['usage-log'] })
      setEditing(false)
    } finally {
      setSaving(false)
    }
  }

  if (!mounted || isLoading) return <AnalyticsCard title="FOS Pricing Defaults" isLoading>{null}</AnalyticsCard>

  const fields = [
    { label: 'Class A Ops ($/1k)', value: rateA, setValue: setRateA },
    { label: 'Class B Ops ($/10k)', value: rateB, setValue: setRateB },
    { label: 'CDN Egress ($/GB)', value: rateCdn, setValue: setRateCdn },
    { label: 'Storage ($/GB/mo)', value: rateStorage, setValue: setRateStorage },
    { label: 'Min. Days Billed/Object', value: minBilledDays, setValue: setMinBilledDays },
  ]

  return (
    <AnalyticsCard
      title="Pricing & Retention Defaults"
      description="Global rates used for cost estimation across all services. Changes apply to all historical views."
      icon={<DollarSign className="h-4 w-4" />}
      headerAction={
        !editing ? (
          <Button size="sm" variant="outline" onClick={() => setEditing(true)} className="h-8 font-bold uppercase tracking-tight">
            <Pencil className="h-3 w-3 mr-1.5" />
            Edit
          </Button>
        ) : null
      }
    >
      <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {fields.map((f) => {
          const inputId = `gs-${f.label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`
          return (
          <div key={f.label} className="space-y-1.5">
            <Label htmlFor={inputId} className="text-[10px] uppercase font-bold text-muted-foreground">{f.label}</Label>
            {editing ? (
              <Input
                id={inputId}
                className="h-8 font-mono text-xs"
                value={f.value}
                onChange={(e) => f.setValue(e.target.value)}
              />
            ) : (
              <div className="h-8 flex items-center font-mono text-xs px-3 rounded-md bg-muted/40 border border-transparent">
                {f.value}
              </div>
            )}
          </div>
          )
        })}
      </div>
      {editing && (
        <div className="flex justify-end gap-2 mt-4">
          <Button size="sm" variant="outline" onClick={handleCancel} disabled={saving} className="h-8 font-bold uppercase tracking-tight">
            Cancel
          </Button>
          <Button size="sm" onClick={handleSave} disabled={saving} className="h-8 font-bold uppercase tracking-tight">
            {saving ? <Loader2 className="h-3 w-3 mr-1.5 animate-spin" /> : <Save className="h-3 w-3 mr-1.5" />}
            Save Changes
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
  const [usageLoggingLoading, setUsageLoggingLoading] = useState(false)
  const [popLocationsOpen, setPopLocationsOpen] = useState(false)

  const { data: queryUsageLoggingSettings } = useQuery({
    queryKey: ['usage-logging-settings'],
    queryFn: async () => {
      return unwrapClient(client.GET('/api/admin/usage-logging'))
    },
    staleTime: 60_000,
  })
  const usageLoggingSettings = mounted ? queryUsageLoggingSettings : undefined

  const usageLoggingEnabled = usageLoggingSettings?.enabled ?? false
  const usageLogRetention = usageLoggingSettings?.retention_days ?? 30

  async function saveUsageLogRetention(days: number) {
    if (!Number.isFinite(days) || days < 1 || days === usageLogRetention) return
    queryClient.setQueryData(['usage-logging-settings'], (old: typeof usageLoggingSettings) => ({ ...old, retention_days: days }))
    try {
      await unwrapClient(client.PATCH('/api/admin/usage-logging', { body: { retention_days: days } }))
    } finally {
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
    }
  }

  async function handleUsageLoggingToggle(enabled: boolean) {
    queryClient.setQueryData(['usage-logging-settings'], (old: typeof usageLoggingSettings) => ({ ...old, enabled }))
    setUsageLoggingLoading(true)
    try {
      await unwrapClient(client.PATCH('/api/admin/usage-logging', { body: { enabled } }))
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
    } catch {
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
    } finally {
      setUsageLoggingLoading(false)
    }
  }

  return (
    <>
      <AnalyticsCard title="Overall Settings" description="Global preferences for the application.">
        <div className="flex flex-col gap-3">
        {/* Compact 2-up grid for the simple toggle/button rows. Each box
            has a fixed shape: title + description block at the top, then a
            right-aligned control strip pinned to the bottom — so the four
            cards line up visually even when the control sets differ in
            width (single Switch vs Switch + inputs + button). Bot
            Intelligence Sources stays full-width below because it embeds
            a data table that would compress poorly in a half-column. */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
          <DiagnosticsPanel />

          <div className="flex flex-col p-3 border rounded-lg gap-3">
            <div className="min-w-0 space-y-0.5">
              <Label id="gs-usage-log-label" className="text-sm font-medium">Log FOS / CDN usage</Label>
              <p className="text-xs text-muted-foreground">
                Records every Class A/B operation and CDN download with function + process context for cost analysis.
              </p>
            </div>
            <div className="flex items-center justify-end gap-2 flex-wrap mt-auto">
              {usageLoggingEnabled && (
                <>
                  <UsageLogRetentionInput initial={usageLogRetention} onSave={saveUsageLogRetention} />
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 text-xs"
                    onClick={() => router.push(buildServiceHref('/admin/usage-log', activeServiceId))}
                  >
                    View Usage Log
                  </Button>
                </>
              )}
              <Switch
                aria-labelledby="gs-usage-log-label"
                checked={usageLoggingEnabled}
                onCheckedChange={handleUsageLoggingToggle}
                disabled={usageLoggingLoading}
              />
            </div>
          </div>

          <div className="flex flex-col p-3 border rounded-lg gap-3">
            <div className="min-w-0 space-y-0.5">
              <Label className="text-sm font-medium">POP location data</Label>
              <p className="text-xs text-muted-foreground">
                Fastly PoP coordinates used by the Impossible Distance insight for geo/RTT spoofing detection.
              </p>
            </div>
            <div className="flex items-center justify-end mt-auto">
              <Button variant="outline" size="sm" onClick={() => setPopLocationsOpen(true)}>
                <MapPin className="h-3.5 w-3.5 mr-1.5" /> Update POP Info
              </Button>
            </div>
          </div>
        </div>

        <BotSourcesPanel />
        </div>
      </AnalyticsCard>

      <PopLocationsModal
        open={popLocationsOpen}
        onOpenChange={setPopLocationsOpen}
      />
    </>
  )
}
