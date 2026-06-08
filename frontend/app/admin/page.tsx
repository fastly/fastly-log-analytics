'use client'
import { AnalyticsCard } from "@/components/AnalyticsCard";
import { SystemHealthCard } from "@/components/SystemHealthCard";

import React, { useState, useMemo } from 'react'
import Link from 'next/link'
import { useQuery, useQueryClient, useMutation } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import type { components } from '@/types/api.generated'
import { useServiceStore } from '@/stores/serviceStore'
import { DataTable } from '@/components/DataTable'
import { ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { ProvisionWizard } from '@/components/ProvisionWizard/ProvisionWizard'
import { PopLocationsModal } from '@/components/PopLocationsModal/PopLocationsModal'
import { TeardownDialog } from '@/components/TeardownDialog'
import { CronSettingsModal } from '@/components/CronSettingsModal/CronSettingsModal'
import { LogSettingsModal } from '@/components/LogSettingsModal/LogSettingsModal'
import { InviteAnalystDialog } from '@/components/InviteAnalystDialog'
import { SSEModal } from '@/components/SSEModal/SSEModal'
import { Button, buttonVariants } from '@/components/ui/button'
import { useRouter } from 'next/navigation'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Plus,
  MapPin,
  Settings,
  Settings2,
  Trash2,
  FileJson,
  ExternalLink,
  ArrowUpDown,
  Play,
  Database,
  CloudDownload,
  UserPlus,
  Bot,
  RefreshCw,
  Wifi,
  Download,
  KeyRound,
  ChevronDown,
  DollarSign,
  Save,
  Loader2,
  Pencil,
  ShieldCheck,
} from 'lucide-react'

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'

import { formatBytes } from '@/lib/utils'
import { useDebugStore } from '@/stores/debugStore'
import { PageHeader } from '@/components/ui/page-header'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useNowMs } from '@/hooks/useNowSeconds'
import { useEffect } from 'react'
import { formatCompactDuration, toUTCDate } from '@/lib/date'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

type ServiceConfig = components["schemas"]["ServiceConfig"]

function SystemJobBox({ job }: { job: any }) {
  const { timeAgo, full, abbr } = useDateFormat()
  const nowMs = useNowMs()

  const lastRunText = job.last_run_at ? timeAgo(job.last_run_at) : 'Never'

  // Pre-fix this had a per-instance setInterval(compute, 1000) that
  // re-rendered every box every second. On a 10-cron page that's 10
  // independent timers firing on the same 1s boundary, each forcing a
  // setState — the main thread was constantly busy and clicks queued
  // behind the cascade ("admin page takes 2 seconds to respond").
  // Now we derive nextRunText on-render from useNowMs() (a single
  // shared global ticker). Same UX, ~10x fewer timers + state updates.
  const nextRunText = job.next_run_at
    ? formatCompactDuration(Math.floor((toUTCDate(job.next_run_at).getTime() - nowMs) / 1000))
    : 'Disabled'

  const isError = job.status === 'error'
  const borderColor = isError ? 'border-destructive/50' : 'border-muted'
  const bgColor = isError ? 'bg-destructive/10' : 'bg-muted/20'

  return (
    <div className={`relative flex flex-col justify-center border rounded-md px-2.5 h-8 shrink-0 ${bgColor} ${borderColor} min-w-[250px] max-w-[320px] flex-1`}>
      <div className="flex items-center gap-2 w-full">
        <TooltipProvider delay={200}>
          <Tooltip>
            <TooltipTrigger render={<span className={`text-[9px] font-bold uppercase tracking-wider shrink-0 truncate max-w-[120px] ${isError ? 'text-destructive' : 'text-muted-foreground'}`} />}>
              {job.name}
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-[250px] text-xs">
              {job.detail || job.name}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
        <div className="w-px h-4 bg-border shrink-0" />
        <div className={`flex-1 min-w-0 flex items-center justify-between text-[9px] whitespace-nowrap ${isError ? 'text-destructive/80' : 'text-muted-foreground'}`}>
          <TooltipProvider delay={200}>
            <Tooltip>
              <TooltipTrigger render={<span className="truncate pr-2 " />}>
                Last: {lastRunText}
              </TooltipTrigger>
              <TooltipContent className="text-xs">
                {job.last_run_at ? `${full(job.last_run_at)} ${abbr()}` : 'Never'}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
          <TooltipProvider delay={200}>
            <Tooltip>
              <TooltipTrigger render={<span className="truncate " />}>
                Next: {nextRunText}
              </TooltipTrigger>
              <TooltipContent className="text-xs">
                {job.next_run_at ? `${full(job.next_run_at)} ${abbr()}` : 'Disabled'}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      </div>
    </div>
  )
}

function UsageLogRetentionInput({ initial, onSave }: { initial: number; onSave: (days: number) => void }) {
  const [value, setValue] = useState(String(initial))
  useEffect(() => { setValue(String(initial)) }, [initial])
  return (
    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
      <span>Keep for</span>
      <Input
        type="number"
        min={1}
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

const PricingSettings = () => {
  const queryClient = useQueryClient()
  const [saving, setSaving] = useState(false)
  const [editing, setEditing] = useState(false)
  const [rateA, setRateA] = useState('')
  const [rateB, setRateB] = useState('')
  const [rateCdn, setRateCdn] = useState('')
  const [rateStorage, setRateStorage] = useState('')
  const [minBilledDays, setMinBilledDays] = useState('')

  const { data: settings, isLoading } = useQuery({
    queryKey: ['usage-logging-settings'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/admin/usage-logging')
      if (!data) return null
      const d = data as any
      setRateA(String(d.class_a_rate_per_1k ?? 0.005))
      setRateB(String(d.class_b_rate_per_10k ?? 0.01))
      setRateCdn(String(d.cdn_egress_rate_per_gb ?? 0.12))
      setRateStorage(String(d.storage_rate_per_gb_month ?? 0.02))
      setMinBilledDays(String(d.min_billed_days ?? 30))
      return d
    },
  })

  function handleCancel() {
    if (settings) {
      setRateA(String(settings.class_a_rate_per_1k ?? 0.005))
      setRateB(String(settings.class_b_rate_per_10k ?? 0.01))
      setRateCdn(String(settings.cdn_egress_rate_per_gb ?? 0.12))
      setRateStorage(String(settings.storage_rate_per_gb_month ?? 0.02))
      setMinBilledDays(String(settings.min_billed_days ?? 30))
    }
    setEditing(false)
  }

  async function handleSave() {
    setSaving(true)
    try {
      await client.PATCH('/api/admin/usage-logging', {
        body: {
          class_a_rate_per_1k: parseFloat(rateA),
          class_b_rate_per_10k: parseFloat(rateB),
          cdn_egress_rate_per_gb: parseFloat(rateCdn),
          storage_rate_per_gb_month: parseFloat(rateStorage),
          min_billed_days: parseInt(minBilledDays),
        } as any,
      })
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
      queryClient.invalidateQueries({ queryKey: ['usage'] })
      queryClient.invalidateQueries({ queryKey: ['usage-log'] })
      setEditing(false)
    } finally {
      setSaving(false)
    }
  }

  if (isLoading) return <AnalyticsCard title="FOS Pricing Defaults" isLoading>{null}</AnalyticsCard>

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
        {fields.map((f) => (
          <div key={f.label} className="space-y-1.5">
            <Label className="text-[10px] uppercase font-bold text-muted-foreground">{f.label}</Label>
            {editing ? (
              <Input
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
        ))}
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

function RebuildLocalViewButton() {
  const [busy, setBusy] = useState(false)
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [error, setError] = useState<string | null>(null)
  async function rebuild() {
    setBusy(true)
    setError(null)
    try {
      const { error: apiError } = await client.POST('/api/admin/rebuild-local-view', {})
      if (apiError) throw new Error(extractApiError(apiError))
      setConfirmOpen(false)
    } catch (e: any) {
      setError(e?.message ?? 'rebuild failed')
    } finally {
      setBusy(false)
    }
  }
  return (
    <>
      <Button variant="outline" size="sm" onClick={() => setConfirmOpen(true)}>
        <CloudDownload className="h-3 w-3 mr-1.5" />
        Rebuild Local View
      </Button>
      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rebuild local view from cloud?</DialogTitle>
            <DialogDescription>
              Clears local Iceberg caches and re-pulls metadata + parquet from FOS via CDN.
              Un-committed buffer data is preserved. This can take a minute on large tables.
            </DialogDescription>
          </DialogHeader>
          {error && <div className="text-xs text-red-500">{error}</div>}
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmOpen(false)} disabled={busy}>
              Cancel
            </Button>
            <Button onClick={rebuild} disabled={busy}>
              {busy ? <Loader2 className="h-3 w-3 mr-1.5 animate-spin" /> : <CloudDownload className="h-3 w-3 mr-1.5" />}
              {busy ? 'Starting…' : 'Rebuild'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}

export default function AdminPage() {
  const queryClient = useQueryClient()
  const { activeServiceId, setActiveServiceId } = useServiceStore()
  const { enabled: debugEnabled, setEnabled: setDebugEnabled, apiCallsEnabled, setApiCallsEnabled } = useDebugStore()
  const router = useRouter()
  const [usageLoggingLoading, setUsageLoggingLoading] = useState(false)
  const [cronService, setCronService] = useState<ServiceConfig | null>(null)
  const [settingsService, setSettingsService] = useState<ServiceConfig | null>(null)
  const [teardownService, setTeardownService] = useState<ServiceConfig | null>(null)
  const [inviteService, setInviteService] = useState<ServiceConfig | null>(null)
  const [credentialsService, setCredentialsService] = useState<ServiceConfig | null>(null)
  const [credMode, setCredMode] = useState<'token' | 'manual'>('token')
  const [credApiToken, setCredApiToken] = useState('')
  const [credAccessKey, setCredAccessKey] = useState('')
  const [credSecretKey, setCredSecretKey] = useState('')
  const [wizardOpen, setWizardOpen] = useState(false)
  const [popLocationsOpen, setPopLocationsOpen] = useState(false)
  const [refreshingSource, setRefreshingSource] = useState<string | null>(null)
  const [ngwafService, setNgwafService] = useState<ServiceConfig | null>(null)
  const [ngwafWorkspaceId, setNgwafWorkspaceId] = useState('')
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<{ id: string; name: string }[]>([])
  const [ngwafFetchError, setNgwafFetchError] = useState('')
  const [ngwafFetching, setNgwafFetching] = useState(false)
  const [ngwafSaving, setNgwafSaving] = useState(false)
  const [ngwafSaved, setNgwafSaved] = useState(false)
  // Security: backend now requires a caller-supplied Fastly token for
  // the PATCH that rebinds the workspace. The admin enters the same token
  // they use to fetch the workspaces list, so the constant-time stored-key
  // match in the backend lets through the legitimate admin flow without
  // requiring them to remember it from somewhere else.
  const [ngwafApiToken, setNgwafApiToken] = useState('')

  function openCredentials(service: ServiceConfig) {
    setCredentialsService(service)
    setCredMode(service.access_level === 'read_write' ? 'token' : 'manual')
    setCredApiToken('')
    setCredAccessKey('')
    setCredSecretKey('')
  }

  function closeCredentials() {
    setCredentialsService(null)
    credentialsMutation.reset()
  }

  const credentialsMutation = useMutation({
    mutationFn: async ({ service_id, payload }: { service_id: string; payload: { api_token: string } | { access_key: string; secret_key: string } }) => {
      const { data } = await client.PATCH("/api/services/{service_id}/credentials", {
        params: { path: { service_id } },
        body: payload as any
      })
      return data
    },
    onSuccess: () => {
      setCredentialsService(null)
    },
  })

  const { data: services, isLoading } = useQuery({
    queryKey: ['services'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/services", { signal })
      return data
    },
  })

  const { data: botSourcesData, refetch: refetchBotSources } = useQuery({
    queryKey: ['bot-sources'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/admin/bot-sources", { signal })
      return data as any
    },
    staleTime: 60_000,
  })

  // Backend gate for the two "Show ... panel" toggles below. The frontend
  // panels render data from response.`_debug_queries` / `_debug_calls` —
  // when DEBUG_RESPONSES=false on the server (the prod default per the
  // 2026 security hardening) those arrays are stripped and the panel
  // shows nothing. Surface that so the toggle doesn't silently lie.
  const { data: debugState } = useQuery({
    queryKey: ['debug-state'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/debug/state' as any, { signal, } as any)
      return data as { debug_responses_enabled: boolean }
    },
    staleTime: 5 * 60_000, // env doesn't change without a restart
  })
  // Default to "enabled" on first paint so the toggle isn't briefly dimmed
  // before the query resolves. Only mark disabled when we have a real
  // false from the backend.
  const debugBackendOn = debugState?.debug_responses_enabled !== false
  const debugDisabledTooltip = !debugBackendOn
    ? 'Backend debug responses are disabled — set DEBUG_RESPONSES=true in the server env (or .env file) and restart to see data here.'
    : undefined

  const { data: systemJobsData, refetch: refetchSystemJobs } = useQuery({
    queryKey: ['system-jobs'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/admin/system-jobs", { signal })
      return data as any
    },
    staleTime: 30_000,
    refetchInterval: 30_000,
  })

  const { data: usageLoggingSettings } = useQuery({
    queryKey: ['usage-logging-settings'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/admin/usage-logging')
      return data
    },
    staleTime: 60_000,
  })

  const usageLoggingEnabled = (usageLoggingSettings as any)?.enabled ?? false
  const usageLogRetention = (usageLoggingSettings as any)?.retention_days ?? 30

  async function saveUsageLogRetention(days: number) {
    if (!Number.isFinite(days) || days < 1 || days === usageLogRetention) return
    queryClient.setQueryData(['usage-logging-settings'], (old: any) => ({ ...old, retention_days: days }))
    try {
      await client.PATCH('/api/admin/usage-logging', { body: { retention_days: days } as any })
    } finally {
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
    }
  }

  async function handleUsageLoggingToggle(enabled: boolean) {
    queryClient.setQueryData(['usage-logging-settings'], (old: any) => ({ ...old, enabled }))
    setUsageLoggingLoading(true)
    try {
      await client.PATCH('/api/admin/usage-logging', { body: { enabled } as any })
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
    } catch {
      queryClient.invalidateQueries({ queryKey: ['usage-logging-settings'] })
    } finally {
      setUsageLoggingLoading(false)
    }
  }

  async function handleRefreshBotSource(sourceId: string) {
    setRefreshingSource(sourceId)
    try {
      await client.POST("/api/admin/bot-sources/{source_id}/refresh", {
        params: { path: { source_id: sourceId } }
      })
      await refetchBotSources()
    } finally {
      setRefreshingSource(null)
    }
  }

  function fmtRelative(iso: string | null | undefined): string {
    if (!iso) return '—'
    const diff = Date.now() - new Date(iso).getTime()
    const mins = Math.floor(diff / 60_000)
    if (mins < 2) return 'just now'
    if (mins < 60) return `${mins}m ago`
    const hrs = Math.floor(mins / 60)
    if (hrs < 24) return `${hrs}h ago`
    return `${Math.floor(hrs / 24)}d ago`
  }

  const columns: ColumnDef<ServiceConfig>[] = React.useMemo(() => [
    {
      accessorKey: 'name',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent text-xs">
          Service Name
          <ArrowUpDown className="ml-2 h-3 w-3" />
        </Button>
      ),
      size: 200,
      cell: ({ row }) => (
        <div className="flex items-center gap-2 font-medium">
          {row.original.service_id === activeServiceId && (
            <Badge variant="default" className="h-5 px-1.5 text-[10px] uppercase font-bold bg-blue-500 hover:bg-blue-600 shadow-none border-none">Active</Badge>
          )}
          {row.getValue('name')}
        </div>
      )
    },
    {
      accessorKey: 'service_id',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent text-xs">
          ID
          <ArrowUpDown className="ml-2 h-3 w-3" />
        </Button>
      ),
      size: 160,
      cell: ({ row }) => {
        const id = row.getValue('service_id') as string;
        return (
          <div className="flex items-center gap-1.5">
            <span className="font-mono text-xs text-muted-foreground">{id}</span>
            <a 
              href={`https://manage.fastly.com/configure/services/${id}`}
              target="_blank"
              rel="noreferrer"
              className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity"
              title="View Service in Fastly"
            >
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
        )
      }
    },
    {
      accessorKey: 'fos_bucket',
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent text-xs">
          FOS Bucket
          <ArrowUpDown className="ml-2 h-3 w-3" />
        </Button>
      ),
      size: 180,
      cell: ({ row }) => {
        const bucket = row.getValue('fos_bucket') as string;
        return (
          <div className="flex items-center gap-1.5">
            <span className="font-mono text-xs text-muted-foreground">{bucket}</span>
            <a 
              href="https://manage.fastly.com/resources/object-storage/buckets"
              target="_blank"
              rel="noreferrer"
              className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity"
              title="View Object Storage in Fastly"
            >
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
        )
      }
    },
    {
      id: 'local_cache',
      accessorFn: (row) => row.duckdb_size_bytes,
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent text-xs">
          Local Cache
          <ArrowUpDown className="ml-2 h-3 w-3" />
        </Button>
      ),
      size: 180,
      cell: ({ row }) => {
        const size = row.original.duckdb_size_bytes
        const files = row.original.cache_file_count || 0
        const rows = row.original.log_row_count || 0
        
        return size ? (
          <div className="flex flex-col gap-0.5">
            <span className="font-mono text-xs tabular-nums text-muted-foreground">{formatBytes(size)}</span>
            <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground/70">
              <span className="flex items-center gap-1">
                {files} file{files === 1 ? '' : 's'}
                {files > 0 && (
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-4 w-4 p-0 ml-0.5 hover:bg-transparent text-muted-foreground hover:text-primary transition-colors"
                    onClick={(e) => {
                      e.stopPropagation()
                      window.open(`/api/download-all?service_id=${row.original.service_id}&include=local`, '_blank')
                    }}
                    title="Download local cache as ZIP"
                  >
                    <CloudDownload className="h-3 w-3" />
                  </Button>
                )}
              </span>
              <span>•</span>
              <span>{rows.toLocaleString()} rows</span>
            </div>
          </div>
        ) : (
          <span className="text-xs text-muted-foreground italic">No cache</span>
        )
      }
    },
    {
      id: 'status',
      accessorFn: (row) => row.cron_sync?.enabled ? 1 : 0,
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent text-xs">
          Cron Sync
          <ArrowUpDown className="ml-2 h-3 w-3" />
        </Button>
      ),
      size: 140,
      cell: ({ row }) => {
        const service = row.original
        const cron = service.cron_sync
        return (
          <div className="flex items-center gap-2">
            {cron?.enabled ? (
              <Badge variant="success" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Enabled</Badge>
            ) : (
              <Badge variant="secondary" className="px-1.5 py-0 shadow-none uppercase text-[10px] font-bold opacity-40">Disabled</Badge>
            )}
            <Button 
              variant="ghost" 
              size="icon" 
              className="h-6 w-6 hover:bg-muted cursor-pointer" 
              onClick={() => setCronService(service)}
              title="Cron Sync Settings"
            >
              <Settings className="h-3 w-3 text-muted-foreground" />
            </Button>
          </div>
        )
      }
    },
    {
      id: 'actions',
      header: 'Actions',
      size: (services?.services?.length || 0) > 0 ? 780 : 120,
      cell: ({ row }) => {
        const service = row.original
        const isActive = service.service_id === activeServiceId
        
        return (
          <div className="flex items-center gap-2">
            {/* Desktop View */}
            <div className="hidden xl:flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                className="h-8 w-32 text-[11px] font-bold uppercase tracking-tight"
                onClick={() => setSettingsService(service)}
              >
                <Settings2 className="h-3 w-3 mr-1.5" /> Log Settings
              </Button>

              <Button
                variant="outline"
                size="sm"
                className="h-8 w-24 text-[11px] font-bold uppercase tracking-tight"
                onClick={() => {
                  // Security: workspace fetch now requires a token,
                  // so we open the dialog empty and the admin pastes
                  // their token + clicks Load Workspaces.
                  setNgwafService(service)
                  setNgwafWorkspaceId(service.ngwaf_workspace_id || '')
                  setNgwafWorkspaces([])
                  setNgwafFetchError('')
                  setNgwafSaved(false)
                  setNgwafApiToken('')
                }}
                title={service.ngwaf_workspace_id ? `NGWAF: ${service.ngwaf_workspace_id}` : 'Configure NGWAF workspace'}
              >
                <Bot className="h-3 w-3 mr-1.5" /> NGWAF
              </Button>

              {service.access_level === 'read_write' && (
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 w-24 text-[11px] font-bold uppercase tracking-tight"
                  onClick={() => setInviteService(service)}
                >
                  <UserPlus className="h-3 w-3 mr-1.5" /> Invite
                </Button>
              )}

              <Button
                variant="outline"
                size="sm"
                className="h-8 w-28 text-[11px] font-bold uppercase tracking-tight"
                onClick={() => openCredentials(service)}
                title="Update FOS access credentials"
              >
                <KeyRound className="h-3 w-3 mr-1.5" /> Rotate Key
              </Button>

              <Button
                variant="outline"
                size="sm"
                className="h-8 w-28 text-[11px] font-bold uppercase tracking-tight border-destructive/50 text-destructive hover:bg-destructive hover:text-white"
                onClick={() => setTeardownService(service)}
              >
                <Trash2 className="h-3 w-3 mr-1.5" /> Teardown
              </Button>
              {!isActive && (
                <Button
                  variant="default"
                  size="sm"
                  className="h-8 w-[105px] text-[11px] font-bold uppercase tracking-tight bg-primary hover:bg-primary/90"
                  onClick={() => {
                    setActiveServiceId(service.service_id)
                    router.push(`/dashboard?service=${service.service_id}`)
                  }}
                >
                  <Play className="h-3 w-3 mr-1.5 fill-current" /> Switch to
                </Button>
              )}
            </div>

            {/* Mobile / Tablet View (Dropdown) */}
            <div className="xl:hidden">
              <DropdownMenu>
                <DropdownMenuTrigger render={
                  <Button variant="outline" size="sm" className="h-8 gap-1.5 px-3 font-bold uppercase text-[10px] tracking-wider">
                    Actions <ChevronDown className="h-3.5 w-3.5" />
                  </Button>
                } />
                <DropdownMenuContent align="end" className="w-52">
                  {!isActive && (
                    <DropdownMenuItem onClick={() => {
                      setActiveServiceId(service.service_id)
                      router.push(`/dashboard?service=${service.service_id}`)
                    }}>
                      <Play className="mr-2 h-4 w-4" /> Switch to Service
                    </DropdownMenuItem>
                  )}
                  <DropdownMenuItem onClick={() => setSettingsService(service)}>
                    <Settings2 className="mr-2 h-4 w-4" /> Log Settings
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={() => {
                    // Security: open empty; user pastes token + clicks Load Workspaces.
                    setNgwafService(service)
                    setNgwafWorkspaceId(service.ngwaf_workspace_id || '')
                    setNgwafWorkspaces([])
                    setNgwafFetchError('')
                    setNgwafSaved(false)
                    setNgwafApiToken('')
                  }}>
                    <Bot className="mr-2 h-4 w-4" /> NGWAF Config
                  </DropdownMenuItem>
                  {service.access_level === 'read_write' && (
                    <DropdownMenuItem onClick={() => setInviteService(service)}>
                      <UserPlus className="mr-2 h-4 w-4" /> Invite User
                    </DropdownMenuItem>
                  )}
                  <DropdownMenuItem onClick={() => openCredentials(service)}>
                    <KeyRound className="mr-2 h-4 w-4" /> Rotate Key
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem 
                    className="text-destructive focus:text-destructive focus:bg-destructive/10"
                    onClick={() => setTeardownService(service)}
                  >
                    <Trash2 className="mr-2 h-4 w-4" /> Teardown Service
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </div>
        )
      }
    }
  ], [activeServiceId, setActiveServiceId, router, services?.services?.length])

  return (
    <div className="space-y-6">
      <PageHeader
        title="Admin"
        description="Manage your global settings, Fastly services, and log ingestion pipelines."
      >
        {/* Navigation chips for sibling admin pages. These used to live
            next to the "Add Service" button in the Service Management
            section, which conflated "act on this service list" with
            "go somewhere else" — and the cluster of three buttons made
            it ambiguous which one performed the destructive action.
            Moving them up to the PageHeader's action slot establishes
            "here's where you switch between admin sub-pages" as a
            top-of-page navigation pattern. */}
        {/* `secondary` variant gives these a visible filled background so
            they read as obviously-clickable nav buttons on a white page.
            The previous `outline` variant rendered as white-on-white and
            only revealed itself on hover, making the slot look empty. */}
        <Link
          href="/admin/share"
          prefetch={true}
          onMouseEnter={() => {
            // Warm the share-status query so by the time the click
            // resolves, /admin/share's useQuery hits a fresh cache
            // entry instead of paying a ~300ms fetch round-trip.
            // staleTime=5s on the destination's useQuery means the
            // prefetched payload counts as fresh for the click that
            // immediately follows.
            queryClient.prefetchQuery({
              queryKey: ['admin', 'share', 'status'],
              queryFn: async ({ signal }) => {
                const { data, response } = await client.GET('/api/admin/share/status' as any, { signal, })
                if (!response.ok) throw new Error(`status ${response.status}`)
                return data
              },
            })
          }}
          data-testid="open-share-dialog"
          className={buttonVariants({ variant: 'secondary', size: 'sm' })}
        >
          <UserPlus className="h-4 w-4 mr-1" /> Share Dashboard
        </Link>
        <Link
          href="/admin/session-scoring"
          prefetch={true}
          onMouseEnter={() => {
            if (!activeServiceId) return
            queryClient.prefetchQuery({
              queryKey: ['scoring-status', activeServiceId],
              queryFn: async ({ signal }) => {
                const { data } = await client.GET(
                  '/api/services/{service_id}/scoring/status',
                  { params: { path: { service_id: activeServiceId } } },
                )
                return data
              },
              staleTime: 20_000,
            })
          }}
          className={buttonVariants({ variant: 'secondary', size: 'sm' })}
        >
          <ShieldCheck className="h-4 w-4 mr-1" /> Session Scoring
        </Link>
      </PageHeader>

      <div className="space-y-4">
        <div className="flex items-center gap-4">
          <h2 className="text-xl font-semibold tracking-tight">Service Management</h2>
          <Button size="sm" onClick={() => setWizardOpen(true)}>
            <Plus className="h-4 w-4 mr-1" /> Add Service
          </Button>
        </div>

        <div className="border rounded-lg bg-card shadow-sm overflow-hidden">
          <DataTable
            columns={columns}
            data={services?.services || []}
            isLoading={isLoading}
            searchKey="name"
          />
        </div>
      </div>

      <SystemHealthCard />

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
          <div className={`flex flex-col p-3 border rounded-lg gap-3 ${!debugBackendOn ? 'opacity-60' : ''}`}>
            <div className="min-w-0 space-y-0.5">
              <Label className="text-sm font-medium">Query debugging panel</Label>
              <p className="text-xs text-muted-foreground">
                Bottom-of-screen panel with DuckDB SQL queries and execution times.
              </p>
              {!debugBackendOn && (
                <p className="text-[11px] text-amber-500" title={debugDisabledTooltip}>
                  Disabled — backend ``DEBUG_RESPONSES`` env is off.
                </p>
              )}
            </div>
            <div className="flex items-center justify-end mt-auto" title={debugDisabledTooltip}>
              <Switch
                checked={debugEnabled}
                onCheckedChange={setDebugEnabled}
                disabled={!debugBackendOn}
              />
            </div>
          </div>

          <div className={`flex flex-col p-3 border rounded-lg gap-3 ${!debugBackendOn ? 'opacity-60' : ''}`}>
            <div className="min-w-0 space-y-0.5">
              <Label className="text-sm font-medium">API call panel</Label>
              <p className="text-xs text-muted-foreground">
                Bottom-of-screen panel with all Fastly API calls and FOS operations per request.
              </p>
              {!debugBackendOn && (
                <p className="text-[11px] text-amber-500" title={debugDisabledTooltip}>
                  Disabled — backend ``DEBUG_RESPONSES`` env is off.
                </p>
              )}
            </div>
            <div className="flex items-center justify-end mt-auto" title={debugDisabledTooltip}>
              <Switch
                checked={apiCallsEnabled}
                onCheckedChange={setApiCallsEnabled}
                disabled={!debugBackendOn}
              />
            </div>
          </div>

          <div className="flex flex-col p-3 border rounded-lg gap-3">
            <div className="min-w-0 space-y-0.5">
              <Label className="text-sm font-medium">Log FOS / CDN usage</Label>
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
                    onClick={() => router.push('/admin/usage-log')}
                  >
                    View Usage Log
                  </Button>
                </>
              )}
              <Switch
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

        {/* Bot Intelligence Sources */}
        <div className="p-4 border rounded-lg space-y-4">
          <div className="flex items-center gap-2">
            <Bot className="h-4 w-4 text-muted-foreground" />
            <Label className="text-sm font-medium">Bot Intelligence Sources</Label>
          </div>
          <p className="text-xs text-muted-foreground -mt-2">
            Known bot registries used to identify and verify bots in log traffic via UA matching and FCrDNS validation.
          </p>

          {/* Sources table */}
          <div className="border rounded-md overflow-hidden text-sm">
            <table className="w-full">
              <thead className="bg-muted/40">
                <tr>
                  <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Source</th>
                  <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Entries</th>
                  <th className="text-right px-3 py-2 text-xs font-medium text-muted-foreground">Last Updated</th>
                  <th className="px-3 py-2" />
                </tr>
              </thead>
              <tbody>
                {(botSourcesData?.sources ?? []).map((src: any) => (
                  <tr key={src.id} className="border-t">
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-1.5">
                        <span className="font-medium">{src.name}</span>
                        {src.url && (
                          <a href={src.url} target="_blank" rel="noreferrer" className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity" title={`View source: ${src.url}`}>
                            <ExternalLink className="h-3 w-3" />
                          </a>
                        )}
                      </div>
                      {!src.last_updated && (
                        <span className="text-xs text-amber-500 block mt-0.5">not cached</span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                      {src.entry_count?.toLocaleString() ?? '—'}
                    </td>
                    <td className="px-3 py-2 text-right text-muted-foreground">
                      {fmtRelative(src.last_updated)}
                    </td>
                    <td className="px-3 py-2 text-right">
                      <Button
                        variant="outline" size="sm"
                        disabled={refreshingSource === src.id}
                        onClick={() => handleRefreshBotSource(src.id)}
                      >
                        <RefreshCw className={`h-3 w-3 mr-1.5 ${refreshingSource === src.id ? 'animate-spin' : ''}`} />
                        Refresh
                      </Button>
                    </td>
                  </tr>
                ))}
                {!botSourcesData && (
                  <tr><td colSpan={4} className="px-3 py-3 text-center text-xs text-muted-foreground">Loading…</td></tr>
                )}
              </tbody>
            </table>
          </div>

          {/* rDNS cache stats */}
          <div className="flex items-center justify-between text-sm">
            <div className="flex items-center gap-4 text-muted-foreground text-xs">
              <span className="flex items-center gap-1.5">
                <Wifi className="h-3.5 w-3.5" />
                rDNS cache: <strong className="text-foreground">{botSourcesData?.rdns.total.toLocaleString() ?? '—'}</strong> IPs
              </span>
              <span>
                Pending: <strong className="text-foreground">{botSourcesData?.rdns.pending.toLocaleString() ?? '—'}</strong>
              </span>
              <span>Last enrichment: {fmtRelative(botSourcesData?.rdns.last_enrichment_at ?? null)}</span>
            </div>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={() => window.open('/api/admin/rdns/export', '_blank')} title="Download rDNS Cache as CSV">
                <Download className="h-3 w-3 mr-1.5" />
                Export CSV
              </Button>
              <SSEModal
                title="Enrich rDNS Cache"
                description={
                  <div className="space-y-2">
                    <p>This will start a manual enrichment batch for the reverse DNS cache.</p>
                    <p className="text-muted-foreground">It will resolve pending IPs and attempt to discover new IPs from your DuckDB log sources.</p>
                  </div>
                }
                endpoint="/api/admin/bot-sources/rdns/enrich"
                body={{}}
                onClose={() => queryClient.invalidateQueries({ queryKey: ['bot-sources'] })}
                trigger={
                  <Button variant="outline" size="sm">
                    <RefreshCw className="h-3 w-3 mr-1.5" />
                    Enrich Now
                  </Button>
                }
              />
              <SSEModal
                title="Seed rDNS Backfill"
                description={
                  <div className="space-y-2">
                    <p>This will scan all log sources for the last 30 days to seed the rDNS cache.</p>
                    <p className="text-muted-foreground text-xs italic">Note: This only enqueues IPs for later resolution. It does not perform lookups immediately.</p>
                  </div>
                }
                endpoint="/api/admin/bot-sources/rdns/backfill"
                body={{}}
                onClose={() => queryClient.invalidateQueries({ queryKey: ['bot-sources'] })}
                trigger={
                  <Button variant="outline" size="sm">
                    <Database className="h-3 w-3 mr-1.5" />
                    Seed Backfill
                  </Button>
                }
              />
            </div>

          </div>

          {/* Maintenance */}
          <div className="space-y-3 pt-2">
            <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Maintenance</Label>
            <div className="flex flex-wrap gap-2">
              <RebuildLocalViewButton />
            </div>
            <p className="text-[11px] text-muted-foreground">
              Drops local caches and re-pulls Iceberg metadata + parquet from FOS via CDN. The local buffer (un-committed data) is left alone.
            </p>
          </div>

          {/* System jobs */}
          <div className="space-y-3 pt-2">
            <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Background Jobs</Label>
            <div className="flex flex-wrap gap-2">
              {(systemJobsData?.jobs ?? []).map((job: any) => (
                <SystemJobBox key={job.id} job={job} />
              ))}
              {!systemJobsData && (
                <div className="text-xs text-muted-foreground italic px-1 py-1">Loading background jobs...</div>
              )}
            </div>
          </div>
        </div>
        </div>
      </AnalyticsCard>

      <PricingSettings />

      <ProvisionWizard
        open={wizardOpen}
        onOpenChange={setWizardOpen}
      />

      <PopLocationsModal
        open={popLocationsOpen}
        onOpenChange={setPopLocationsOpen}
      />

      {cronService && (
        <CronSettingsModal 
          service={cronService} 
          open={!!cronService} 
          onOpenChange={(open) => !open && setCronService(null)} 
        />
      )}

      {settingsService && (
        <LogSettingsModal 
          service={settingsService}
          open={!!settingsService} 
          onOpenChange={(open) => !open && setSettingsService(null)} 
        />
      )}

      <InviteAnalystDialog
        service={inviteService}
        open={!!inviteService}
        onOpenChange={(open) => !open && setInviteService(null)}
      />

      {/* Teardown Dialog */}
      <TeardownDialog
        service={teardownService}
        open={!!teardownService}
        onOpenChange={(open) => !open && setTeardownService(null)}
        onComplete={() => {
          queryClient.invalidateQueries({ queryKey: ['services'] })
          queryClient.invalidateQueries({ queryKey: ['bootstrap'] })
          setTeardownService(null)
        }}
      />

      {/* Rotate FOS Credentials Dialog */}
      <Dialog open={!!credentialsService} onOpenChange={(open) => { if (!open) closeCredentials() }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Rotate FOS Credentials</DialogTitle>
            <DialogDescription>
              Replace the Fastly Object Storage access key for <strong>{credentialsService?.name}</strong>.
              {credentialsService?.access_level === 'read_write'
                ? ' Use your Fastly API token to auto-generate a new key, or enter one manually.'
                : ' Enter the new key credentials manually.'}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            {/* Mode toggle — admins only */}
            {credentialsService?.access_level === 'read_write' && (
              <div className="flex rounded-md border overflow-hidden text-xs font-semibold">
                <button
                  type="button"
                  className={`flex-1 py-1.5 transition-colors ${credMode === 'token' ? 'bg-primary text-primary-foreground' : 'hover:bg-muted text-muted-foreground'}`}
                  onClick={() => { setCredMode('token'); credentialsMutation.reset() }}
                >
                  Auto (API Token)
                </button>
                <button
                  type="button"
                  className={`flex-1 py-1.5 transition-colors ${credMode === 'manual' ? 'bg-primary text-primary-foreground' : 'hover:bg-muted text-muted-foreground'}`}
                  onClick={() => { setCredMode('manual'); credentialsMutation.reset() }}
                >
                  Manual
                </button>
              </div>
            )}

            {/* Token mode */}
            {credMode === 'token' && credentialsService?.access_level === 'read_write' && (
              <div className="space-y-1.5">
                <Label htmlFor="cred-api-token" className="text-sm">Fastly API Token</Label>
                <p className="text-xs text-muted-foreground">
                  A new <code>read-write-objects</code> FOS key will be created for this bucket. The old key will be deleted automatically.
                </p>
                <Input
                  id="cred-api-token"
                  type="password"
                  placeholder="Fastly API token"
                  value={credApiToken}
                  onChange={(e) => setCredApiToken(e.target.value)}
                  className="font-mono text-sm"
                />
              </div>
            )}

            {/* Manual mode */}
            {(credMode === 'manual' || credentialsService?.access_level !== 'read_write') && (
              <>
                <div className="space-y-1.5">
                  <Label htmlFor="cred-access-key" className="text-sm">Access Key ID</Label>
                  <Input
                    id="cred-access-key"
                    placeholder="FOS access key ID"
                    value={credAccessKey}
                    onChange={(e) => setCredAccessKey(e.target.value)}
                    className="font-mono text-sm"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="cred-secret-key" className="text-sm">Secret Access Key</Label>
                  <Input
                    id="cred-secret-key"
                    type="password"
                    placeholder="FOS secret access key"
                    value={credSecretKey}
                    onChange={(e) => setCredSecretKey(e.target.value)}
                    className="font-mono text-sm"
                  />
                </div>
              </>
            )}

            {credentialsMutation.isError && (
              <p className="text-sm text-destructive">
                {(credentialsMutation.error as any)?.message ?? 'Failed to update credentials.'}
              </p>
            )}
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={closeCredentials}>Cancel</Button>
            <Button
              disabled={
                credentialsMutation.isPending ||
                (credMode === 'token' ? !credApiToken : !credAccessKey || !credSecretKey)
              }
              onClick={() => {
                if (!credentialsService) return
                const payload = credMode === 'token'
                  ? { api_token: credApiToken }
                  : { access_key: credAccessKey, secret_key: credSecretKey }
                credentialsMutation.mutate({ service_id: credentialsService.service_id, payload })
              }}
            >
              {credentialsMutation.isPending
                ? (credMode === 'token' ? 'Creating key…' : 'Validating…')
                : (credMode === 'token' ? 'Rotate Key' : 'Save Credentials')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* NGWAF Workspace Dialog */}
      <Dialog open={!!ngwafService} onOpenChange={(open) => { if (!open) setNgwafService(null) }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Bot className="h-5 w-5 text-primary" />
              NGWAF Bot Enrichment
            </DialogTitle>
            <DialogDescription>
              Set the NGWAF workspace for <strong>{ngwafService?.name}</strong>. When configured, the bot sync cron will enrich log data with specific bot names from Fastly NGWAF.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            {/* Security: token must be supplied before workspace fetch
                AND before workspace save. Single input drives both. */}
            {ngwafService && !ngwafSaved && (
              <div className="space-y-1">
                <Label htmlFor="ngwaf-api-token" className="text-xs font-semibold">
                  Fastly API token
                </Label>
                <p className="text-[10px] text-muted-foreground">
                  Required to list AND save NGWAF workspace bindings (security /).
                </p>
                <div className="flex gap-2">
                  <Input
                    id="ngwaf-api-token"
                    type="password"
                    placeholder="Fastly API token"
                    value={ngwafApiToken}
                    onChange={(e) => setNgwafApiToken(e.target.value)}
                    className="h-8 font-mono text-xs flex-1"
                    autoComplete="off"
                  />
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={!ngwafApiToken.trim() || ngwafFetching}
                    onClick={async () => {
                      if (!ngwafService) return
                      setNgwafWorkspaces([])
                      setNgwafFetchError('')
                      setNgwafFetching(true)
                      try {
                        const { data } = await client.GET("/api/provision/ngwaf-workspaces" as any, {
                          params: { query: { service_id: ngwafService.service_id, token: ngwafApiToken } }
                        })
                        setNgwafWorkspaces((data as any)?.workspaces || [])
                      } catch (e: any) {
                        setNgwafFetchError(e?.message || 'Could not load workspaces')
                      } finally {
                        setNgwafFetching(false)
                      }
                    }}
                    className="h-8 text-xs"
                  >
                    {ngwafFetching ? 'Loading…' : 'Load'}
                  </Button>
                </div>
              </div>
            )}

            {ngwafFetching ? (
              <p className="text-xs text-muted-foreground animate-pulse">Loading workspaces…</p>
            ) : ngwafWorkspaces.length > 0 ? (
              <div className="space-y-1">
                <Label className="text-xs font-semibold">Select workspace</Label>
                <Select value={ngwafWorkspaceId} onValueChange={(v) => setNgwafWorkspaceId(v ?? '')}>
                  <SelectTrigger className="h-8 text-xs">
                    <SelectValue placeholder="Choose a workspace…" />
                  </SelectTrigger>
                  <SelectContent>
                    {ngwafWorkspaces.map(w => (
                      <SelectItem key={w.id} value={w.id} className="text-xs">
                        {w.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            ) : ngwafFetchError ? (
              <p className="text-xs text-destructive">{ngwafFetchError}</p>
            ) : null}

            {ngwafSaved && (
              <p className="text-xs text-green-600 font-medium">Workspace saved. The NGWAF sync cron will start on the next scheduler tick.</p>
            )}
          </div>

          <DialogFooter>
            {ngwafSaved ? (
              <Button size="sm" onClick={() => setNgwafService(null)}>Close</Button>
            ) : (
              <>
                <Button variant="outline" size="sm" onClick={() => setNgwafService(null)}>Cancel</Button>
                <Button
                  size="sm"
                  disabled={ngwafSaving || !ngwafApiToken.trim()}
                  title={!ngwafApiToken.trim() ? 'Enter your Fastly API token to save' : undefined}
                  onClick={async () => {
                    if (!ngwafService) return
                    setNgwafSaving(true)
                    try {
                      // Security: backend requires a Fastly token bound
                      // to this service. We pass whatever token the admin
                      // entered above; backend accepts either the stored key
                      // (constant-time match) or a token with the 'global'
                      // scope on this service.
                      await client.PATCH("/api/provision/services/{service_id}/ngwaf-workspace" as any, {
                        params: {
                          path: { service_id: ngwafService.service_id },
                          query: { token: ngwafApiToken },
                        },
                        body: { ngwaf_workspace_id: ngwafWorkspaceId.trim() || null } as any,
                      })
                      setNgwafSaved(true)
                      queryClient.invalidateQueries({ queryKey: ['services'] })
                    } catch (e: any) {
                      setNgwafFetchError(e?.message || 'Failed to save')
                    } finally {
                      setNgwafSaving(false)
                    }
                  }}
                >
                  {ngwafSaving ? 'Saving…' : 'Save'}
                </Button>
              </>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
