'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Database, FileCode, ClipboardList, RefreshCw, ChevronRight, Settings, Clock, X, Check } from 'lucide-react'
import { queryKeys } from '@/lib/api'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button, buttonVariants } from '@/components/ui/button'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { DataTableReadonly, ColumnVisibilityDropdown, DateTimeCell } from '@/components/DataTable'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import type { ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import type { CatalogMaps } from './useLogsPageState'

type AuditLogEntry = components['schemas']['AuditLogEntry']

interface AuditDetails {
  bucket?: string
  fos_bucket_name?: string
  prefix?: string
  fos_prefix?: string
  region?: string
  fos_region?: string
  sample_rate?: number | string
  log_period?: number | string
  edge_only?: boolean
  cdn_url?: string
  enable_cron_sync?: boolean
  log_retention_days?: number
  delete_after?: boolean
  log_fields?: { groups?: string[]; field_overrides?: Record<string, boolean> }
  log_fields_deployed?: boolean
  groups_before?: string[]
  groups_after?: string[]
  fields_added?: string[]
  fields_removed?: string[]
}

function AuditKVRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex justify-between items-center text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-mono">{value}</span>
    </div>
  )
}

function AuditGroupDiffCard({
  title,
  colorClass,
  bgClass,
  ids,
  groups,
}: {
  title: string
  colorClass: string
  bgClass: string
  ids: string[]
  groups: CatalogMaps['groups']
}) {
  return (
    <div className={cn('border rounded-md p-3', bgClass)}>
      <h4 className={cn('text-xs font-semibold mb-2 uppercase tracking-wide', colorClass)}>{title}</h4>
      <div className="text-[10px] font-medium text-muted-foreground mb-1 uppercase">Groups</div>
      <div className="flex flex-col gap-1">
        {ids.map((id) => {
          const g = groups[id === null ? 'null' : String(id)]
          return (
            <div key={id} className="text-xs font-mono text-foreground/80 break-words">
              {g ? g.label : id}
            </div>
          )
        })}
        {!ids.length && <div className="text-xs italic text-muted-foreground">None</div>}
      </div>
    </div>
  )
}

export function useAuditColumns(catalogMaps: CatalogMaps): ColumnDef<AuditLogEntry>[] {
  return React.useMemo(
    () => [
      {
        accessorKey: 'timestamp',
        id: 'timestamp',
        meta: { label: 'Time' },
        header: 'Time',
        cell: ({ row }) => <DateTimeCell iso={row.original.timestamp} />,
      },
      {
        accessorKey: 'event_type',
        id: 'event_type',
        meta: { label: 'Event Type' },
        header: 'Event Type',
        cell: ({ row }) => {
          const type = row.original.event_type || 'unknown'
          const colorClass =
            type === 'provision'
              ? 'bg-green-500/10 text-green-600'
              : type === 'teardown'
                ? 'bg-red-500/10 text-red-600'
                : type === 'fastly_activation'
                  ? 'bg-blue-500/10 text-blue-600'
                  : type.includes('update')
                    ? 'bg-amber-500/10 text-amber-600'
                    : 'bg-slate-500/10 text-slate-600'
          return (
            <Badge className={cn('w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold', colorClass)}>
              {type.replace(/_/g, ' ')}
            </Badge>
          )
        },
      },
      {
        accessorKey: 'actor',
        id: 'actor',
        meta: { label: 'Actor' },
        header: 'Actor',
        cell: ({ row }) => <span className="text-muted-foreground">{row.original.actor}</span>,
      },
      {
        accessorKey: 'details',
        id: 'details',
        meta: { label: 'Details' },
        header: 'Details',
        cell: ({ row }) => {
          const details: AuditDetails = row.original.details
          if (!details || typeof details !== 'object' || Object.keys(details).length === 0) {
            return <span className="text-muted-foreground italic text-[10px]">No details available</span>
          }
          const type = row.original.event_type || 'unknown'
          return (
            <Dialog>
              <DialogTrigger
                className={cn(
                  buttonVariants({ variant: 'ghost', size: 'sm' }),
                  'h-6 text-[10px] bg-muted/40 hover:bg-muted/60 text-muted-foreground',
                )}
              >
                <FileCode className="h-3 w-3 mr-1.5" /> View Details
              </DialogTrigger>
              <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
                <DialogHeader>
                  <DialogTitle className="text-sm font-semibold capitalize flex items-center gap-2">
                    <Settings className="w-4 h-4 text-primary" />
                    {type.replace(/_/g, ' ')} Details
                  </DialogTitle>
                </DialogHeader>
                {type === 'provision' ? (
                  <div className="space-y-4 mt-2">
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                      <div className="border rounded-md p-3 bg-muted/20">
                        <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5">
                          <Database className="w-3 h-3" /> Storage
                        </h4>
                        <div className="space-y-1.5">
                          <AuditKVRow label="Bucket" value={details.bucket || details.fos_bucket_name || '-'} />
                          <AuditKVRow label="Prefix" value={details.prefix || details.fos_prefix || '(none)'} />
                          <AuditKVRow label="Region" value={details.region || details.fos_region || '-'} />
                        </div>
                      </div>
                      <div className="border rounded-md p-3 bg-muted/20">
                        <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5">
                          <Settings className="w-3 h-3" /> Configuration
                        </h4>
                        <div className="space-y-1.5">
                          <AuditKVRow
                            label="Sample Rate"
                            value={`${details.sample_rate || '-'}${details.sample_rate ? '%' : ''}`}
                          />
                          {details.log_period && <AuditKVRow label="Log Period" value={`${details.log_period}s`} />}
                          {details.edge_only !== undefined && (
                            <AuditKVRow label="Edge Only" value={details.edge_only ? 'Yes' : 'No'} />
                          )}
                          {details.cdn_url && (
                            <div className="flex items-center text-sm">
                              <span className="text-muted-foreground w-32">CDN URL</span>
                              <span className="font-mono truncate ml-2 max-w-[200px]" title={details.cdn_url}>
                                {details.cdn_url}
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                    {(details.enable_cron_sync !== undefined || details.log_retention_days !== undefined) && (
                      <div className="border rounded-md p-3 bg-muted/20">
                        <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5">
                          <Clock className="w-3 h-3" /> Automation & Retention
                        </h4>
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-1.5">
                          {details.enable_cron_sync !== undefined && (
                            <AuditKVRow label="Cron Sync" value={details.enable_cron_sync ? 'Enabled' : 'Disabled'} />
                          )}
                          {details.log_retention_days !== undefined && (
                            <AuditKVRow label="Retention" value={`${details.log_retention_days} days`} />
                          )}
                          {details.delete_after !== undefined && (
                            <AuditKVRow label="Auto Delete" value={details.delete_after ? 'Yes' : 'No'} />
                          )}
                        </div>
                      </div>
                    )}
                    {details.log_fields && (
                      <div className="border rounded-md p-3 bg-muted/20">
                        <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-3 flex items-center gap-1.5 border-b pb-2">
                          <ClipboardList className="w-3 h-3" /> Initial Log Fields
                        </h4>
                        <div className="space-y-4">
                          <div>
                            <div className="text-[10px] font-medium text-muted-foreground mb-2 uppercase">
                              Selected Groups
                            </div>
                            <div className="flex flex-wrap gap-1.5">
                              {details.log_fields.groups?.map((id) => {
                                const g = catalogMaps.groups[id === null ? 'null' : String(id)]
                                return (
                                  <Badge key={id} variant="outline" className="text-[10px] py-0 font-normal bg-background/50">
                                    {g ? g.label : id}
                                  </Badge>
                                )
                              })}
                              {!details.log_fields.groups?.length && (
                                <span className="text-xs text-muted-foreground italic">None</span>
                              )}
                            </div>
                          </div>
                          {details.log_fields.field_overrides &&
                            Object.keys(details.log_fields.field_overrides).length > 0 && (
                              <div>
                                <div className="text-[10px] font-medium text-muted-foreground mb-2 uppercase">
                                  Field Overrides
                                </div>
                                <div className="flex flex-wrap gap-1.5">
                                  {Object.entries(details.log_fields.field_overrides).map(([id, enabled]) => {
                                    const f = catalogMaps.fields[id]
                                    return (
                                      <Badge
                                        key={id}
                                        className={cn(
                                          'text-[10px] py-0 font-normal border shadow-none',
                                          enabled
                                            ? 'bg-green-500/10 text-green-600 border-green-500/20'
                                            : 'bg-red-500/10 text-red-600 border-red-500/20',
                                        )}
                                      >
                                        {enabled ? '+' : '-'}
                                        {f ? f.label : id}
                                      </Badge>
                                    )
                                  })}
                                </div>
                              </div>
                            )}
                        </div>
                      </div>
                    )}
                  </div>
                ) : type === 'logging_settings_update' ? (
                  <div className="space-y-4 mt-2">
                    <div className="border rounded-md p-3 bg-muted/20">
                      <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-3 flex items-center gap-1.5 border-b pb-2">
                        <Settings className="w-3 h-3" /> Settings Deployed
                      </h4>
                      <div className="flex flex-col gap-2">
                        {Object.entries(details).map(([key, val]) => {
                          if (key === 'log_fields_deployed') return null
                          const { from, to } = val as { from?: unknown; to?: unknown }
                          return (
                            <div key={key} className="flex justify-between items-center text-xs">
                              <span className="text-muted-foreground capitalize">{key.replace(/_/g, ' ')}</span>
                              <div className="flex items-center gap-2">
                                <span className="text-muted-foreground line-through opacity-70">{String(from)}</span>
                                <ChevronRight className="w-3 h-3 text-muted-foreground" />
                                <span className="font-mono">{String(to)}</span>
                              </div>
                            </div>
                          )
                        })}
                        {Object.keys(details).filter((k) => k !== 'log_fields_deployed').length === 0 && (
                          <span className="text-xs text-muted-foreground italic">No settings changed.</span>
                        )}
                      </div>
                    </div>
                    {details.log_fields_deployed && (
                      <div className="border rounded-md p-3 bg-green-500/10 border-green-500/20">
                        <h4 className="text-[10px] font-semibold text-green-700 uppercase tracking-wide mb-2 flex items-center gap-1.5">
                          <ClipboardList className="w-3 h-3" /> Log Format Updated
                        </h4>
                        <p className="text-xs text-green-700/80">
                          The latest standard and custom field selections have been compiled into VCL and deployed to
                          Fastly.
                        </p>
                      </div>
                    )}
                  </div>
                ) : type === 'log_format_update' && details.groups_before && details.groups_after ? (
                  <div className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <AuditGroupDiffCard
                        title="Before"
                        colorClass="text-red-600"
                        bgClass="bg-red-500/5"
                        ids={details.groups_before}
                        groups={catalogMaps.groups}
                      />
                      <AuditGroupDiffCard
                        title="After"
                        colorClass="text-green-600"
                        bgClass="bg-green-500/5"
                        ids={details.groups_after}
                        groups={catalogMaps.groups}
                      />
                    </div>
                    {((details.fields_added?.length ?? 0) > 0 || (details.fields_removed?.length ?? 0) > 0) && (
                      <div className="grid grid-cols-2 gap-4 mt-2">
                        {(
                          [
                            {
                              title: 'Fields Removed',
                              icon: X,
                              ids: details.fields_removed,
                              prefix: '-',
                              border: 'border-red-200 dark:border-red-900/30',
                              headerColor: 'text-red-600',
                              itemColor: 'text-red-600/90',
                            },
                            {
                              title: 'Fields Added',
                              icon: Check,
                              ids: details.fields_added,
                              prefix: '+',
                              border: 'border-green-200 dark:border-green-900/30',
                              headerColor: 'text-green-600',
                              itemColor: 'text-green-600/90',
                            },
                          ] as const
                        ).map(({ title, icon: Icon, ids, prefix, border, headerColor, itemColor }) => (
                          <div key={title} className={cn('border rounded-md p-3', border)}>
                            <div className={cn('text-[10px] font-medium mb-1 uppercase flex items-center gap-1.5', headerColor)}>
                              <Icon className="w-3 h-3" /> {title}
                            </div>
                            <div className="flex flex-col gap-1">
                              {ids?.map((id) => {
                                const f = catalogMaps.fields[id]
                                return (
                                  <div
                                    key={id}
                                    className={cn('text-xs font-mono break-words', itemColor)}
                                    title={f?.description}
                                  >
                                    {prefix} {f ? f.label : id}
                                  </div>
                                )
                              })}
                              {!ids?.length && <div className="text-xs italic text-muted-foreground">None</div>}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="flex flex-col gap-2 mt-2">
                    {Object.entries(details).map(([key, value]) => {
                      if (
                        (key.toLowerCase().includes('prefix') && !value) ||
                        value === '' ||
                        value == null ||
                        (type === 'fastly_activation' && key === 'active')
                      ) {
                        return null
                      }
                      const valString = typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)
                      return (
                        <div key={key} className="flex flex-col border rounded p-3 bg-muted/20">
                          <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-1.5">
                            {key.replace(/_/g, ' ')}
                          </span>
                          {typeof value === 'object' ? (
                            <pre className="text-xs font-mono bg-background p-2 rounded overflow-x-auto text-foreground/90 whitespace-pre-wrap">
                              {valString}
                            </pre>
                          ) : (
                            <span className="text-sm font-mono text-foreground/90 break-all">{valString}</span>
                          )}
                        </div>
                      )
                    })}
                  </div>
                )}
              </DialogContent>
            </Dialog>
          )
        },
      },
    ],
    [catalogMaps],
  )
}

export function ServiceHistoryTab({
  auditColumns,
  auditLogs,
  isLoadingAudit,
  isFetchingAudit,
  eventFilter,
  setEventFilter,
  activeServiceId,
}: {
  auditColumns: ColumnDef<AuditLogEntry>[]
  auditLogs: components['schemas']['AuditLogsResponse'] | undefined
  isLoadingAudit: boolean
  isFetchingAudit: boolean
  eventFilter: string
  setEventFilter: (v: string) => void
  activeServiceId: string | null | undefined
}) {
  const queryClient = useQueryClient()
  return (
    <div className="p-0">
      <DataTableReadonly
        columns={auditColumns}
        data={auditLogs?.entries || []}
        isLoading={isLoadingAudit}
        initialSorting={[{ id: 'timestamp', desc: true }]}
        renderToolbar={(table) => (
          <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 border-b gap-4">
            <div className="flex items-center gap-4">
              <h3 className="text-sm font-medium whitespace-nowrap">Service History</h3>
              <Select value={eventFilter} onValueChange={(v) => setEventFilter(v || 'all')}>
                <SelectTrigger className="h-8 w-[200px] text-xs">
                  <SelectValue placeholder="All events" />
                </SelectTrigger>
                <SelectContent>
                  {[
                    ['all', 'All events'],
                    ['provision', 'Provision'],
                    ['fastly_activation', 'Fastly Activation'],
                    ['cron_settings_update', 'Cron Settings Update'],
                    ['logging_settings_update', 'Log Settings Updated'],
                    ['log_format_update', 'Log Format Update'],
                  ].map(([val, label]) => (
                    <SelectItem key={val} value={val}>
                      {label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown table={table} />
              <Button
                variant="outline"
                size="sm"
                className="h-8 text-xs"
                disabled={isFetchingAudit}
                onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.admin.auditLogsFor(activeServiceId) })}
              >
                <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingAudit ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
          </div>
        )}
      />
    </div>
  )
}
