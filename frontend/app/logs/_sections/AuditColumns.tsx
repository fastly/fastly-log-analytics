'use client'

import React from 'react'
import {
  ArrowUpDown,
  FileCode,
  Database,
  Settings,
  ClipboardList,
  Clock,
  ChevronRight,
  X,
  Check,
} from 'lucide-react'
import { Button, buttonVariants } from "@/components/ui/button"
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { DateTimeCell } from '@/components/DataTable'
import { ColumnDef } from '@tanstack/react-table'
import { cn } from '@/lib/utils'

type CatalogMaps = {
  groups: Record<string, { label: string, description: string }>
  fields: Record<string, { label: string, description: string }>
}

export function useAuditColumns(catalogMaps: CatalogMaps): ColumnDef<any>[] {
  return React.useMemo(() => [
    {
      accessorKey: 'timestamp',
      id: 'timestamp',
      meta: { label: 'Time' },
      header: ({ column }) => (
        <Button variant="ghost" onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')} className="-ml-2.5 h-8 data-[state=open]:bg-accent">
          Time
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: ({ row }) => <DateTimeCell iso={row.original.timestamp} />
    },
    {
      accessorKey: 'event_type',
      id: 'event_type',
      meta: { label: 'Event Type' },
      header: 'Event Type',
      cell: ({ row }) => {
        const type = row.original.event_type || 'unknown'
        const colorClass = type === 'provision' ? 'bg-green-500/10 text-green-600' :
                           type === 'teardown' ? 'bg-red-500/10 text-red-600' :
                           type === 'fastly_activation' ? 'bg-blue-500/10 text-blue-600' :
                           type.includes('update') ? 'bg-amber-500/10 text-amber-600' :
                           'bg-slate-500/10 text-slate-600'
        return (
          <Badge className={cn("w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold", colorClass)}>
            {type.replace(/_/g, ' ')}
          </Badge>
        )
      }
    },
    {
      accessorKey: 'actor',
      id: 'actor',
      meta: { label: 'Actor' },
      header: 'Actor',
      cell: ({ row }) => <span className="text-muted-foreground">{row.original.actor}</span>
    },
    {
      accessorKey: 'details',
      id: 'details',
      meta: { label: 'Details' },
      header: 'Details',
      cell: ({ row }) => {
        const details = row.original.details
        if (!details || typeof details !== 'object' || Object.keys(details).length === 0) {
          return <span className="text-muted-foreground italic text-[10px]">No details available</span>
        }

        const type = row.original.event_type || 'unknown'

        return (
          <Dialog>
            <DialogTrigger className={cn(buttonVariants({ variant: "ghost", size: "sm" }), "h-6 text-[10px] bg-muted/40 hover:bg-muted/60 text-muted-foreground")}>
              <FileCode className="h-3 w-3 mr-1.5" />
              View Details
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
                        <div className="flex justify-between items-center text-xs">
                          <span className="text-muted-foreground">Bucket</span>
                          <span className="font-mono">{details.bucket || details.fos_bucket_name || '-'}</span>
                        </div>
                        <div className="flex justify-between items-center text-xs">
                          <span className="text-muted-foreground">Prefix</span>
                          <span className="font-mono">{details.prefix || details.fos_prefix || '(none)'}</span>
                        </div>
                        <div className="flex justify-between items-center text-xs">
                          <span className="text-muted-foreground">Region</span>
                          <span className="font-mono">{details.region || details.fos_region || '-'}</span>
                        </div>
                      </div>
                    </div>

                    <div className="border rounded-md p-3 bg-muted/20">
                      <h4 className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-1.5">
                        <Settings className="w-3 h-3" /> Configuration
                      </h4>
                      <div className="space-y-1.5">
                        <div className="flex justify-between items-center text-xs">
                          <span className="text-muted-foreground">Sample Rate</span>
                          <span className="font-mono">{details.sample_rate || '-'}{details.sample_rate ? '%' : ''}</span>
                        </div>
                        {details.log_period && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Log Period</span>
                            <span className="font-mono">{details.log_period}s</span>
                          </div>
                        )}
                        {details.edge_only !== undefined && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Edge Only</span>
                            <span className="font-mono">{details.edge_only ? 'Yes' : 'No'}</span>
                          </div>
                        )}
                        {details.cdn_url && (
                            <div className="flex items-center text-sm">
                                <span className="text-muted-foreground w-32">CDN URL</span>
                                <span className="font-mono truncate ml-2 max-w-[200px]" title={details.cdn_url}>{details.cdn_url}</span>
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
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Cron Sync</span>
                            <span className="font-mono">{details.enable_cron_sync ? 'Enabled' : 'Disabled'}</span>
                          </div>
                        )}
                        {details.log_retention_days !== undefined && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Retention</span>
                            <span className="font-mono">{details.log_retention_days} days</span>
                          </div>
                        )}
                        {details.delete_after !== undefined && (
                          <div className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground">Auto Delete</span>
                            <span className="font-mono">{details.delete_after ? 'Yes' : 'No'}</span>
                          </div>
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
                          <div className="text-[10px] font-medium text-muted-foreground mb-2 uppercase">Selected Groups</div>
                          <div className="flex flex-wrap gap-1.5">
                            {details.log_fields.groups?.map((id: string) => {
                              const g = catalogMaps.groups[id === null ? "null" : String(id)]
                              return (
                                <Badge key={id} variant="outline" className="text-[10px] py-0 font-normal bg-background/50">
                                  {g ? g.label : id}
                                </Badge>
                              )
                            })}
                            {(!details.log_fields.groups || details.log_fields.groups.length === 0) && (
                                <span className="text-xs text-muted-foreground italic">None</span>
                            )}
                          </div>
                        </div>

                        {details.log_fields.field_overrides && Object.keys(details.log_fields.field_overrides).length > 0 && (
                          <div>
                            <div className="text-[10px] font-medium text-muted-foreground mb-2 uppercase">Field Overrides</div>
                            <div className="flex flex-wrap gap-1.5">
                              {Object.entries(details.log_fields.field_overrides).map(([id, enabled]) => {
                                const f = catalogMaps.fields[id]
                                return (
                                  <Badge
                                    key={id}
                                    className={cn(
                                        "text-[10px] py-0 font-normal border shadow-none",
                                        enabled ? "bg-green-500/10 text-green-600 border-green-500/20" : "bg-red-500/10 text-red-600 border-red-500/20"
                                    )}
                                  >
                                    {enabled ? '+' : '-'}{f ? f.label : id}
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
                        if (key === 'log_fields_deployed') return null;
                        const label = key.replace(/_/g, ' ');
                        const from = (val as any).from;
                        const to = (val as any).to;
                        return (
                          <div key={key} className="flex justify-between items-center text-xs">
                            <span className="text-muted-foreground capitalize">{label}</span>
                            <div className="flex items-center gap-2">
                              <span className="text-muted-foreground line-through opacity-70">{String(from)}</span>
                              <ChevronRight className="w-3 h-3 text-muted-foreground" />
                              <span className="font-mono">{String(to)}</span>
                            </div>
                          </div>
                        )
                      })}
                      {Object.keys(details).filter(k => k !== 'log_fields_deployed').length === 0 && (
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
                         The latest standard and custom field selections have been compiled into VCL and deployed to Fastly.
                       </p>
                    </div>
                  )}
                </div>
              ) : type === 'log_format_update' && details.groups_before && details.groups_after ? (
                <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="border rounded-md p-3 bg-red-500/5">
                      <h4 className="text-xs font-semibold text-red-600 mb-2 uppercase tracking-wide">Before</h4>
                      <div className="space-y-3">
                        <div>
                          <div className="text-[10px] font-medium text-muted-foreground mb-1 uppercase">Groups</div>
                          <div className="flex flex-col gap-1">
                            {details.groups_before.map((id: string) => {
                              const g = catalogMaps.groups[id === null ? "null" : String(id)]
                              return <div key={id} className="text-xs font-mono text-foreground/80 break-words">{g ? `${g.label}` : id}</div>
                            })}
                            {!details.groups_before.length && <div className="text-xs italic text-muted-foreground">None</div>}
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="border rounded-md p-3 bg-green-500/5">
                      <h4 className="text-xs font-semibold text-green-600 mb-2 uppercase tracking-wide">After</h4>
                      <div className="space-y-3">
                        <div>
                          <div className="text-[10px] font-medium text-muted-foreground mb-1 uppercase">Groups</div>
                          <div className="flex flex-col gap-1">
                            {details.groups_after.map((id: string) => {
                              const g = catalogMaps.groups[id === null ? "null" : String(id)]
                              return <div key={id} className="text-xs font-mono text-foreground/80 break-words">{g ? `${g.label}` : id}</div>
                            })}
                            {!details.groups_after.length && <div className="text-xs italic text-muted-foreground">None</div>}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  {(details.fields_added?.length > 0 || details.fields_removed?.length > 0) && (
                    <div className="grid grid-cols-2 gap-4 mt-2">
                      <div className="border rounded-md p-3 border-red-200 dark:border-red-900/30">
                        <div className="text-[10px] font-medium text-red-600 mb-1 uppercase flex items-center gap-1.5"><X className="w-3 h-3" /> Fields Removed</div>
                        <div className="flex flex-col gap-1">
                          {details.fields_removed?.map((id: string) => {
                            const f = catalogMaps.fields[id]
                            return <div key={id} className="text-xs font-mono text-red-600/90 break-words" title={f?.description}>- {f ? f.label : id}</div>
                          })}
                          {(!details.fields_removed || !details.fields_removed.length) && <div className="text-xs italic text-muted-foreground">None</div>}
                        </div>
                      </div>
                      <div className="border rounded-md p-3 border-green-200 dark:border-green-900/30">
                        <div className="text-[10px] font-medium text-green-600 mb-1 uppercase flex items-center gap-1.5"><Check className="w-3 h-3" /> Fields Added</div>
                        <div className="flex flex-col gap-1">
                          {details.fields_added?.map((id: string) => {
                            const f = catalogMaps.fields[id]
                            return <div key={id} className="text-xs font-mono text-green-600/90 break-words" title={f?.description}>+ {f ? f.label : id}</div>
                          })}
                          {(!details.fields_added || !details.fields_added.length) && <div className="text-xs italic text-muted-foreground">None</div>}
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div className="flex flex-col gap-2 mt-2">
                  {Object.entries(details).map(([key, value]) => {
                    if (
                      (key.toLowerCase().includes('prefix') && !value) ||
                      value === '' ||
                      value === null ||
                      value === undefined ||
                      (type === 'fastly_activation' && key === 'active')
                    ) {
                      return null
                    }

                    const valString = typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)

                    return (
                      <div key={key} className="flex flex-col border rounded p-3 bg-muted/20">
                        <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide mb-1.5">{key.replace(/_/g, ' ')}</span>
                        {typeof value === 'object' ? (
                          <pre className="text-xs font-mono bg-background p-2 rounded overflow-x-auto text-foreground/90 whitespace-pre-wrap">{valString}</pre>
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
      }
    }
  ], [catalogMaps])
}
