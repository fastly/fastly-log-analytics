'use client'
import React from 'react'
import { ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Settings,
  Settings2,
  Trash2,
  ExternalLink,
  ArrowUpDown,
  Play,
  CloudDownload,
  UserPlus,
  Bot,
  ChevronDown,
  KeyRound,
} from 'lucide-react'
import { formatBytes } from '@/lib/utils'

type ServiceConfig = components["schemas"]["ServiceConfig"]

export interface ServiceColumnDeps {
  activeServiceId: string | null
  setActiveServiceId: (id: string) => void
  router: { push: (href: string) => void }
  servicesLength: number
  setCronService: (s: ServiceConfig) => void
  setSettingsService: (s: ServiceConfig) => void
  setTeardownService: (s: ServiceConfig) => void
  setInviteService: (s: ServiceConfig) => void
  openNgwaf: (s: ServiceConfig) => void
  openCredentials: (s: ServiceConfig) => void
}

export function buildServiceColumns(deps: ServiceColumnDeps): ColumnDef<ServiceConfig>[] {
  const {
    activeServiceId,
    setActiveServiceId,
    router,
    servicesLength,
    setCronService,
    setSettingsService,
    setTeardownService,
    setInviteService,
    openNgwaf,
    openCredentials,
  } = deps

  return [
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
                    aria-label="Download local cache as ZIP"
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
              aria-label="Cron sync settings"
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
      size: servicesLength > 0 ? 780 : 120,
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
                onClick={() => openNgwaf(service)}
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
                  <DropdownMenuItem onClick={() => openNgwaf(service)}>
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
  ]
}
