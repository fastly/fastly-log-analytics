'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  RefreshCw,
  Trash2,
  ChevronDown,
} from 'lucide-react'
import { Button, buttonVariants } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { ConfirmDialog } from "@/components/ui/confirm-dialog"
import { DataTable } from '@/components/DataTable'
import { ColumnDef } from '@tanstack/react-table'
import { cn } from '@/lib/utils'
import { CronScheduleBox } from './CronScheduleBox'

export function CronTab({
  cronColumns,
  cronLogs,
  isLoadingCron,
  isFetchingCron,
  orderedSchedules,
  taskFilter,
  setTaskFilter,
  statusFilter,
  setStatusFilter,
  isAnalyst,
  activeServiceId,
  setDisplayedJobs,
  setSelectedConsoleJobId,
  setConsoleOpen,
  isPurgeOpen,
  setIsPurgeOpen,
  purgeMutation,
}: {
  cronColumns: ColumnDef<any>[]
  cronLogs: any
  isLoadingCron: boolean
  isFetchingCron: boolean
  orderedSchedules: Array<{ task: string; activeJob: any; schedule: any }>
  taskFilter: string
  setTaskFilter: (v: string) => void
  statusFilter: string
  setStatusFilter: (v: string) => void
  isAnalyst: boolean
  activeServiceId: string | null | undefined
  setDisplayedJobs: React.Dispatch<React.SetStateAction<any[]>>
  setSelectedConsoleJobId: (id: number | string | null) => void
  setConsoleOpen: (open: boolean) => void
  isPurgeOpen: boolean
  setIsPurgeOpen: (open: boolean) => void
  purgeMutation: { isPending: boolean; mutate: () => void }
}) {
  const queryClient = useQueryClient()

  return (
    <div className="p-0">
      <DataTable
        columns={cronColumns}
        data={(cronLogs?.entries || []).filter((e: any) => e.status !== 'running')}
        isLoading={isLoadingCron}
        initialSorting={[{ id: 'started_at', desc: true }]}
        onRowClick={(row: any) => {
          setDisplayedJobs(prev => {
            if (prev.some((j: any) => j.id === row.id)) return prev
            return [...prev, { ...row, status: row.status }]
          })
          setSelectedConsoleJobId(row.id)
          setConsoleOpen(true)
        }}
        renderToolbar={(table) => (
          <>
            {orderedSchedules.length > 0 && (
              <div className="p-4 border-b bg-muted/10">
                <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
                  {orderedSchedules.map((item) => (
                    <CronScheduleBox
                      key={item.task}
                      schedule={item.schedule || { task: item.task }}
                      activeJob={item.activeJob}
                      compact={item.task === 'expire'}
                      onOpenConsole={(jobId) => {
                        setConsoleOpen(true)
                        setSelectedConsoleJobId(jobId)
                      }}
                    />
                  ))}
                </div>
              </div>
            )}
            <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 border-b gap-4 bg-card">
              <div className="flex flex-wrap items-center gap-4">
                <h3 className="text-sm font-medium whitespace-nowrap">Recent Cron Activity</h3>
                <div className="flex items-center gap-2">
                  <Select value={taskFilter} onValueChange={(v) => setTaskFilter(v || 'all')}>
                    <SelectTrigger className="h-8 w-[140px] text-xs">
                      <SelectValue placeholder="All tasks" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All tasks</SelectItem>
                      <SelectItem value={isAnalyst ? 'metadata_sync' : 'sync'}>Sync</SelectItem>
                      {!isAnalyst && <SelectItem value="full_sync">Full Sync</SelectItem>}
                      {!isAnalyst && <SelectItem value="gap_heal">Gap Heal</SelectItem>}
                      <SelectItem value="alerts">Alerts</SelectItem>
                      {!isAnalyst && <SelectItem value="commit">Commit</SelectItem>}
                      {!isAnalyst && <SelectItem value="optimize">Optimize</SelectItem>}
                      {!isAnalyst && <SelectItem value="local_compact">Local Compact</SelectItem>}
                      {!isAnalyst && <SelectItem value="expire">Expire</SelectItem>}
                      {!isAnalyst && <SelectItem value="ngwaf_sync">NGWAF Sync</SelectItem>}
                      {!isAnalyst && <SelectItem value="metadata_cleanup">Metadata Cleanup</SelectItem>}
                    </SelectContent>
                  </Select>
                  <Select value={statusFilter} onValueChange={(v) => setStatusFilter(v || 'all')}>
                    <SelectTrigger className="h-8 w-[140px] text-xs">
                      <SelectValue placeholder="All statuses" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All statuses</SelectItem>
                      <SelectItem value="success">Success</SelectItem>
                      {!isAnalyst && <SelectItem value="partial_success">Partial Success</SelectItem>}
                      <SelectItem value="error">Error</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="flex items-center gap-2">
                  <DropdownMenu>
                    <DropdownMenuTrigger
                      className={buttonVariants({ variant: "outline", size: "sm", className: "h-8" })}
                    >
                      <span className="flex items-center text-xs">
                        Columns <ChevronDown className="ml-2 h-4 w-4" />
                      </span>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end" className="w-auto min-w-[200px]">
                      {table
                        .getAllColumns()
                        .filter((column: any) => column.getCanHide())
                        .map((column: any) => {
                          return (
                            <DropdownMenuCheckboxItem
                              key={column.id}
                              className="capitalize whitespace-nowrap"
                              checked={column.getIsVisible()}
                              onCheckedChange={(value) =>
                                column.toggleVisibility(!!value)
                              }
                            >
                              {column.id.replace(/_/g, ' ')}
                            </DropdownMenuCheckboxItem>
                          )
                        })}
                    </DropdownMenuContent>
                  </DropdownMenu>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => queryClient.invalidateQueries({ queryKey: ['admin', 'cron-logs', activeServiceId] })}
                    disabled={isFetchingCron}
                    className="h-8 text-xs"
                  >
                    <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingCron ? 'animate-spin' : ''}`} />
                    Refresh
                  </Button>
                  <button
                    className={cn(buttonVariants({ variant: "outline", size: "sm" }), "h-8 text-xs border-destructive/50 text-destructive hover:bg-destructive hover:text-white cursor-pointer")}
                    onClick={() => setIsPurgeOpen(true)}
                  >
                    <Trash2 className="h-3 w-3 mr-1.5" /> Purge Logs
                  </button>
                </div>
                <ConfirmDialog
                  open={isPurgeOpen}
                  onOpenChange={setIsPurgeOpen}
                  title="Purge all cron logs?"
                  description="This will permanently delete the entire history of automated background job records for this service."
                  confirmLabel="Purge"
                  isDangerous
                  isPending={purgeMutation.isPending}
                  onConfirm={() => purgeMutation.mutate()}
                />
            </div>
          </>
        )}
      />
    </div>
  )
}
