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
import { HelpCircle } from 'lucide-react'
import { HelpDialog } from '@/components/ui/help-dialog'
import { CRON_GROUPS, CRON_DISPLAY_NAMES, CRON_EXPLANATIONS } from './CronExplanations'
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
  status,
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
  status: any
  activeServiceId: string | null | undefined
  setDisplayedJobs: React.Dispatch<React.SetStateAction<any[]>>
  setSelectedConsoleJobId: (id: number | string | null) => void
  setConsoleOpen: (open: boolean) => void
  isPurgeOpen: boolean
  setIsPurgeOpen: (open: boolean) => void
  purgeMutation: { isPending: boolean; mutate: () => void }
}) {
  const queryClient = useQueryClient()
  const [isHelpOpen, setIsHelpOpen] = React.useState(false)

  // Memoize the filtered rows — a fresh `.filter()` array on every render
  // makes TanStack Table see new data identity each render, which fires
  // its built-in `resetPageIndex()` on every render → setPagination →
  // re-render → new array → loop. React surfaces this as
  // "Maximum update depth exceeded" originating in setPageIndex. The
  // infinite re-render also tears down sibling components' useEffect
  // setup, which is why CronLiveLog ends up stuck on
  // "Waiting for stream...": useSSE's start() call is aborted by the
  // cleanup before the fetch has a chance to register a chunk handler.
  const tableData = React.useMemo(
    () => (cronLogs?.entries || []).filter((e: any) => e.status !== 'running'),
    [cronLogs?.entries],
  )

  return (
    <div className="p-0">
      <DataTable
        columns={cronColumns}
        data={tableData}
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
              <div className="p-4 border-b bg-muted/10 space-y-5">
                <div className="flex items-center gap-2">
                  <h3 className="text-sm font-medium">Cron Jobs</h3>
                  <button onClick={() => setIsHelpOpen(true)} aria-label="About background cron jobs" className="text-muted-foreground hover:text-foreground">
                    <HelpCircle className="h-4 w-4" />
                  </button>
                </div>

                <HelpDialog
                  open={isHelpOpen}
                  onOpenChange={setIsHelpOpen}
                  title="Background Cron Jobs"
                  icon={<HelpCircle className="h-5 w-5" />}
                  size="xl"
                >
                  <div className="space-y-6">
                    <p>
                      Fastly Log Analytics uses a distributed architecture of background jobs to ingest logs, perform maintenance, and pre-calculate analytics.
                    </p>
                    {CRON_GROUPS.map(group => (
                      <div key={group.title} className="space-y-2">
                        <h4 className="font-semibold text-foreground">{group.title}</h4>
                        <ul className="space-y-3">
                          {group.tasks.map(task => (
                            <li key={task} className="pl-4 border-l-2 border-muted">
                              <div className="flex items-center gap-2 mb-1">
                                <span className="font-medium text-foreground">{CRON_DISPLAY_NAMES[task] || task}</span>
                                <span className="text-[10px] font-mono text-muted-foreground px-1.5 py-0.5 bg-muted rounded border">{task}</span>
                              </div>
                              <p className="text-xs text-muted-foreground/80 leading-relaxed">{CRON_EXPLANATIONS[task]}</p>
                            </li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </div>
                </HelpDialog>

                <div className="space-y-5">
                  {CRON_GROUPS.map(group => {
                    const groupTasks = orderedSchedules.filter(s => group.tasks.includes(s.task))
                    if (groupTasks.length === 0) return null

                    return (
                      <div key={group.title} className="space-y-3">
                        <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">{group.title}</div>
                        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
                          {groupTasks.map((item) => (
                            <CronScheduleBox
                              key={item.task}
                              schedule={item.schedule || { task: item.task }}
                              activeJob={item.activeJob}
                              compact={item.task === 'expire_snapshots'}
                              onOpenConsole={(jobId) => {
                                setConsoleOpen(true)
                                setSelectedConsoleJobId(jobId)
                              }}
                            />
                          ))}
                        </div>
                      </div>
                    )
                  })}
                  {(() => {
                    const groupedTaskNames = CRON_GROUPS.flatMap(g => g.tasks)
                    const otherTasks = orderedSchedules.filter(s => !groupedTaskNames.includes(s.task))
                    if (otherTasks.length === 0) return null
                    return (
                      <div className="space-y-3">
                        <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">Other Tasks</div>
                        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
                          {otherTasks.map((item) => (
                            <CronScheduleBox
                              key={item.task}
                              schedule={item.schedule || { task: item.task }}
                              activeJob={item.activeJob}
                              compact={item.task === 'expire_snapshots'}
                              onOpenConsole={(jobId) => {
                                setConsoleOpen(true)
                                setSelectedConsoleJobId(jobId)
                              }}
                            />
                          ))}
                        </div>
                      </div>
                    )
                  })()}
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
                      <SelectItem value={isAnalyst ? 'metadata_sync' : 'log_discovery'}>Sync</SelectItem>
                      {!isAnalyst && <SelectItem value="rum_sync">RUM Sync</SelectItem>}
                      {!isAnalyst && <SelectItem value="full_sync">Full Sync</SelectItem>}
                      {!isAnalyst && <SelectItem value="gap_heal">Gap Heal</SelectItem>}
                      <SelectItem value="alerts">Alerts</SelectItem>
                      {!isAnalyst && <SelectItem value="log_ingest">Ingest Logs</SelectItem>}
                      {!isAnalyst && <SelectItem value="rum_commit">RUM Commit</SelectItem>}
                      {!isAnalyst && <SelectItem value="optimize">Optimize</SelectItem>}
                      {!isAnalyst && <SelectItem value="local_compact">Local Compact</SelectItem>}
                      {!isAnalyst && <SelectItem value="rollup_compact_daily">Rollup Compact</SelectItem>}
                      {!isAnalyst && <SelectItem value="rollup_hour_heal">Rollup Heal</SelectItem>}
                      {!isAnalyst && <SelectItem value="expire_snapshots">Expire</SelectItem>}
                      {!isAnalyst && !!status?.ngwaf_workspace_id && <SelectItem value="ngwaf_sync">NGWAF Sync</SelectItem>}
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
