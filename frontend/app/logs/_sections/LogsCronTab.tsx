'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { RefreshCw, HelpCircle, Trash2 } from 'lucide-react'
import { queryKeys } from '@/lib/api'
import { cn } from '@/lib/utils'
import type { SyncStatus } from '@/hooks/useSSE'
import { Button, buttonVariants } from '@/components/ui/button'
import { ConfirmDialog, HelpDialog } from '@/components/ui/confirm-dialog'
import { DataTable, ColumnVisibilityDropdown } from '@/components/DataTable'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import type { ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import type { CronRun } from './useLogsPageState'
import {
  CRON_EXPLANATIONS,
  CRON_DISPLAY_NAMES,
  CRON_GROUPS,
  CronScheduleBox,
  CronScheduleGroupSection,
  type CronScheduleEntry,
} from './LogsCronSchedule'
import { useCronColumns } from './LogsCronColumns'
import { CronLiveLog, FloatingOperationsDock } from './LogsCronConsole'

export {
  CRON_EXPLANATIONS,
  CRON_DISPLAY_NAMES,
  CRON_GROUPS,
  CronScheduleBox,
  CronScheduleGroupSection,
  useCronColumns,
  CronLiveLog,
  FloatingOperationsDock,
}

export function CronTab({
  cronColumns,
  cronLogs,
  isLoadingCron,
  isFetchingCron,
  orderedSchedules,
  isLoadingSchedule = false,
  taskFilter,
  setTaskFilter,
  statusFilter,
  setStatusFilter,
  status,
  activeServiceId,
  setDisplayedJobs,
  setSelectedConsoleJobId,
  setConsoleOpen,
  isPurgeOpen,
  setIsPurgeOpen,
  purgeMutation,
}: {
  cronColumns: ColumnDef<CronRun>[]
  cronLogs: components['schemas']['CronRunsResponse'] | undefined
  isLoadingCron: boolean
  isFetchingCron: boolean
  orderedSchedules: Array<{ task: string; activeJob: CronRun | undefined; schedule: CronScheduleEntry }>
  isLoadingSchedule?: boolean
  taskFilter: string
  setTaskFilter: (v: string) => void
  statusFilter: string
  setStatusFilter: (v: string) => void
  status: SyncStatus | undefined
  activeServiceId: string | null | undefined
  setDisplayedJobs: React.Dispatch<React.SetStateAction<CronRun[]>>
  setSelectedConsoleJobId: (id: number | string | null) => void
  setConsoleOpen: (open: boolean) => void
  isPurgeOpen: boolean
  setIsPurgeOpen: (open: boolean) => void
  purgeMutation: { isPending: boolean; mutate: () => void }
}) {
  const queryClient = useQueryClient()
  const [isHelpOpen, setIsHelpOpen] = React.useState(false)
  const tableData = React.useMemo(() => (cronLogs?.entries || []).filter((e) => e.status !== 'running'), [cronLogs?.entries])
  const handleOpenConsole = React.useCallback(
    (jobId: number | string) => {
      setConsoleOpen(true)
      setSelectedConsoleJobId(jobId)
    },
    [setConsoleOpen, setSelectedConsoleJobId],
  )
  const groupedTaskNames = React.useMemo(() => CRON_GROUPS.flatMap((g) => g.tasks), [])
  const otherTasks = React.useMemo(
    () => orderedSchedules.filter((s) => !groupedTaskNames.includes(s.task)),
    [orderedSchedules, groupedTaskNames],
  )
  const taskFilterOptions: Array<[string, string]> = [
    ['rum_sync', 'RUM Sync'],
    ['full_sync', 'Full Sync'],
    ['gap_heal', 'Gap Heal'],
    ['alerts', 'Alerts'],
    ['commit', 'Commit'],
    ['rum_commit', 'RUM Commit'],
    ['optimize', 'Optimize'],
    ['local_compact', 'Local Compact'],
    ['rollup_compact_daily', 'Rollup Compact'],
    ['rollup_hour_heal', 'Rollup Heal'],
    ['expire_snapshots', 'Expire'],
    ...(status?.ngwaf_workspace_id ? [['ngwaf_sync', 'NGWAF Sync'] as [string, string]] : []),
    ['metadata_cleanup', 'Metadata Cleanup'],
  ]

  return (
    <div className="p-0">
      <DataTable
        columns={cronColumns}
        data={tableData}
        isLoading={isLoadingCron}
        initialSorting={[{ id: 'started_at', desc: true }]}
        onRowClick={(row: CronRun) => {
          setDisplayedJobs((prev) => (prev.some((j) => j.id === row.id) ? prev : [...prev, { ...row, status: row.status }]))
          setSelectedConsoleJobId(row.id)
          setConsoleOpen(true)
        }}
        renderToolbar={(table) => (
          <>
            {(orderedSchedules.length > 0 || isLoadingSchedule) && (
              <div className="p-4 border-b bg-muted/10 space-y-5">
                <div className="flex items-center gap-2">
                  <h3 className="text-sm font-medium">Cron Jobs</h3>
                  <button
                    onClick={() => setIsHelpOpen(true)}
                    aria-label="About background cron jobs"
                    className="text-muted-foreground hover:text-foreground"
                  >
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
                      Fastly Log Analytics uses a distributed architecture of background jobs to ingest logs, perform
                      maintenance, and pre-calculate analytics.
                    </p>
                    {CRON_GROUPS.map((group) => (
                      <div key={group.title} className="space-y-2">
                        <h4 className="font-semibold text-foreground">{group.title}</h4>
                        <ul className="space-y-3">
                          {group.tasks.map((task) => (
                            <li key={task} className="pl-4 border-l-2 border-muted">
                              <div className="flex items-center gap-2 mb-1">
                                <span className="font-medium text-foreground">{CRON_DISPLAY_NAMES[task] || task}</span>
                                <span className="text-[10px] font-mono text-muted-foreground px-1.5 py-0.5 bg-muted rounded border">
                                  {task}
                                </span>
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
                  {orderedSchedules.length === 0 ? (
                    CRON_GROUPS.map((group) => (
                      <div key={group.title} className="space-y-3">
                        <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">
                          {group.title}
                        </div>
                        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-2 w-full">
                          {group.tasks.slice(0, 3).map((task) => (
                            <div
                              key={task}
                              className="h-8 rounded-md border border-muted bg-muted/20 animate-pulse min-w-[130px] flex-1"
                            />
                          ))}
                        </div>
                      </div>
                    ))
                  ) : (
                    <>
                      {CRON_GROUPS.map((group) => (
                        <CronScheduleGroupSection
                          key={group.title}
                          title={group.title}
                          items={orderedSchedules.filter((s) => group.tasks.includes(s.task))}
                          onOpenConsole={handleOpenConsole}
                        />
                      ))}
                      <CronScheduleGroupSection title="Other Tasks" items={otherTasks} onOpenConsole={handleOpenConsole} />
                    </>
                  )}
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
                      <SelectItem value="log_discovery">Log Discovery</SelectItem>
                      {taskFilterOptions.map(([val, label]) => (
                        <SelectItem key={val} value={val}>
                          {label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <Select value={statusFilter} onValueChange={(v) => setStatusFilter(v || 'all')}>
                    <SelectTrigger className="h-8 w-[140px] text-xs">
                      <SelectValue placeholder="All statuses" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All statuses</SelectItem>
                      <SelectItem value="success">Success</SelectItem>
                      <SelectItem value="partial_success">Partial Success</SelectItem>
                      <SelectItem value="error">Error</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <ColumnVisibilityDropdown table={table} />
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 text-xs"
                  disabled={isFetchingCron}
                  onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.admin.cronLogsFor(activeServiceId) })}
                >
                  <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingCron ? 'animate-spin' : ''}`} />
                  Refresh
                </Button>
                <button
                  className={cn(
                    buttonVariants({ variant: 'outline', size: 'sm' }),
                    'h-8 text-xs border-destructive/50 text-destructive hover:bg-destructive hover:text-white cursor-pointer',
                  )}
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
