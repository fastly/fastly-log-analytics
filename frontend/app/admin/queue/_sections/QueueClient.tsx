'use client'

import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Sparkline } from '@/components/Sparkline'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import type { components } from '@/types/api.generated'

import { useAdminEventStream } from '@/hooks/useAdminEventStream'

type TrendBatch = components['schemas']['MetricHistoryBatchResponse']

interface CeleryStatusResponse {
  broker_reachable?: boolean
  queues?: Record<string, number>
  workers?: {
    count: number
    active_tasks: Record<string, { id: string; name: string; args: unknown[]; time_start: number }[]>
    stats: Record<string, unknown>
    registered: Record<string, string[]>
    scheduled: Record<string, unknown[]>
    error?: string
  }
  schedules?: Array<{
    name: string
    task: string
  }>
  ledger?: Record<string, number>
}

export function QueueClient() {
  const { state: sseConnected } = useAdminEventStream(true, ['celery-status'])

  const { data, isLoading, error } = useQuery({
    queryKey: ['admin', 'celery-status'],
    queryFn: async ({ signal }): Promise<CeleryStatusResponse> => {
      const { data, response } = await client.GET('/api/admin/celery/status', { signal })
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      return data as unknown as CeleryStatusResponse
    },
    // We only poll if SSE isn't fully established as a fallback,
    // otherwise the stream pushes to this query key directly.
    refetchInterval: sseConnected === 'open' ? false : 5000,
  })

  const { data: trends, isLoading: trendsLoading, error: trendsError } = useQuery({
    queryKey: ['admin', 'metric-history-batch', '1h'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/admin/metric-history/batch', {
        params: { query: { since: '1h' } },
        signal,
      })
      return data as TrendBatch
    },
    refetchInterval: 60_000,
  })

  if (isLoading) return <div>Loading...</div>
  if (error) return <div className="text-red-500">Error: {(error as Error).message}</div>
  if (!data) return null

  const queues = data.queues ? Object.entries(data.queues).sort((a, b) => a[0].localeCompare(b[0])) : []
  const activeWorkers = data.workers?.active_tasks ? Object.entries(data.workers.active_tasks) : []
  const schedules = data.schedules || []

  const queuePoints = trends?.series?.['celery_queue_depth'] ?? []
  const workerPoints = trends?.series?.['celery_active_tasks'] ?? []
  const latestQueue = queuePoints.length ? queuePoints[queuePoints.length - 1].value : null
  const latestWorker = workerPoints.length ? workerPoints[workerPoints.length - 1].value : null

  return (
    <div className="grid gap-6">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <AnalyticsCard
          title="Total Queue Depth (1h)"
          description="Aggregate sum of all Celery queue lengths."
          isLoading={trendsLoading}
          error={trendsError as AnalyticsCardError | null}
          isEmpty={!trendsLoading && !trendsError && queuePoints.length === 0}
        >
          <div className="flex items-baseline justify-between mb-2">
            <div className="text-2xl font-semibold tabular-nums">
              {latestQueue === null ? '–' : latestQueue}
            </div>
            <div className="text-xs text-muted-foreground">
              {queuePoints.length > 0 ? `${queuePoints.length} samples` : 'no samples yet'}
            </div>
          </div>
          <div className="text-foreground/70">
            <Sparkline
              points={queuePoints}
              height={80}
              label="Queue Depth"
              formatValue={(v) => String(v)}
            />
          </div>
        </AnalyticsCard>

        <AnalyticsCard
          title="Active Tasks (1h)"
          description="Total actively executing Celery tasks across all workers."
          isLoading={trendsLoading}
          error={trendsError as AnalyticsCardError | null}
          isEmpty={!trendsLoading && !trendsError && workerPoints.length === 0}
        >
          <div className="flex items-baseline justify-between mb-2">
            <div className="text-2xl font-semibold tabular-nums">
              {latestWorker === null ? '–' : latestWorker}
            </div>
            <div className="text-xs text-muted-foreground">
              {workerPoints.length > 0 ? `${workerPoints.length} samples` : 'no samples yet'}
            </div>
          </div>
          <div className="text-foreground/70">
            <Sparkline
              points={workerPoints}
              height={80}
              label="Active Tasks"
              formatValue={(v) => String(v)}
            />
          </div>
        </AnalyticsCard>
      </div>
      <AnalyticsCard
        title="Queue Depths"
        helpTitle="Queue Depths"
        helpContent={
          <div className="space-y-4 text-sm">
            <p>
              This table shows the current number of pending messages waiting to be processed in each Celery queue.
            </p>
            <ul className="list-disc pl-5 space-y-2">
              <li><strong>celery:</strong> The default queue.</li>
              <li><strong>q.sync:</strong> High-priority queue for discovering logs.</li>
              <li><strong>q.commit:</strong> Background queue for writing records to Iceberg.</li>
            </ul>
          </div>
        }
      >
        {queues.length === 0 ? (
          <p className="text-sm text-muted-foreground">No queues found or Celery mode is inactive.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Queue Name</TableHead>
                <TableHead className="text-right">Messages</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {queues.map(([queueName, count]) => (
                <TableRow key={queueName}>
                  <TableCell className="font-mono text-sm">{queueName}</TableCell>
                  <TableCell className="text-right tabular-nums">{count as React.ReactNode}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title={`Active Workers (${data.workers?.count || 0})`}
        helpTitle="Active Workers"
        helpContent={
          <div className="space-y-4 text-sm">
            <p>
              Displays all currently running Celery worker nodes and the tasks they are actively executing.
            </p>
            <p>
              If a worker is listed but has no active tasks, it is idle and waiting for messages from the queue.
            </p>
          </div>
        }
      >
        {activeWorkers.length === 0 ? (
          <p className="text-sm text-muted-foreground">No active workers found.</p>
        ) : (
          <div className="space-y-6">
            {activeWorkers.map(([workerName, tasks]) => (
              <div key={workerName}>
                <h4 className="font-mono text-sm font-semibold mb-2 flex items-center">
                  {workerName}
                  <Badge variant="outline" className="ml-2">
                    {tasks.length} active task{tasks.length === 1 ? '' : 's'}
                  </Badge>
                </h4>
                {tasks.length > 0 ? (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>ID</TableHead>
                        <TableHead>Name</TableHead>
                        <TableHead>Args</TableHead>
                        <TableHead>Start Time</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {tasks.map((task) => (
                        <TableRow key={task.id}>
                          <TableCell className="font-mono text-xs">{task.id}</TableCell>
                          <TableCell className="text-sm font-medium">{task.name}</TableCell>
                          <TableCell className="font-mono text-xs text-muted-foreground">
                            {JSON.stringify(task.args)}
                          </TableCell>
                          <TableCell className="text-xs">
                            {new Date(task.time_start * 1000).toLocaleString()}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                ) : (
                  <p className="text-xs text-muted-foreground italic">Idle</p>
                )}
              </div>
            ))}
          </div>
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title="Ingest Ledger Status"
        helpTitle="Ingest Ledger Status"
        helpContent={
          <div className="space-y-4 text-sm">
            <p>
              Shows the global sum of tracked file ingestion states across all services.
            </p>
            <ul className="list-disc pl-5 space-y-2">
              <li><strong>SUCCESS:</strong> Fully ingested into the system.</li>
              <li><strong>PENDING:</strong> Currently downloading or queued for commit.</li>
              <li><strong>ERROR:</strong> Failed during ingestion and will be retried.</li>
            </ul>
          </div>
        }
      >
        {!data.ledger || Object.keys(data.ledger).length === 0 ? (
          <p className="text-sm text-muted-foreground">Ledger is empty.</p>
        ) : (
          <div className="flex flex-wrap gap-4">
            {Object.entries(data.ledger).map(([status, count]) => (
              <div key={status} className="flex flex-col items-center justify-center p-4 border rounded-lg bg-card min-w-[120px]">
                <span className="text-3xl font-bold">{count}</span>
                <span className="text-xs text-muted-foreground uppercase mt-1">{status}</span>
              </div>
            ))}
          </div>
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title={`RedBeat Schedules (${schedules.length})`}
        helpTitle="RedBeat Schedules"
        helpContent={
          <div className="space-y-4 text-sm">
            <p>
              Displays all registered background schedules (crons/intervals) stored in Redis via RedBeat.
            </p>
            <p>
              These are responsible for ticking recurrent operations like syncing logs, compacting storage, and gathering metrics.
            </p>
          </div>
        }
      >
        {schedules.length === 0 ? (
          <p className="text-sm text-muted-foreground">No schedules found.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Schedule Name</TableHead>
                <TableHead>Task</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {schedules.map((schedule) => (
                <TableRow key={schedule.name}>
                  <TableCell className="font-mono text-xs">{schedule.name}</TableCell>
                  <TableCell className="font-mono text-xs">{schedule.task}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </AnalyticsCard>
    </div>
  )
}
