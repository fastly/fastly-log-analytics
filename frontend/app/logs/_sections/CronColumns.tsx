'use client'

import React, { useState } from 'react'
import {
  Loader2,
  Copy,
  Check,
} from 'lucide-react'
import { Button } from "@/components/ui/button"
import { Badge } from '@/components/ui/badge'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { DateTimeCell } from '@/components/DataTable'
import { ColumnDef } from '@tanstack/react-table'
import { cn } from '@/lib/utils'
import { CRON_EXPLANATIONS } from './CronExplanations'
import { LiveTimer } from './CronScheduleBox'

export function useCronColumns(isAnalyst: boolean): ColumnDef<any>[] {
  return React.useMemo(() => [
    {
      accessorKey: 'started_at',
      id: 'started_at',
      meta: { label: 'Started At' },
      // Plain-label header — StaticTableHeader supplies the sort button +
      // arrow. A <Button> here nested button-in-button (invalid HTML →
      // hydration mismatch / React #418) and double-fired the sort.
      header: 'Started At',
      cell: ({ row }) => <DateTimeCell iso={row.original.started_at} />
    },
    {
      id: 'finished_at',
      meta: { label: 'Finished At' },
      accessorFn: (row: any) => {
        if (!row.started_at || row.duration_s == null) return null
        return new Date(new Date(row.started_at).getTime() + row.duration_s * 1000).toISOString()
      },
      enableSorting: false,
      header: () => (
        <span className="text-xs font-medium px-2.5">Finished At</span>
      ),
      cell: ({ row }) => {
        if (row.original.status === 'running') {
          return <span className="text-muted-foreground">—</span>
        }
        const startIso = row.original.started_at
        const dur = row.original.duration_s
        if (!startIso || dur == null) {
          return <span className="text-muted-foreground">—</span>
        }
        const finishedIso = new Date(new Date(startIso).getTime() + dur * 1000).toISOString()
        return <DateTimeCell iso={finishedIso} />
      }
    },
    {
      accessorKey: 'task',
      id: 'task',
      meta: { label: 'Task' },
      header: 'Task',
      cell: ({ row }) => {
        const isSync = row.original.task === 'sync' || row.original.task === 'metadata_sync'
        const exp = CRON_EXPLANATIONS[row.original.task] || 'Background job.'
        return (
          <div className="flex flex-col gap-1 py-1">
             <TooltipProvider delay={200}>
               <Tooltip>
                 <TooltipTrigger render={
                   <Badge className={cn("w-fit px-1.5 py-0 shadow-none text-[10px] uppercase font-bold", isSync ? "bg-blue-500/10 text-blue-600 hover:bg-blue-500/20" : "bg-purple-500/10 text-purple-600 hover:bg-purple-500/20")}>
                     {row.original.task === 'metadata_sync' ? 'sync' : row.original.task}
                   </Badge>
                 } />
                 <TooltipContent side="right" className="max-w-[250px] text-xs">
                   <p>{exp}</p>
                 </TooltipContent>
               </Tooltip>
             </TooltipProvider>
             {row.original.summary && <span className="text-[11px] text-muted-foreground whitespace-normal break-words leading-tight">{row.original.summary}</span>}
          </div>
        )
      }
    },
    {
      accessorKey: 'status',
      id: 'status',
      meta: { label: 'Status' },
      header: 'Status',
      cell: ({ row }) => {
        const val = row.original.status
        const err = row.original.error_message
        const [copied, setCopied] = useState(false)

        const handleCopy = (e: React.MouseEvent) => {
          e.stopPropagation()
          if (err) {
            navigator.clipboard.writeText(err)
            setCopied(true)
            setTimeout(() => setCopied(false), 2000)
          }
        }

        if (val === 'running') {
          return (
            <Badge variant="outline" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold border-blue-500/30 text-blue-700 dark:text-blue-400 bg-blue-500/10 flex items-center gap-1 w-fit">
              <Loader2 className="w-3 h-3 animate-spin" />
              Running
            </Badge>
          )
        }
        if (val === 'skipped') {
          return <Badge variant="secondary" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">No Alerts</Badge>
        }
        if (val === 'success' && (!row.original.corrupt_rows || row.original.corrupt_rows === 0)) {
          return <Badge variant="success" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Success</Badge>
        }
        if (val === 'partial_success' || (val === 'success' && row.original.corrupt_rows > 0)) {
          return <Badge variant="warning" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Partial Success</Badge>
        }
        return (
          <div className="flex items-center gap-1.5">
            <Tooltip>
              <TooltipTrigger render={<Badge variant="destructive" className="px-1.5 py-0 shadow-none  uppercase text-[10px] font-bold" />}>
                Error
              </TooltipTrigger>
              <TooltipContent className="max-w-[400px] break-words bg-destructive text-white dark:text-white">
                <p className="text-xs font-mono">{err || 'Unknown error'}</p>
              </TooltipContent>
            </Tooltip>
            {err && (
              <Button
                variant="ghost"
                size="icon"
                aria-label="Copy full error message"
                className="h-6 w-6 text-muted-foreground hover:text-foreground"
                onClick={handleCopy}
                title="Copy full error message"
              >
                {copied ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
              </Button>
            )}
          </div>
        )
      }
    },
    {
      accessorKey: 'duration_s',
      id: 'duration_s',
      meta: { label: 'Duration' },
      header: 'Duration',
      cell: ({ row }) => {
        const s = row.original.duration_s
        if (row.original.status === 'running') {
          // Use a simple localized timer component for running jobs
          return <LiveTimer startedAt={row.original.started_at} />
        }
        const fmt = s < 1 ? `${Math.round(s * 1000)}ms` : s < 60 ? `${s.toFixed(1)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`
        return <span className="font-mono text-muted-foreground tabular-nums text-xs">{fmt}</span>
      }
    },
    {
      accessorKey: 'files_downloaded',
      id: 'files_downloaded',
      meta: { label: 'Files Downloaded' },
      header: 'Files Processed',
      cell: ({ row }) => {
        if (row.original.status === 'running') {
          return (
             <span className="font-mono text-muted-foreground text-xs italic">Processing...</span>
          )
        }

        const task = row.original.task;

        let count = row.original.files_downloaded || 0;
        let label = 'raw logs';

        if (task === 'alerts') {
          label = count === 1 ? 'alert evaluated' : 'alerts evaluated';
        } else if (task === 'commit' || task === 'rum_commit') {
          if (!row.original.rows_ingested) return <span className="text-muted-foreground">—</span>
          // A commit task takes X local buffer files and turns them into 1 cloud file.
          // By eagerly pulling it, we cached that 1 new cloud file.
          count = 1;
          label = 'cloud file cached';
        } else if (task === 'metadata_sync') {
          if (!isAnalyst) return <span className="text-muted-foreground">—</span>
          label = 'cloud files downloaded';
        } else if (task === 'optimize') {
          count = row.original.parquet_files_optimized || 0;
          label = 'files merged';
        } else if (task === 'local_compact') {
          label = 'files merged';
        } else if (task === 'rum_sync') {
          label = 'raw RUM logs';
        } else if (task === 'ngwaf_sync') {
          label = count === 1 ? 'bot record' : 'bot records';
        }

        return (
          <span className="font-mono text-muted-foreground tabular-nums text-xs" title={label}>
            {count.toLocaleString()} <span className="text-[10px] text-muted-foreground">{label}</span>
          </span>
        )
      }
    },
    {
      accessorKey: 'rows_ingested',
      header: isAnalyst ? 'Log Entries Imported' : 'Log Entries Processed',
      cell: ({ row }) => {
        if (row.original.status === 'running') {
           return <span className="font-mono text-muted-foreground text-xs italic">Processing...</span>
        }

        const task = row.original.task
        if (task === 'optimize' || task === 'commit' || task === 'rum_commit' || task === 'local_compact') {
          return <span className="text-muted-foreground">—</span>
        }

        if (task === 'alerts') {
          const triggered = row.original.rows_ingested || 0
          if (triggered === 0) return <span className="text-muted-foreground">—</span>
          return (
            <span className="font-mono tabular-nums text-xs text-amber-500 font-medium">
              {triggered} {triggered === 1 ? 'alert triggered' : 'alerts triggered'}
            </span>
          )
        }

        const rows = row.original.rows_ingested || 0
        const corrupt = row.original.corrupt_rows || 0
        const [copiedCorrupt, setCopiedCorrupt] = useState(false)

        if (task === 'metadata_sync') {
          if (rows === 0) return <span className="text-muted-foreground">—</span>
          return (
            <span className="font-mono text-muted-foreground tabular-nums text-xs">
              {rows.toLocaleString()}
            </span>
          )
        }

        return (
          <div className="flex items-center gap-2">
            <span className="font-mono text-muted-foreground tabular-nums text-xs">
              {rows.toLocaleString()}
            </span>
            {corrupt > 0 && (
              <div className="flex items-center gap-1 group/corrupt">
                <Tooltip>
                  <TooltipTrigger render={<Badge variant="destructive" className="px-1.5 py-0 shadow-none  text-[10px] font-bold" />}>
                    {corrupt.toLocaleString()} Skipped
                  </TooltipTrigger>
                  <TooltipContent>
                    These lines were skipped due to missing timestamps or invalid JSON structure.
                  </TooltipContent>
                </Tooltip>
                {row.original.error_message && (
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label="Copy corrupt-line details"
                    className="h-5 w-5 text-muted-foreground hover:text-foreground opacity-0 group-hover/corrupt:opacity-100 transition-opacity"
                    onClick={(e) => {
                      e.stopPropagation()
                      navigator.clipboard.writeText(row.original.error_message)
                      setCopiedCorrupt(true)
                      setTimeout(() => setCopiedCorrupt(false), 2000)
                    }}
                    title="Copy corrupt lines"
                  >
                    {copiedCorrupt ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
                  </Button>
                )}
              </div>
            )}
          </div>
        )
      }
    },
    ...(isAnalyst ? [] : [
      {
        accessorKey: 'files_deleted_fos',
        header: 'Log Files Deleted',
        cell: ({ row }: any) => {
          if (row.original.status === 'running' || row.original.task !== 'sync') {
             return <span className="text-muted-foreground">—</span>
          }
          return (
            <span className="font-mono text-muted-foreground tabular-nums text-xs">
              {(row.original.files_deleted_fos || 0).toLocaleString()}
            </span>
          )
        }
      },
      {
        id: 'rows_committed',
        header: 'Rows Committed',
        cell: ({ row }: any) => {
          if (row.original.status === 'running') {
             return <span className="font-mono text-muted-foreground text-xs italic">Processing...</span>
          }
          // For commit tasks, rows_ingested holds the rows committed to Iceberg.
          // For sync tasks, this field holds rows written to the local buffer.
          const val = row.original.task === 'commit' ? row.original.rows_ingested : null
          return (
            <span className="font-mono text-muted-foreground tabular-nums text-xs">
              {val !== null ? val.toLocaleString() : <span className="text-muted-foreground">—</span>}
            </span>
          )
        }
      }
    ])
  ], [isAnalyst])
}
