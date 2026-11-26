'use client'

import React from 'react'
import { useMounted } from '@/hooks/useBootstrap'
import { Terminal, Activity, Bug, Filter, Code2 } from 'lucide-react'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'

export type QueryMode = 'structured' | 'raw'

export function ModeToggle({ mode, onModeChange }: { mode: QueryMode; onModeChange: (next: QueryMode) => void }) {
  return (
    <Tabs value={mode} onValueChange={(v) => onModeChange(v as QueryMode)}>
      <TabsList>
        <TabsTrigger value="structured">
          <Filter className="h-3.5 w-3.5" />
          Structured
        </TabsTrigger>
        <TabsTrigger value="raw">
          <Code2 className="h-3.5 w-3.5" />
          Edit Raw SQL
        </TabsTrigger>
      </TabsList>
    </Tabs>
  )
}

export function StructuredMode({ structuredSql }: { structuredSql: string }) {
  const mounted = useMounted()
  return (
    <div className="p-4 bg-muted/10">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2 font-semibold">
        Generated SQL (sync&apos;d with filter bar)
      </div>
      <pre className="text-xs font-mono whitespace-pre-wrap break-words text-foreground/90 bg-background border rounded p-3 overflow-x-auto min-h-[106px]">
        {mounted
          ? structuredSql
          : 'SELECT *\nFROM logs\nWHERE -- building query preview…\nORDER BY timestamp DESC\nLIMIT 100'}
      </pre>
      <div className="text-[10px] text-muted-foreground mt-2">
        Edit the date range or filters in the header bar above to refine. Click column headers below to change sort
        order — the query re-runs server-side.
      </div>
    </div>
  )
}

export type ToggleDataset = 'logs' | 'client_vitals' | 'client_errors'

const DATASET_OPTIONS: readonly {
  value: ToggleDataset
  label: string
  Icon: React.ComponentType<{ className?: string }>
}[] = [
  { value: 'logs', label: 'CDN Request Logs', Icon: Terminal },
  { value: 'client_vitals', label: 'RUM Web Vitals', Icon: Activity },
  { value: 'client_errors', label: 'RUM JS Errors', Icon: Bug },
]

export function DatasetToggle({
  dataset,
  onDatasetChange,
}: {
  dataset: ToggleDataset
  onDatasetChange: (next: ToggleDataset) => void
}) {
  return (
    <Tabs value={dataset} onValueChange={(v) => onDatasetChange(v as ToggleDataset)}>
      <TabsList className="grid w-full grid-cols-3 max-w-[480px]">
        {DATASET_OPTIONS.map(({ value, label, Icon }) => (
          <TabsTrigger key={value} value={value} className="flex items-center gap-1.5">
            <Icon className="h-3.5 w-3.5" />
            <span className="truncate">{label}</span>
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  )
}

export const RUM_SCHEMAS: Record<string, { name: string; type: string }[]> = {
  client_vitals: [
    { name: 'timestamp', type: 'timestamp' },
    { name: 'metric_name', type: 'string' },
    { name: 'metric_value', type: 'double' },
    { name: 'metric_rating', type: 'string' },
    { name: 'pathname', type: 'string' },
    { name: 'browser', type: 'string' },
    { name: 'os', type: 'string' },
    { name: 'device', type: 'string' },
    { name: 'cid', type: 'string' },
    { name: 'req_id', type: 'string' },
  ],
  client_errors: [
    { name: 'timestamp', type: 'timestamp' },
    { name: 'error_message', type: 'string' },
    { name: 'error_file', type: 'string' },
    { name: 'error_line', type: 'integer' },
    { name: 'error_col', type: 'integer' },
    { name: 'pathname', type: 'string' },
    { name: 'browser', type: 'string' },
    { name: 'os', type: 'string' },
    { name: 'device', type: 'string' },
    { name: 'cid', type: 'string' },
    { name: 'req_id', type: 'string' },
  ],
}

export const DEFAULT_LOG_SCHEMA: { name: string; type: string }[] = [
  { name: 'timestamp', type: 'TIMESTAMPTZ' },
  { name: 'ip', type: 'VARCHAR' },
  { name: 'url', type: 'VARCHAR' },
  { name: 'host', type: 'VARCHAR' },
  { name: 'method', type: 'VARCHAR' },
  { name: 'status', type: 'INTEGER' },
  { name: 'cache', type: 'VARCHAR' },
  { name: 'country', type: 'VARCHAR' },
  { name: 'pop', type: 'VARCHAR' },
  { name: 'asn', type: 'INTEGER' },
  { name: 'ua', type: 'VARCHAR' },
]
