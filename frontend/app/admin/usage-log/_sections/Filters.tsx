'use client'

import React from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { DATE_PRESETS } from './shared'

export interface UsageLogFiltersProps {
  preset: number
  setPreset: (n: number) => void
  usageType: string
  setUsageType: (v: string) => void
  operationFilter: string
  setOperationFilter: (v: string) => void
  processFilter: string
  setProcessFilter: (v: string) => void
  isFetching: boolean
  isLoading: boolean
}

export function UsageLogFilters({
  preset,
  setPreset,
  usageType,
  setUsageType,
  operationFilter,
  setOperationFilter,
  processFilter,
  setProcessFilter,
  isFetching,
  isLoading,
}: UsageLogFiltersProps) {
  return (
    <div className="flex flex-wrap items-center gap-3 px-4 py-3 border-b">
      <div className="flex items-center gap-1.5">
        {DATE_PRESETS.map(p => (
          <Button
            key={p.hours}
            size="sm"
            variant={preset === p.hours ? 'default' : 'outline'}
            className="h-7 px-3 text-xs"
            onClick={() => setPreset(p.hours)}
          >
            {p.label}
          </Button>
        ))}
      </div>

      <div className="flex items-center gap-1.5">
        <Label className="text-xs text-muted-foreground shrink-0">Type</Label>
        <Select value={usageType || 'all'} onValueChange={v => setUsageType(!v || v === 'all' ? '' : v)}>
          <SelectTrigger className="h-7 text-xs w-32">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all" className="text-xs">All</SelectItem>
            <SelectItem value="FOS" className="text-xs">FOS (A+B)</SelectItem>
            <SelectItem value="FOS-A" className="text-xs">FOS Class A</SelectItem>
            <SelectItem value="FOS-B" className="text-xs">FOS Class B</SelectItem>
            <SelectItem value="CDN" className="text-xs">CDN</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="flex items-center gap-1.5">
        <Label className="text-xs text-muted-foreground shrink-0">Operation</Label>
        <Input
          className="h-7 text-xs w-40 font-mono"
          placeholder="e.g. GET_OBJECT"
          value={operationFilter}
          onChange={e => setOperationFilter(e.target.value)}
        />
      </div>

      <div className="flex items-center gap-1.5">
        <Label className="text-xs text-muted-foreground shrink-0">Process</Label>
        <Input
          className="h-7 text-xs w-44 font-mono"
          placeholder="e.g. cron:sync"
          value={processFilter}
          onChange={e => setProcessFilter(e.target.value)}
        />
      </div>

      {isFetching && !isLoading && (
        <span className="text-xs text-muted-foreground animate-pulse">Refreshing…</span>
      )}
    </div>
  )
}
