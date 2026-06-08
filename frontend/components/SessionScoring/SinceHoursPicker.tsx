'use client'

import * as React from 'react'
import { Clock } from 'lucide-react'

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

const PRESETS = [
  { value: 1, label: 'Last 1h' },
  { value: 6, label: 'Last 6h' },
  { value: 24, label: 'Last 24h' },
  { value: 72, label: 'Last 3d' },
  { value: 168, label: 'Last 7d' },
] as const

interface SinceHoursPickerProps {
  value: number
  onChange: (sinceHours: number) => void
}

/**
 * Time-range picker for the session-scoring page. Drives every card
 * that takes a `sinceHours` prop (ScoringHealthCard, ThresholdSlider,
 * TopFlaggedTable, ScoreDistChart, ComplianceChart, RocPrCurves).
 *
 * Capped at 168h (7d) because the backend's threshold-preview /
 * health endpoints already enforce `le=168` — going wider needs a
 * separate "deep history" endpoint that paginates the DuckDB scan.
 */
export function SinceHoursPicker({ value, onChange }: SinceHoursPickerProps) {
  return (
    <Select value={String(value)} onValueChange={(v) => onChange(Number(v) || 24)}>
      <SelectTrigger className="h-8 w-[120px] text-xs">
        <Clock className="h-3.5 w-3.5 mr-1 text-muted-foreground" />
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {PRESETS.map((p) => (
          <SelectItem key={p.value} value={String(p.value)} className="text-xs">
            {p.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}
