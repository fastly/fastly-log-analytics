'use client'

import React from 'react'
import { AlertTriangle, Clock } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { cn } from '@/lib/utils'

interface ScoringControlsProps {
  flaggedOnly: boolean
  setFlaggedOnly: (v: boolean) => void
  minReqs: number | ''
  setMinReqs: (v: number | '') => void
  min4xxPct: number | ''
  setMin4xxPct: (v: number | '') => void
  data: any
  isFetching: boolean
  isLoadingInitial: boolean
  refetch: () => void
}

export function ScoringControls({
  flaggedOnly,
  setFlaggedOnly,
  minReqs,
  setMinReqs,
  min4xxPct,
  setMin4xxPct,
  data,
  isFetching,
  isLoadingInitial,
  refetch,
}: ScoringControlsProps) {
  return (
    <div className={cn("flex flex-wrap items-center gap-4 p-3 border rounded-lg bg-muted/30 transition-opacity duration-100", isFetching && !isLoadingInitial && "opacity-40 pointer-events-none")}>
      <div className="flex items-center gap-2">
        <Switch
          id="flagged-only"
          checked={flaggedOnly}
          onCheckedChange={setFlaggedOnly}
        />
        <Label htmlFor="flagged-only" className="text-sm cursor-pointer flex items-center gap-1">
          <AlertTriangle className="h-3.5 w-3.5 text-yellow-500" /> Flagged only
        </Label>
      </div>

      <div className="flex items-center gap-2">
        <Label className="text-xs text-muted-foreground whitespace-nowrap">Min. requests</Label>
        <Input
          type="number"
          min={0}
          value={minReqs}
          onChange={e => setMinReqs(e.target.value === '' ? '' : Number(e.target.value))}
          placeholder={data?.min_reqs_flag?.toString() ?? "1000"}
          className="h-8 w-20 text-sm text-right"
        />
      </div>

      <div className="flex items-center gap-2">
        <Label className="text-xs text-muted-foreground whitespace-nowrap">Min. 4xx%</Label>
        <Input
          type="number"
          min={0}
          max={100}
          value={min4xxPct}
          onChange={e => setMin4xxPct(e.target.value === '' ? '' : Number(e.target.value))}
          placeholder={data?.min_4xx_pct_flag?.toString() ?? "20"}
          className="h-8 w-20 text-sm text-right"
        />
      </div>

      {(flaggedOnly || minReqs !== '' || min4xxPct !== '') && (
        <Button
          variant="ghost"
          size="sm"
          className="h-8 text-xs ml-auto"
          onClick={() => { setFlaggedOnly(false); setMinReqs(''); setMin4xxPct('') }}
        >
          Clear filters
        </Button>
      )}

      <Button
        variant="outline"
        size="sm"
        className="h-8 ml-auto"
        onClick={() => refetch()}
        disabled={isFetching}
      >
        {isFetching ? <Clock className="h-3.5 w-3.5 mr-2 animate-spin" /> : <Clock className="h-3.5 w-3.5 mr-2" />}
        Refresh
      </Button>
    </div>
  )
}
