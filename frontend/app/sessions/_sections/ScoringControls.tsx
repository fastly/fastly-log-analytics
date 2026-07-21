'use client'

import React from 'react'
import { AlertTriangle, Clock, Film } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { cn } from '@/lib/utils'

interface ScoringControlsProps {
  flaggedOnly: boolean
  setFlaggedOnly: (v: boolean) => void
  streamingOnly: boolean
  setStreamingOnly: (v: boolean) => void
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
  streamingOnly,
  setStreamingOnly,
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

      {data?.has_cmcd && (
        <div className="flex items-center gap-2">
          <Switch
            id="streaming-only"
            checked={streamingOnly}
            onCheckedChange={setStreamingOnly}
          />
          <Label htmlFor="streaming-only" className="text-sm cursor-pointer flex items-center gap-1">
            <Film className="h-3.5 w-3.5 text-blue-500" /> Streaming sessions
          </Label>
        </div>
      )}

      <div className="flex items-center gap-2">
        <Label className="text-xs text-muted-foreground whitespace-nowrap">Min. requests</Label>
        <Input
          type="number"
          min={0}
          value={minReqs}
          onChange={e => setMinReqs(e.target.value === '' ? '' : Number(e.target.value))}
          // Prefix with "≥" so the placeholder reads as a hint (the scoring
          // system's default flag threshold) rather than a value already set
          // on this filter — the input itself starts empty and rows are
          // unfiltered until a value is typed.
          placeholder={`≥ ${data?.min_reqs_flag ?? 1000}`}
          className="h-8 w-24 text-sm text-right"
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
          placeholder={`≥ ${data?.min_4xx_pct_flag ?? 20}`}
          className="h-8 w-24 text-sm text-right"
        />
      </div>

      {(flaggedOnly || streamingOnly || minReqs !== '' || min4xxPct !== '') && (
        <Button
          variant="ghost"
          size="sm"
          className="h-8 text-xs ml-auto"
          onClick={() => { setFlaggedOnly(false); setStreamingOnly(false); setMinReqs(''); setMin4xxPct('') }}
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
