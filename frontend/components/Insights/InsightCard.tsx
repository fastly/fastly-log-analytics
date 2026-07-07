'use client'

import React, { useState } from 'react'
import dynamic from 'next/dynamic'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { InsightCardData } from '@/types/api'
import { cn } from '@/lib/utils'
import { AlertTriangle, Info, CheckCircle, AlertCircle, HelpCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ImpossibleDistanceModal } from './ImpossibleDistanceModal'
import { InsightDataModal } from './InsightDataModal'
import { InsightItemRow } from './InsightItemRow'
import { ImpossibleDistanceData, ScriptedTrafficData } from './types'
import { CacheCollapseModal } from './CacheCollapseModal'
import { ScriptedTrafficModal } from './ScriptedTrafficModal'

// Lazy-load the help modal: its index statically imports ~28KB of
// per-insight help copy (InsightHelpModal/sections/*) that otherwise ships
// in the /insights route chunk even though most users never open a help
// dialog. next/dynamic(ssr:false) defers that chunk to the first help-button
// click. The `helpEverOpened` latch below keeps the modal mounted after the
// first open (so Radix's close animation still plays) while still keeping
// the chunk off the cold-load critical path.
const InsightHelpModal = dynamic(
  () => import('./InsightHelpModal').then((m) => m.InsightHelpModal),
  { ssr: false },
)

interface InsightCardProps {
  insight: InsightCardData
  windowHours: string
  baselineHours: string
}

const SEVERITY_ICON = {
  clean: CheckCircle,
  info: Info,
  warning: AlertTriangle,
  critical: AlertCircle,
  error: AlertCircle,
}

// Icon colors use the -600 ramp on yellow/blue to clear WCAG 1.4.3
// 3:1 non-text contrast on the white card background (yellow-500 ~2.6:1,
// blue-500 ~3.5:1 marginal; -600 lands at ~4.5:1 and ~5:1 respectively).
const SEVERITY_ICON_COLOR = {
  clean: 'text-green-500',
  info: 'text-blue-600',
  warning: 'text-yellow-600',
  critical: 'text-red-500',
  error: 'text-red-600',
}

// Exported so the /insights section headers can reuse the exact same severity
// palette for their per-section rollup chips (keeps card + chip colours in one
// place). Keys match the backend severity ramp.
export const SEVERITY_BADGE_CLASS = {
  clean: 'bg-green-50 text-green-700 border-green-200 dark:bg-green-950/30 dark:text-green-400 dark:border-green-800',
  info: 'bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950/30 dark:text-blue-400 dark:border-blue-800',
  warning: 'bg-yellow-50 text-yellow-700 border-yellow-200 dark:bg-yellow-950/30 dark:text-yellow-400 dark:border-yellow-800',
  critical: 'bg-red-50 text-red-700 border-red-200 dark:bg-red-950/30 dark:text-red-400 dark:border-red-800',
  error: 'bg-red-100 text-red-800 border-red-300 dark:bg-red-950/50 dark:text-red-400 dark:border-red-800',
}

export function InsightCard({ insight, windowHours, baselineHours }: InsightCardProps) {
  const [isHelpOpen, setIsHelpOpen] = useState(false)
  // Latches true on first help-button click so the lazy InsightHelpModal
  // mounts once and stays mounted (close animation), loading its chunk only
  // when a user actually asks for help.
  const [helpEverOpened, setHelpEverOpened] = useState(false)
  const [isDataModalOpen, setIsDataModalOpen] = useState(false)
  const [selectedMapItem, setSelectedMapItem] = useState<ImpossibleDistanceData | null>(null)
  const [selectedCollapseUrl, setSelectedCollapseUrl] = useState<string | null>(null)
  const [selectedScriptedItem, setSelectedScriptedItem] = useState<ScriptedTrafficData | null>(null)

  const Icon = SEVERITY_ICON[insight.severity as keyof typeof SEVERITY_ICON] || AlertCircle
  const iconColor = SEVERITY_ICON_COLOR[insight.severity as keyof typeof SEVERITY_ICON_COLOR] || 'text-muted-foreground'
  const badgeClass = SEVERITY_BADGE_CLASS[insight.severity as keyof typeof SEVERITY_BADGE_CLASS] || ''

  return (
    <>
      <Card className="h-full flex flex-col">
        <CardHeader className="pb-2">
          <div className="flex items-start justify-between gap-2">
            <div className="flex items-center gap-2 min-w-0">
              <Icon className={cn('h-5 w-5 shrink-0', iconColor)} />
              <CardTitle className="text-base leading-tight">{insight.title}</CardTitle>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <Badge
                variant="outline"
                className={cn('text-[11px] sm:text-[10px] uppercase tracking-wider border', badgeClass)}
              >
                {insight.severity}
              </Badge>
              <Button
                variant="ghost"
                size="icon"
                aria-label="How this insight works"
                className="h-6 w-6 text-muted-foreground hover:text-foreground"
                onClick={() => { setHelpEverOpened(true); setIsHelpOpen(true) }}
                title="How this works"
              >
                <HelpCircle className="h-4 w-4" />
              </Button>
            </div>
          </div>
          <CardDescription className="text-xs mt-1">
            {insight.description}
          </CardDescription>
        </CardHeader>

        <CardContent className="flex-1 flex flex-col pt-0">
          <p className="text-sm font-medium my-2 leading-snug">{insight.summary}</p>

          {insight.items && insight.items.length > 0 && (
            <div className="space-y-1.5 mt-1">
              {insight.items.slice(0, 5).map((item, i) => (
                <InsightItemRow
                  key={i}
                  item={item}
                  insightId={insight.id}
                  onMapClick={(data) => setSelectedMapItem(data)}
                  onCacheCollapseClick={(url) => setSelectedCollapseUrl(url)}
                  onScriptedTrafficClick={(data) => setSelectedScriptedItem(data)}
                />
              ))}
              {insight.items.length > 5 && (
                <div className="flex justify-center py-1">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-[11px] sm:text-[10px] h-6 px-2 text-muted-foreground hover:text-foreground"
                    onClick={() => setIsDataModalOpen(true)}
                  >
                    + Show {insight.items.length - 5} more
                  </Button>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {helpEverOpened && (
        <InsightHelpModal
          insightId={insight.id}
          isOpen={isHelpOpen}
          onOpenChange={setIsHelpOpen}
        />
      )}

      <InsightDataModal
        insight={insight}
        isOpen={isDataModalOpen}
        onOpenChange={setIsDataModalOpen}
        onMapClick={(data) => setSelectedMapItem(data)}
        onCacheCollapseClick={(url) => setSelectedCollapseUrl(url)}
        onScriptedTrafficClick={(data) => setSelectedScriptedItem(data)}
      />

      <ImpossibleDistanceModal
        isOpen={!!selectedMapItem}
        onOpenChange={(open) => !open && setSelectedMapItem(null)}
        data={selectedMapItem}
      />

      <CacheCollapseModal
        isOpen={!!selectedCollapseUrl}
        onOpenChange={(open) => !open && setSelectedCollapseUrl(null)}
        url={selectedCollapseUrl}
        windowHours={windowHours}
        baselineHours={baselineHours}
      />

      <ScriptedTrafficModal
        isOpen={!!selectedScriptedItem}
        onOpenChange={(open) => !open && setSelectedScriptedItem(null)}
        data={selectedScriptedItem}
      />
    </>
  )
}
