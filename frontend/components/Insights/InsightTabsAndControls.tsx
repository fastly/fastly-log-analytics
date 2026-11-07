'use client'

import React from 'react'
import {
  CheckCircle,
  Filter,
  Info,
  Loader2,
  RefreshCw,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import {
  BASELINE_OPTIONS,
  cn,
  normalizeTabKey,
  SECTION_LOADING_MESSAGES,
  summarizeSeverities,
  WINDOW_OPTIONS,
  type InsightSectionGroup,
  type SectionableInsight,
  type SeveritySummary,
} from '@/lib/utils'
import { SEVERITY_BADGE_CLASS } from './shared'

export const STATUS_OPTIONS = [
  { label: 'All Statuses', value: 'all' },
  { label: 'Critical', value: 'critical' },
  { label: 'Warning', value: 'warning' },
  { label: 'Info', value: 'info' },
  { label: 'Clean', value: 'clean' },
  // `error` severity insights (the insight computation itself failed) were
  // silently dropped from every non-"all" filter without this option.
  { label: 'Error', value: 'error' },
]

// Full per-severity rollup chips (e.g. "2 CRITICAL" "3 WARNING" "4 CLEAN"),
// reusing the exact card palette (SEVERITY_BADGE_CLASS) so the panel header and
// the cards read the same. Shown in the ACTIVE panel header — the whole
// breakdown, not the compact critical/warning-only badges on the tab triggers.
// Renders nothing when there are no known severities (skeleton entries have
// none), so the loading panel header shows only the count.
export function SeverityChips({ severities }: { severities: SeveritySummary[] }) {
  if (severities.length === 0) return null
  return (
    <span className="ml-1 flex flex-wrap items-center gap-1">
      {severities.map(({ severity, count }) => (
        <Badge
          key={severity}
          variant="outline"
          className={cn(
            'border px-1.5 py-0 text-[10px] font-medium uppercase tracking-wide tabular-nums',
            SEVERITY_BADGE_CLASS[severity as keyof typeof SEVERITY_BADGE_CLASS] ?? '',
          )}
        >
          {count} {severity}
        </Badge>
      ))}
    </span>
  )
}

// Compact per-tab severity badges: colored count-only pills for the high-signal
// severities (critical=red, warning=amber). clean/info/error are folded away
// here to keep the tab row scannable — the FULL breakdown lives in the active
// panel header (SeverityChips). Screen readers still get the severity name via
// the sr-only span so a bare "2" isn't ambiguous. Colors reuse the AA-cleared
// SEVERITY_BADGE_CLASS palette (axe color-contrast gate).
const TAB_BADGE_SEVERITIES = new Set(['critical', 'warning'])

const TAB_LABEL_LINES: Record<string, [string, string]> = {
  all: ['All', 'Categories'],
  security: ['Security &', 'Threat Detection'],
  origin: ['Origin Health &', 'Stability'],
  edge: ['Edge & Delivery', 'Performance'],
  network: ['Network &', 'Routing Path'],
  traffic: ['Traffic &', 'Volumetrics'],
}

export function TabSeverityBadges({ severities }: { severities: SeveritySummary[] }) {
  const compact = severities.filter((s) => TAB_BADGE_SEVERITIES.has(s.severity))
  if (compact.length === 0) return null
  return (
    <span className="inline-flex h-[18px] shrink-0 items-center gap-1">
      {compact.map(({ severity, count }) => (
        <span
          key={severity}
          className={cn(
            'inline-flex min-w-[1.125rem] items-center justify-center rounded-full border px-1 text-[10px] font-semibold leading-4 tabular-nums',
            SEVERITY_BADGE_CLASS[severity as keyof typeof SEVERITY_BADGE_CLASS] ?? '',
          )}
        >
          {count}
          <span className="sr-only"> {severity}</span>
        </span>
      ))}
    </span>
  )
}

// The tabs UI. One tab per section in fixed INSIGHT_SECTIONS order
// (All Categories first, then security, origin, edge, network, traffic, + Other last),
// the tab bar rendered from `groups` (skeleton groups during
// load, real groups after, so the row never pops in — CLS). base-ui Tabs give
// roving-tabindex + arrow-key nav for free, and Tabs.Panel UNMOUNTS inactive
// panels by default (keepMounted defaults false) — so a section's cards only
// render when its tab is active (the "load when I click it" behaviour the user
// asked for, even though /api/insights returns everything in one call).
//
// The active tab is controlled by the parent so the selection survives the
// loading→loaded swap and status-filter changes; `activeKey` falls back to the
// first group whenever the current selection isn't among the rendered groups.
export function InsightTabs<T extends SectionableInsight>({
  groups,
  activeTab,
  onTabChange,
  isLoading,
  renderCards,
  emptyMessage,
  isCounted = () => true,
}: {
  groups: InsightSectionGroup<T>[]
  activeTab: string | null
  onTabChange: (key: string) => void
  isLoading?: boolean
  renderCards: (group: InsightSectionGroup<T>) => React.ReactNode
  emptyMessage?: (group: InsightSectionGroup<T>) => { title: string; description: string }
  // Cards excluded here (queued placeholders) still render but are not counted as findings.
  isCounted?: (card: T) => boolean
}) {
  const normalizedActive = normalizeTabKey(activeTab)
  const activeKey =
    groups.some((g) => g.section.key === normalizedActive)
      ? normalizedActive
      : groups[0]?.section.key ?? 'all'

  return (
    <Tabs value={activeKey} onValueChange={(value) => onTabChange(normalizeTabKey(String(value)))}>
      {/* Single-row horizontal tab bar on desktop (lg:flex-nowrap) where each
          trigger sizes proportionally to its content (flex-auto) with compact
          horizontal icon + label + count/badges layout. */}
      <TabsList className="group-data-horizontal/tabs:h-auto flex w-full flex-wrap items-stretch justify-start gap-0.5 rounded-xl border border-border/60 p-1 lg:flex-nowrap min-[1400px]:gap-1 min-[1600px]:gap-1.5 min-[1600px]:p-1.5">
        {groups.map(({ section, cards }) => {
          const Icon = section.icon
          const lines = TAB_LABEL_LINES[section.key]
          return (
            <TabsTrigger
              key={section.key}
              value={section.key}
              data-tab={section.key}
              data-category={section.key}
              data-alias={section.key === 'edge' ? 'performance' : section.key === 'network' ? 'routing' : undefined}
              className="flex h-auto min-h-[2.75rem] flex-auto items-center justify-between gap-1 rounded-lg border border-border/80 bg-background px-1 py-1.5 text-[11px] shadow-2xs hover:bg-accent/60 data-active:border-primary data-active:bg-primary data-active:font-semibold data-active:text-primary-foreground dark:border-border dark:bg-card/80 dark:hover:bg-card min-[1400px]:gap-1.5 min-[1400px]:px-2 min-[1400px]:text-xs min-[1600px]:gap-2 min-[1600px]:px-3"
            >
              <span className="inline-flex items-center gap-1 text-left min-[1400px]:gap-1.5 min-[1600px]:gap-2">
                <Icon className="size-3 shrink-0 min-[1400px]:size-3.5" aria-hidden="true" />
                {lines ? (
                  <span className="text-left leading-tight">
                    <span className="block whitespace-nowrap">{lines[0]}</span>{' '}
                    <span className="block whitespace-nowrap">{lines[1]}</span>
                  </span>
                ) : (
                  <span className="whitespace-nowrap text-left leading-tight">{section.label}</span>
                )}
              </span>
              <span className="inline-flex shrink-0 items-center gap-1">
                <span className="text-xs tabular-nums">{cards.filter(isCounted).length}</span>
                <TabSeverityBadges severities={summarizeSeverities(cards.filter(isCounted))} />
              </span>
            </TabsTrigger>
          )
        })}
      </TabsList>

      {groups.map((group) => {
        const { section, cards } = group
        const Icon = section.icon
        const empty = emptyMessage?.(group)
        return (
          <TabsContent
            key={section.key}
            value={section.key}
            data-tab-content={section.key}
            data-alias={section.key === 'edge' ? 'performance' : section.key === 'network' ? 'routing' : undefined}
            className="mt-4 space-y-4"
          >
            <div>
              <h2 className="flex flex-wrap items-center gap-2 text-lg font-semibold tracking-tight">
                <Icon className="h-5 w-5 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span>{section.label}</span>
                <span className="text-xs font-normal text-muted-foreground tabular-nums">
                  ({cards.filter(isCounted).length})
                </span>
                <SeverityChips severities={summarizeSeverities(cards.filter(isCounted))} />
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">{section.description}</p>
            </div>
            {cards.length === 0 ? (
              isLoading ? (
                <div
                  className="text-center py-20 text-sm text-muted-foreground min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]"
                  data-testid="insights-loading"
                >
                  <Loader2 className="inline-block animate-spin h-5 w-5 mb-2 text-primary" />
                  <span>{SECTION_LOADING_MESSAGES[section.key] ?? 'Loading insights…'}</span>
                </div>
              ) : (
                <div
                  className="py-16 text-center border rounded-xl border-dashed bg-muted/10 min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]"
                  data-testid="insights-empty-pass"
                >
                  {empty ? <Info className="h-10 w-10 text-muted-foreground mb-3" /> : <CheckCircle className="h-10 w-10 text-emerald-500 mb-3" />}
                  <h3 className="text-base font-semibold">
                    {empty ? empty.title : section.key === 'all'
                      ? 'No anomalies detected'
                      : `No anomalies detected in ${section.label}`}
                  </h3>
                  <p className="text-xs text-muted-foreground mt-1 max-w-sm">
                    {empty ? empty.description : section.key === 'all'
                      ? 'All monitored metrics across all categories are operating within normal baseline parameters.'
                      : 'All monitored metrics in this category are operating within normal baseline parameters.'}
                  </p>
                </div>
              )
            ) : (
              renderCards(group)
            )}
          </TabsContent>
        )
      })}
    </Tabs>
  )
}

export interface InsightsHeaderControlsProps {
  statusFilter: string
  setStatusFilter: (val: string) => void
  windowHours: string
  setWindowHours: (val: string) => void
  baselineHours: string
  setBaselineHours: (val: string) => void
  isRemoteAnalyst: boolean
  isRescanning: boolean
  activeServiceId: string | null | undefined
  onRescan: () => void
}

export function InsightsHeaderControls({
  statusFilter,
  setStatusFilter,
  windowHours,
  setWindowHours,
  baselineHours,
  setBaselineHours,
  isRemoteAnalyst,
  isRescanning,
  activeServiceId,
  onRescan,
}: InsightsHeaderControlsProps) {
  return (
    <div className="flex items-end gap-2">
      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Status Filter</span>
          <span className="w-3 h-3 shrink-0" />
        </div>
        <Select value={statusFilter} onValueChange={(val) => val && setStatusFilter(val)}>
          <SelectTrigger className="w-[140px]">
            <Filter className="w-3 h-3 mr-2 opacity-50" />
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            {STATUS_OPTIONS.map((o) => (
              <SelectItem key={o.value} value={o.value}>
                {o.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex flex-col gap-1 ml-2">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Window</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="inline-block w-3 h-3 opacity-50" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[200px] text-xs">
                The current time period you want to check for anomalies
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <Select value={windowHours} onValueChange={(val) => val && setWindowHours(val)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Window" />
          </SelectTrigger>
          <SelectContent>
            {WINDOW_OPTIONS.map((o) => (
              <SelectItem key={o.value} value={o.value}>
                {o.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <span className="text-xs font-bold text-muted-foreground uppercase tracking-widest pb-2">vs</span>

      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Baseline</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="inline-block w-3 h-3 opacity-50" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[200px] text-xs">
                The historical period to compare against (acts as the &apos;normal&apos; baseline)
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <Select value={baselineHours} onValueChange={(val) => val && setBaselineHours(val)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Baseline" />
          </SelectTrigger>
          <SelectContent>
            {BASELINE_OPTIONS.map((o) => (
              <SelectItem key={o.value} value={o.value}>
                {o.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {!isRemoteAnalyst && (
        <div className="flex flex-col gap-1 ml-2 justify-end">
          <Button
            variant="outline"
            size="sm"
            onClick={onRescan}
            disabled={isRescanning || !activeServiceId}
            className="h-9 gap-1.5 px-3 text-xs font-medium"
            title="Force re-evaluation of all anomaly detectors against recent logs"
          >
            <RefreshCw className={cn('h-3.5 w-3.5', isRescanning && 'animate-spin')} />
            <span>{isRescanning ? 'Scanning…' : 'Re-scan'}</span>
          </Button>
        </div>
      )}
    </div>
  )
}
