// frontend/app/rum/_sections/RumClient.tsx
'use client';

import React, { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useAnalyticsReportQuery } from '@/hooks/useAnalyticsReportQuery';
import { AnalyticsCard } from '@/components/AnalyticsCard';
import { RumStatusPanel } from '@/components/Rum/RumStatusPanel';
import { ReportLayout } from '@/components/ReportLayout';
import { RumFaroVersionCard } from '@/components/Rum/RumFaroVersionCard';
import { useIsAnalyst } from '@/hooks/useIsAnalyst';
import { useReportRangeAnchor } from '@/hooks/useReportRangeAnchor';
import { CopyButton } from '@/components/ui/copy-button';

import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton';
import { PlotlyChart } from '@/components/PlotlyChart';
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart';
import { useTimezone } from '@/hooks/useDateFormat';
import { formatDate } from '@/lib/date';
import { Terminal, Activity, Monitor, ShieldAlert, Cpu, PieChart, TrendingUp, Eye, Radio, BookOpen } from 'lucide-react';
import { adminJsonResult } from '@/lib/api';
import { arrayOrEmpty, recordOrNull } from '@/lib/utils';
import type { StripIndex, WithRequired, components } from '@/types/api';
import type { FiltersPayload } from '@/types/filters';
import type { RangeWireArgs } from '@/lib/range-wire';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { DataTable, DataTableReadonly } from '@/components/DataTable';
import { ColumnDef } from '@tanstack/react-table';
import { useRumStatus } from '@/hooks/useServiceQuery';
import { CHART_COLORS } from '@/lib/chart-builders'

import { formatCount } from '@/lib/format'
interface RumClientProps extends Pick<RangeWireArgs, 'relativeRange' | 'isAutoRange' | 'anchor'> {
  serviceId: string | null;
  startTime: string | null;
  endTime: string | null;
  filterPayload: FiltersPayload;
}

interface CwvColors {
  text: string;
  bg: string;
  label: string;
}

type RumWire = components['schemas']['RumAnalyticsResponse']
type RumWorstPage = components['schemas']['RumWorstPage']
type RumJsError = components['schemas']['RumJsError']
type RumCwvDistribution = components['schemas']['RumCwvDistribution']

// Every data-branch response carries these; the all-optional wire model only
// omits them on the 500 path, which never reaches this consumer.
type RumAnalytics = WithRequired<
  RumWire,
  'beacon_count' | 'vitals' | 'worst_pages' | 'errors' | 'trends' | 'environments'
>

// Edge-log blob attached to each beacon. The keys read by the table are typed;
// whatever else the producer (or a Faro payload) carries rides the index signature.
interface RumRawLog {
  city?: string;
  region?: string;
  country?: string;
  pop?: string;
  tls?: string;
  ttfb?: number | string | null;
  faro_payload?: unknown;
  meta?: unknown;
  measurements?: unknown;
  events?: unknown;
  [key: string]: unknown;
}

type RumLiveEvent = Omit<StripIndex<components['schemas']['RumLiveEventItem']>, 'browser' | 'os' | 'raw_log'> & {
  // Legacy payloads carried ``{ name }`` objects here instead of plain strings.
  browser: components['schemas']['RumLiveEventItem']['browser'] | { name?: string };
  os: components['schemas']['RumLiveEventItem']['os'] | { name?: string };
  raw_log?: RumRawLog;
}

const WORST_PAGE_COLUMNS: ColumnDef<RumWorstPage>[] = [
  {
    id: 'path',
    header: 'Path',
    accessorFn: (p) => p.path,
    size: 260,
    cell: ({ row }) => <span className="font-semibold font-mono text-xs">{row.original.path}</span>,
  },
  {
    id: 'views',
    header: 'Pageviews',
    accessorFn: (p) => p.views,
    size: 110,
    cell: ({ row }) => <span className="block text-right text-sm">{row.original.views}</span>,
  },
  {
    id: 'avg_load_time',
    header: 'Avg Load (s)',
    accessorFn: (p) => p.avg_load_time ?? -1,
    size: 120,
    cell: ({ row }) => (
      <span className="block text-right text-sm">
        {row.original.avg_load_time != null ? `${row.original.avg_load_time.toFixed(1)}s` : '—'}
      </span>
    ),
  },
  {
    id: 'lcp_p75',
    header: 'LCP p75',
    accessorFn: (p) => p.lcp_p75 ?? -1,
    size: 100,
    cell: ({ row }) => (
      <span className="block text-right text-sm text-amber-500">
        {row.original.lcp_p75 != null ? `${row.original.lcp_p75.toFixed(2)}s` : '—'}
      </span>
    ),
  },
  {
    id: 'cls_p75',
    header: 'CLS p75',
    accessorFn: (p) => p.cls_p75 ?? -1,
    size: 100,
    cell: ({ row }) => (
      <span className="block text-right text-sm">
        {row.original.cls_p75 != null ? row.original.cls_p75.toFixed(2) : '—'}
      </span>
    ),
  },
]

function envName(v: string | { name?: string } | null | undefined): string {
  if (typeof v === 'object' && v) return v.name || 'Unknown';
  return v || 'Unknown';
}

function getThresholdColors(
  p75: number | null | undefined,
  goodMax: number,
  poorMin: number,
): CwvColors {
  if (p75 == null) {
    return { text: 'text-muted-foreground', bg: 'bg-muted/10', label: 'No Data' };
  }
  if (p75 <= goodMax) {
    return { text: 'text-emerald-500', bg: 'bg-emerald-500/10', label: 'Good' };
  }
  if (p75 > poorMin) {
    return { text: 'text-rose-500', bg: 'bg-rose-500/10', label: 'Poor' };
  }
  return { text: 'text-amber-500', bg: 'bg-amber-500/10', label: 'Needs Improvement' };
}

function getLcpColors(p75: number | null | undefined): CwvColors {
  return getThresholdColors(p75, 2.5, 4.0);
}

function getClsColors(p75: number | null | undefined): CwvColors {
  return getThresholdColors(p75, 0.1, 0.25);
}

function getInpColors(p75: number | null | undefined): CwvColors {
  return getThresholdColors(p75, 200, 500);
}

function buildEnvPieData(envMap: Record<string, number> | undefined) {
  return [
    {
      labels: Object.keys(envMap || {}),
      values: Object.values(envMap || {}),
      type: 'pie' as const,
      hole: 0.4,
      textinfo: 'label+percent' as const,
    },
  ];
}

export function RumClient({
  serviceId,
  startTime,
  endTime,
  filterPayload,
  relativeRange,
  isAutoRange,
  anchor,
}: RumClientProps) {
  const timezone = useTimezone();
  const [selectedEvent, setSelectedEvent] = useState<RumLiveEvent | null>(null);

  const columns = React.useMemo<ColumnDef<RumLiveEvent>[]>(() => [
    {
      accessorKey: 'type',
      id: 'type',
      meta: { label: 'Status' },
      header: 'Status',
      cell: ({ row }) => {
        const type = row.original.type;
        return (
          <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-full text-[10px] font-bold ${
            type === 'error'
              ? 'bg-rose-500/10 text-rose-500 border border-rose-500/20'
              : 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20'
          }`}>
            <span className={`h-1.5 w-1.5 rounded-full ${type === 'error' ? 'bg-rose-500' : 'bg-emerald-500'}`} />
            {type === 'error' ? 'Error' : 'Success'}
          </span>
        );
      }
    },
    {
      accessorKey: 'time',
      id: 'time',
      meta: { label: 'Time' },
      header: 'Time',
      cell: ({ row }) => (
        <span className="font-mono text-xs text-muted-foreground whitespace-nowrap">
          {formatDate(row.original.time, timezone, 'HH:mm:ss')}
        </span>
      )
    },
    {
      accessorKey: 'path',
      id: 'path',
      meta: { label: 'Page Path' },
      header: 'Page Path',
      cell: ({ row }) => (
        <span className="font-semibold font-mono text-xs text-zinc-300 block max-w-[200px] truncate" title={row.original.path}>
          {row.original.path}
        </span>
      )
    },
    {
      accessorKey: 'desc',
      id: 'desc',
      meta: { label: 'Description' },
      header: 'Description',
      cell: ({ row }) => (
        <span className="text-muted-foreground text-xs block max-w-[250px] truncate" title={row.original.desc}>
          {row.original.desc}
        </span>
      )
    },
    {
      id: 'environment',
      accessorFn: (row) => {
        return `${envName(row.browser)} on ${envName(row.os)}`;
      },
      meta: { label: 'Environment' },
      header: 'Environment',
      cell: ({ row }) => {
        const b = envName(row.original.browser);
        const o = envName(row.original.os);
        return (
          <span className="text-muted-foreground text-xs block max-w-[150px] truncate" title={`${b} on ${o}`}>
            {b} on {o}
          </span>
        );
      }
    },
    {
      id: 'geo',
      accessorFn: (row) => {
        const raw = row.raw_log || {};
        return `${raw.city || ''}, ${raw.country || ''}`;
      },
      meta: { label: 'Location' },
      header: 'Location',
      cell: ({ row }) => {
        const raw = row.original.raw_log || {};
        const location = [raw.city, raw.region, raw.country].filter(Boolean).join(', ');
        return (
          <span className="text-muted-foreground text-xs block max-w-[150px] truncate" title={location}>
            {location || '—'}
          </span>
        );
      }
    },
    {
      id: 'pop',
      accessorFn: (row) => (row.raw_log || {}).pop || '',
      meta: { label: 'Edge POP' },
      header: 'Edge POP',
      cell: ({ row }) => {
        const raw = row.original.raw_log || {};
        return (
          <span className="font-mono text-xs text-muted-foreground uppercase">
            {raw.pop || '—'}
          </span>
        );
      }
    },
    {
      id: 'tls',
      accessorFn: (row) => (row.raw_log || {}).tls || '',
      meta: { label: 'TLS' },
      header: 'TLS',
      cell: ({ row }) => {
        const raw = row.original.raw_log || {};
        return (
          <span className="font-mono text-xs text-muted-foreground">
            {raw.tls ? `TLS ${raw.tls}` : '—'}
          </span>
        );
      }
    },
    {
      id: 'ttfb',
      accessorFn: (row) => {
        const raw = row.raw_log || {};
        return raw.ttfb != null ? parseFloat(String(raw.ttfb)) : null;
      },
      meta: { label: 'TTFB (Edge)' },
      header: 'TTFB (Edge)',
      cell: ({ row }) => {
        const raw = row.original.raw_log || {};
        return (
          <span className="font-mono text-xs text-muted-foreground">
            {raw.ttfb != null ? `${raw.ttfb}ms` : '—'}
          </span>
        );
      }
    },
    {
      id: 'actions',
      header: () => <span className="sr-only">Actions</span>,
      cell: ({ row }) => (
        <div className="text-right">
          <Button
            variant="outline"
            size="sm"
            onClick={(e) => {
              e.stopPropagation();
              setSelectedEvent(row.original);
            }}
            className="h-8 text-xs font-medium"
          >
            Details
          </Button>
        </div>
      )
    }
  ], [timezone]);

  // Fetch status
  const { data: status, isLoading: isStatusLoading } = useRumStatus(serviceId);

  // Fetch analytics
  const { data: analytics, isLoading } = useAnalyticsReportQuery<RumAnalytics | null>({
    namespace: ['rum-analytics'],
    serviceId,
    filterPayload,
    range: { relativeRange, isAutoRange, startTime, endTime, anchor },
    queryFn: async ({ rangeBody }) => {
      if (!serviceId) return null;
      const params = new URLSearchParams();
      for (const [k, v] of Object.entries(rangeBody)) {
        if (v) params.append(k, v);
      }
      if (filterPayload && Object.keys(filterPayload).length > 0) {
        params.append('filters', JSON.stringify(filterPayload));
      }
      const qs = params.toString();
      const url = `/api/services/${serviceId}/rum/analytics${qs ? `?${qs}` : ''}`;
      const res = await adminJsonResult<'/api/services/{service_id}/rum/analytics', RumAnalytics>(
        '/api/services/{service_id}/rum/analytics',
        url,
      );
      if (!res.ok) {
        throw new Error(res.errorText || `RUM analytics failed (HTTP ${res.status})`);
      }
      return recordOrNull(res.data);
    },
    queryOptions: { enabled: !!serviceId && !!status?.enabled, refetchInterval: false },
  });

  const formattedTimestamps = React.useMemo(() => {
    return (analytics?.trends?.timestamps || []).map((t: string) =>
      formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss')
    );
  }, [analytics?.trends?.timestamps, timezone]);

  const trendChartLayout = React.useMemo(
    () => ({
      margin: { t: 30, b: 40, l: 30, r: 10 },
      legend: {
        orientation: 'h' as const,
        y: 1.08,
        x: 0.5,
        xanchor: 'center' as const,
        yanchor: 'bottom' as const,
      },
    }),
    [],
  );

  const stackedBarLayout = React.useMemo(
    () => ({
      barmode: 'stack' as const,
      margin: { t: 30, b: 40, l: 30, r: 10 },
      legend: {
        orientation: 'h' as const,
        y: 1.08,
        x: 0.5,
        xanchor: 'center' as const,
        yanchor: 'bottom' as const,
      },
    }),
    [],
  );

  const donutChartLayout = React.useMemo(
    () => ({
      height: 250,
      margin: { t: 10, b: 10, l: 10, r: 10 },
      showlegend: false,
    }),
    [],
  );

  const webVitalsTrendData = React.useMemo(
    () => [
      {
        x: formattedTimestamps,
        y: analytics?.trends?.lcp || [],
        name: 'LCP (s)',
        type: 'scatter' as const,
        mode: 'lines+markers' as const,
        line: { color: CHART_COLORS.success },
      },
      {
        x: formattedTimestamps,
        y: analytics?.trends?.cls || [],
        name: 'CLS',
        type: 'scatter' as const,
        mode: 'lines+markers' as const,
        line: { color: CHART_COLORS.info },
      },
    ],
    [formattedTimestamps, analytics?.trends?.lcp, analytics?.trends?.cls],
  );

  const errorRateTrendData = React.useMemo(
    () => [
      {
        x: formattedTimestamps,
        y: analytics?.trends?.error_rate || [],
        name: 'Error Rate (%)',
        type: 'scatter' as const,
        mode: 'lines+markers' as const,
        line: { color: '#f43f5e' },
        fill: 'tozeroy' as const,
      },
    ],
    [formattedTimestamps, analytics?.trends?.error_rate],
  );

  const browserPieData = React.useMemo(
    () => buildEnvPieData(analytics?.environments?.browsers),
    [analytics?.environments?.browsers],
  );

  const osPieData = React.useMemo(
    () => buildEnvPieData(analytics?.environments?.os),
    [analytics?.environments?.os],
  );

  const devicePieData = React.useMemo(
    () => buildEnvPieData(analytics?.environments?.devices),
    [analytics?.environments?.devices],
  );

  const beaconsOverTimeData = React.useMemo(
    () => [
      {
        x: formattedTimestamps,
        y: analytics?.trends?.pageviews || [],
        name: 'Pageviews',
        type: 'bar' as const,
        marker: { color: CHART_COLORS.info },
      },
      {
        x: formattedTimestamps,
        y: analytics?.trends?.interactions || [],
        name: 'Interactions',
        type: 'bar' as const,
        marker: { color: CHART_COLORS.success },
      },
      {
        x: formattedTimestamps,
        y: analytics?.trends?.errors || [],
        name: 'Errors',
        type: 'bar' as const,
        marker: { color: '#f43f5e' },
      },
    ],
    [
      formattedTimestamps,
      analytics?.trends?.pageviews,
      analytics?.trends?.interactions,
      analytics?.trends?.errors,
    ],
  );

  // Fetch live ticker
  const { data: liveEvents, isLoading: isLiveEventsLoading } = useQuery({
    queryKey: ['rum-live-events', serviceId, startTime, endTime, filterPayload],
    queryFn: async () => {
      if (!serviceId) return [];
      const params = new URLSearchParams();
      if (startTime) params.append('start_time', startTime);
      if (endTime) params.append('end_time', endTime);
      if (filterPayload) {
        params.append('filters', JSON.stringify(filterPayload));
      }
      const qs = params.toString();
      const url = qs ? `/api/services/${serviceId}/rum/live-events?${qs}` : `/api/services/${serviceId}/rum/live-events`;

      const res = await adminJsonResult<'/api/services/{service_id}/rum/live-events', RumLiveEvent[]>(
        '/api/services/{service_id}/rum/live-events',
        url,
      );
      if (!res.ok) {
        throw new Error(res.errorText || `RUM live events failed (HTTP ${res.status})`);
      }
      return arrayOrEmpty(res.data);
    },
    enabled: !!serviceId && !!status?.enabled,
    refetchInterval: false,
  });

  if (isStatusLoading || isLoading) {
    return <DashboardSkeleton />;
  }

  if (!status?.enabled) {
    return <RumStatusPanel />;
  }

  if (!analytics) {
    return <DashboardSkeleton />;
  }

  if (analytics.no_data) {
    return (
      <div className="space-y-6">
        <div className="flex justify-between items-center bg-emerald-500/10 p-3 rounded-lg border border-emerald-500/20 text-emerald-900 dark:text-emerald-300 shadow-sm animate-in fade-in-50 duration-300">
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 animate-pulse rounded-full bg-emerald-500" aria-hidden="true" />
            <span className="text-xs font-bold uppercase tracking-wider">Live RUM Stream Active</span>
          </div>
          <span className="text-xs opacity-85 font-medium">Waiting for real-time beacons…</span>
        </div>

        <AnalyticsCard title="Real User Monitoring Status">
          <div className="space-y-6 text-center py-12">
            <div>
              <p className="text-lg font-semibold text-muted-foreground mb-4">{analytics.message}</p>
              <div className="flex justify-center mb-6">
                <div className="w-48 h-2 bg-secondary rounded-full overflow-hidden">
                  <div
                    style={{ width: `${(analytics.beacon_count / 10) * 100}%` }}
                    className="h-full bg-emerald-500 transition-all"
                  />
                </div>
              </div>
              <p className="text-sm text-muted-foreground">
                Make sure the RUM script is installed on your website and users are visiting your site to generate beacons.
              </p>
            </div>
          </div>
        </AnalyticsCard>
      </div>
    );
  }

  if (analytics.beacon_count === 0) {
    return (
      <div className="space-y-6">
        <AnalyticsCard title="Real User Monitoring Dashboard">
          <div className="space-y-6 text-center py-12">
            <div>
              <div className="mx-auto w-16 h-16 bg-muted/20 rounded-full flex items-center justify-center mb-4 border border-border">
                <Activity className="h-8 w-8 text-muted-foreground/50" />
              </div>
              <p className="text-lg font-semibold text-foreground">No data for this time period</p>
              <p className="text-sm text-muted-foreground mt-2 max-w-sm mx-auto">
                No real-time beacons were received during the selected time window. Try expanding your time range or check back later.
              </p>
            </div>
          </div>
        </AnalyticsCard>
      </div>
    );
  }

  const lcpColors = getLcpColors(analytics.vitals.lcp.p75);
  const clsColors = getClsColors(analytics.vitals.cls.p75);
  const inpColors = getInpColors(analytics.vitals.inp.p75);

  // Distribution helper
  const renderDistributionBar = (distribution: RumCwvDistribution | null) => {
    const dist = distribution ?? { good: 0, needs_improvement: 0, poor: 0 };
    return (
    <div className="w-full mt-2">
      <div className="flex h-2.5 w-full overflow-hidden rounded-full bg-secondary">
        <div style={{ width: `${dist.good}%` }} className="bg-emerald-500" title={`Good: ${dist.good}%`} />
        <div style={{ width: `${dist.needs_improvement}%` }} className="bg-amber-400" title={`Needs Improvement: ${dist.needs_improvement}%`} />
        <div style={{ width: `${dist.poor}%` }} className="bg-rose-500" title={`Poor: ${dist.poor}%`} />
      </div>
      <div className="flex justify-between text-[10px] text-muted-foreground mt-1">
        <span>{dist.good}% Good</span>
        <span>{dist.needs_improvement}% Needs Imp.</span>
        <span>{dist.poor}% Poor</span>
      </div>
    </div>
    );
  };

  return (
    <div className="space-y-6">


      {/* Summary KPI Cards Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {[
          { label: 'Total Beacons', value: analytics.beacon_count ?? 0, textClass: 'text-blue-500', bgClass: 'bg-blue-500/10', Icon: Activity },
          { label: 'Pageviews', value: analytics.pageview_count ?? 0, textClass: 'text-emerald-500', bgClass: 'bg-emerald-500/10', Icon: Eye },
          { label: 'Interactions', value: analytics.interaction_count ?? 0, textClass: 'text-amber-500', bgClass: 'bg-amber-500/10', Icon: Cpu },
          { label: 'JavaScript Errors', value: analytics.error_count ?? 0, textClass: 'text-rose-500', bgClass: 'bg-rose-500/10', Icon: ShieldAlert },
        ].map(({ label, value, textClass, bgClass, Icon }) => (
          <div key={label} className="bg-background/40 backdrop-blur-md border border-muted/50 rounded-xl p-4 flex items-center justify-between shadow-sm">
            <div className="space-y-1">
              <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">{label}</p>
              <p className={`text-2xl font-extrabold ${textClass}`}>{formatCount(value)}</p>
            </div>
            <div className={`p-2.5 ${bgClass} rounded-lg`}>
              <Icon className={`h-5 w-5 ${textClass}`} />
            </div>
          </div>
        ))}
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          {
            title: 'Largest Contentful Paint (LCP)',
            Icon: Activity,
            colors: lcpColors,
            formatted: analytics.vitals.lcp.p75 != null ? `${analytics.vitals.lcp.p75.toFixed(2)}s` : '—',
            distribution: analytics.vitals.lcp.distribution,
          },
          {
            title: 'Cumulative Layout Shift (CLS)',
            Icon: Cpu,
            colors: clsColors,
            formatted: analytics.vitals.cls.p75 != null ? analytics.vitals.cls.p75.toFixed(2) : '—',
            distribution: analytics.vitals.cls.distribution,
          },
          {
            title: 'Interaction to Next Paint (INP)',
            Icon: Monitor,
            colors: inpColors,
            formatted: analytics.vitals.inp.p75 != null ? `${analytics.vitals.inp.p75.toFixed(2)}ms` : '—',
            distribution: analytics.vitals.inp.distribution,
          },
        ].map(({ title, Icon, colors, formatted, distribution }) => (
          <AnalyticsCard
            key={title}
            title={<span className={colors.text}>{title}</span>}
            icon={<Icon className={colors.text} />}
            headerAction={
              <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider ${colors.bg} ${colors.text}`}>
                {colors.label}
              </span>
            }
          >
            <div className="space-y-2">
              <p className={`text-3xl font-extrabold ${colors.text}`}>{formatted}</p>
              <p className={`text-xs ${colors.text} font-semibold uppercase tracking-wider`}>75th Percentile</p>
              {renderDistributionBar(distribution)}
            </div>
          </AnalyticsCard>
        ))}
      </div>

      {/* Trends Row */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard title="Web Vitals Trend" icon={<TrendingUp />}>
          <TimeSeriesChart
            data={webVitalsTrendData}
            layout={trendChartLayout}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height={250}
          />
        </AnalyticsCard>

        <AnalyticsCard title="JS Error Rate Trend" icon={<TrendingUp className="text-rose-500" />}>
          <TimeSeriesChart
            data={errorRateTrendData}
            layout={trendChartLayout}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height={250}
          />
        </AnalyticsCard>
      </div>

      {/* Worst Pages & Error tables */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard title="Worst Performing Pages" icon={<Monitor />} >
          <DataTableReadonly
            columns={WORST_PAGE_COLUMNS}
            data={analytics.worst_pages}
            hideToolbar
            showPagination={false}
            tableCaption="Worst performing pages"
            emptyMessage="No page data in this window"
          />
        </AnalyticsCard>

        <AnalyticsCard title="JavaScript Runtime Exceptions" icon={<ShieldAlert className="text-rose-500" />}>
          <div className="space-y-4">
            {analytics.errors.length === 0 ? (
              <p className="text-sm text-muted-foreground py-4 text-center">No JS errors captured in this window</p>
            ) : (
              analytics.errors.map((e: RumJsError) => (
                <div key={`${e.message}-${e.file}-${e.line}-${e.col}`} className="p-3 bg-rose-500/5 rounded-lg border border-rose-500/10 space-y-1.5">
                  <div className="flex justify-between items-start gap-4">
                    <p className="text-xs font-semibold text-rose-600 font-mono break-all">{e.message}</p>
                    <span className="bg-rose-500/10 text-rose-600 px-2 py-0.5 rounded text-[10px] font-bold">
                      {e.count}x
                    </span>
                  </div>
                  <p className="text-[10px] text-muted-foreground font-mono">
                    File: {e.file}:{e.line}:{e.col}
                  </p>
                </div>
              ))
            )}
          </div>
        </AnalyticsCard>
      </div>

      {/* Environment Donuts Row */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[
          { title: 'Browser Distribution', data: browserPieData, a11yTitle: 'Browser Distribution Donut Chart' },
          { title: 'OS Distribution', data: osPieData, a11yTitle: 'OS Distribution Donut Chart' },
          { title: 'Device Distribution', data: devicePieData, a11yTitle: 'Device Distribution Donut Chart' },
        ].map(({ title, data, a11yTitle }) => (
          <AnalyticsCard key={title} title={title} icon={<PieChart />}>
            <PlotlyChart
              data={data}
              layout={donutChartLayout}
              a11yTitle={a11yTitle}
            />
          </AnalyticsCard>
        ))}
      </div>

      {/* Beacons Over Time Row */}
      <div className="mb-6">
        <AnalyticsCard title="Beacons Over Time" icon={<Activity className="text-primary" />}>
          <TimeSeriesChart
            data={beaconsOverTimeData}
            layout={stackedBarLayout}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height={250}
          />
        </AnalyticsCard>
      </div>

      {/* Live Monitor Feed Activity Ticker */}
      <AnalyticsCard title="Live Activity Monitor" icon={<Terminal className="h-4 w-4" />}>
        <div className="w-full">
          <DataTable
            columns={columns}
            data={liveEvents || []}
            searchKey="path"
            isLoading={isLiveEventsLoading}
            showPagination={false}
            emptyMessage="Waiting for real-time RUM user events..."
            onRowClick={(row) => setSelectedEvent(row)}
          />
        </div>
      </AnalyticsCard>

      {/* Beacon Details Modal */}
      <Dialog open={selectedEvent !== null} onOpenChange={(open) => { if (!open) setSelectedEvent(null); }}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle className="text-lg font-bold flex items-center gap-2">
              <Activity className={selectedEvent?.type === 'error' ? 'text-rose-500 h-5 w-5' : 'text-emerald-500 h-5 w-5'} />
              RUM Beacon Details
            </DialogTitle>
            <DialogDescription>
              Extracted client characteristics and complete raw payload captured at the Fastly edge.
            </DialogDescription>
          </DialogHeader>

          {selectedEvent && (() => {
            let faroPayload: unknown = null;
            let edgeMetadata: Record<string, unknown> | null = null;

            const rawLog = selectedEvent.raw_log;
            if (rawLog) {
              if (rawLog.faro_payload) {
                faroPayload = rawLog.faro_payload;
                edgeMetadata = { ...rawLog };
                delete edgeMetadata.faro_payload;
              } else if (rawLog.meta && (rawLog.measurements || rawLog.events)) {
                faroPayload = rawLog;
                edgeMetadata = {
                  time: selectedEvent.time,
                  type: selectedEvent.type,
                  path: selectedEvent.path,
                  desc: selectedEvent.desc,
                  browser: selectedEvent.browser,
                  os: selectedEvent.os
                };
              } else {
                edgeMetadata = rawLog;
              }
            }

            return (
              <div className="space-y-4 my-2 text-xs">
                {/* Event Metadata Grid */}
                <div className="grid grid-cols-2 gap-4 bg-muted/20 p-4 rounded-lg border border-border">
                  <div>
                    <span className="text-muted-foreground block mb-1">Timestamp</span>
                    <span className="font-mono">{formatDate(selectedEvent.time, timezone, 'yyyy-MM-dd HH:mm:ss (zzz)')}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground block mb-1">Event Type</span>
                    <span className={`capitalize font-semibold ${selectedEvent.type === 'error' ? 'text-rose-500' : 'text-emerald-500'}`}>
                      {selectedEvent.type}
                    </span>
                  </div>
                  <div>
                    <span className="text-muted-foreground block mb-1">Url Path</span>
                    <span className="font-mono font-semibold">{selectedEvent.path}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground block mb-1">Client Platform</span>
                    <span>{envName(selectedEvent.browser)} on {envName(selectedEvent.os)}</span>
                  </div>
                  <div className="col-span-2">
                    <span className="text-muted-foreground block mb-1">Description</span>
                    <span className={`font-mono block p-2 rounded ${selectedEvent.type === 'error' ? 'bg-rose-500/10 text-rose-500 border border-rose-500/20' : 'bg-muted/40 text-foreground'}`}>
                      {selectedEvent.desc}
                    </span>
                  </div>
                </div>

                {faroPayload != null && (
                  <div className="space-y-2">
                    <span className="text-muted-foreground font-semibold block">Client Faro JSON Payload (Full)</span>
                    <div className="bg-emerald-950/5 dark:bg-emerald-950/15 border border-emerald-500/15 rounded-lg p-4 overflow-auto max-h-[250px] font-mono text-[11px] leading-relaxed text-foreground">
                      <pre>{JSON.stringify(faroPayload, null, 2)}</pre>
                    </div>
                  </div>
                )}

                <div className="space-y-2">
                  <span className="text-muted-foreground font-semibold block">
                    {faroPayload ? "Edge Log Representation (Extracted Row Metadata)" : "Raw Beacon Log JSON"}
                  </span>
                  <div className="bg-muted/30 border border-border rounded-lg p-4 overflow-auto max-h-[250px] font-mono text-[11px] leading-relaxed text-foreground">
                    <pre>{JSON.stringify(edgeMetadata || rawLog || {}, null, 2)}</pre>
                  </div>
                </div>
              </div>
            );
          })()}
        </DialogContent>
      </Dialog>
    </div>
  );
}

interface RumPageClientProps {
  initialServiceId?: string;
}

interface ServiceSyncProps {
  serviceId: string | null;
  onSync: (id: string | null) => void;
}

function ServiceSync({ serviceId, onSync }: ServiceSyncProps) {
  useEffect(() => {
    onSync(serviceId);
  }, [serviceId, onSync]);
  return null;
}

export function RumPageClient({ initialServiceId }: RumPageClientProps) {
  const [showInstructions, setShowInstructions] = useState(false);
  const [activeServiceId, setActiveServiceId] = useState<string | null>(initialServiceId || null);
  const isAnalyst = useIsAnalyst();
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor();

  const scriptTag = '<script src="/js/rum.js"></script>';

  return (
    <>
      <ReportLayout
        title="Real User Monitoring"
        description="Monitor real user performance metrics including Core Web Vitals, JavaScript errors, and session analytics."
        icon={Radio}
        serviceId={initialServiceId}
        headerActions={
          <div className="flex items-center gap-3">
            <RumFaroVersionCard serviceId={activeServiceId} />
            {!isAnalyst && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => setShowInstructions(true)}
                className="flex items-center gap-2"
              >
                <BookOpen className="h-4 w-4" />
                Installation Instructions
              </Button>
            )}
          </div>
        }
      >
        {({ startTime, endTime, activeServiceId: sid, filterPayload }) => (
          <>
            <ServiceSync serviceId={sid} onSync={setActiveServiceId} />
            <RumClient
              serviceId={sid}
              startTime={startTime}
              endTime={endTime}
              filterPayload={filterPayload}
              relativeRange={relativeRange}
              isAutoRange={isAutoRange}
              anchor={anchor}
            />
          </>
        )}
      </ReportLayout>

      {!isAnalyst && (
        <Dialog open={showInstructions} onOpenChange={setShowInstructions}>
          <DialogContent className="max-w-2xl">
            <DialogHeader>
              <DialogTitle>Installation Instructions</DialogTitle>
              <DialogDescription>
                Add Real User Monitoring to your website to start collecting performance data.
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <h4 className="font-semibold text-sm">Step 1: Add the RUM Script</h4>
                <p className="text-sm text-muted-foreground">
                  Add this single line to the `&lt;head&gt;` section of your HTML or any template that renders across your entire site. The version hash automatically updates when we release new RUM features:
                </p>
                <div className="bg-muted rounded-md overflow-hidden border">
                  <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/50">
                    <span className="text-[10px] font-mono text-muted-foreground">installation code</span>
                    <CopyButton
                      text={scriptTag}
                      ariaLabel="Copy script tag"
                      copiedAriaLabel="Copied script tag"
                      className="h-6 w-6 hover:bg-muted-foreground/10"
                    />
                  </div>
                  <div className="p-3 font-mono text-xs overflow-x-auto">
                    <code>{scriptTag}</code>
                  </div>
                </div>
              </div>

              <div className="space-y-2">
                <h4 className="font-semibold text-sm">Step 2: Works Everywhere</h4>
                <p className="text-sm text-muted-foreground">
                  This script works on any website regardless of framework:
                </p>
                <ul className="text-sm text-muted-foreground list-disc list-inside space-y-1">
                  <li>Plain HTML, PHP, Django, Rails, etc.</li>
                  <li>React, Vue, Svelte, Angular, Next.js</li>
                  <li>Static sites, dynamic apps, and everything in between</li>
                </ul>
              </div>

              <div className="space-y-2">
                <h4 className="font-semibold text-sm">Step 3: Start Collecting Data</h4>
                <p className="text-sm text-muted-foreground">
                  Once deployed, real user beacons will automatically be sent to your analytics dashboard when users visit your site. After collecting 1+ beacons, you&apos;ll see real performance data appear on this page.
                </p>
              </div>

              <div className="bg-blue-50 dark:bg-blue-950 p-3 rounded-md space-y-2">
                <p className="text-sm font-semibold text-blue-900 dark:text-blue-100">💡 Tips:</p>
                <ul className="text-xs text-blue-800 dark:text-blue-200 space-y-1">
                  <li>• Beacons capture Web Vitals, JavaScript errors, and session info automatically</li>
                  <li>• The script is lightweight and asynchronous — no performance impact</li>
                  <li>• Data is sent securely to your Fastly-backed analytics backend</li>
                  <li>• The version hash automatically updates when we release RUM improvements — just copy the script tag again from these instructions</li>
                </ul>
              </div>
            </div>
          </DialogContent>
        </Dialog>
      )}
    </>
  );
}

export default RumPageClient;
