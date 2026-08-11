// frontend/app/rum/_sections/RumClient.tsx
'use client';

import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AnalyticsCard } from '@/components/AnalyticsCard';
import { RumStatusPanel } from '@/components/Rum/RumStatusPanel';

import { GenericPageSkeleton } from '@/components/skeletons/PageSkeleton';
import { PlotlyChart } from '@/components/PlotlyChart';
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart';
import { useTimezone } from '@/hooks/useTimezone';
import { formatDate } from '@/lib/date';
import { Terminal, Activity, Monitor, ShieldAlert, Cpu, PieChart, TrendingUp, Eye } from 'lucide-react';
import { adminFetch } from '@/lib/api';
import { useIsAnalyst } from '@/hooks/useIsAnalyst';
import type { FiltersPayload } from '@/types/filters';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { DataTable } from '@/components/DataTable/DataTable';
import { ColumnDef } from '@tanstack/react-table';

interface RumClientProps {
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

interface RumWorstPage {
  path: string;
  views: number;
  avg_load_time: number | null;
  lcp_p75: number | null;
  cls_p75: number | null;
}

interface RumJsError {
  message: string;
  file: string;
  line: number;
  col: number;
  count: number;
}

interface RumLiveEvent {
  time: string;
  type: string;
  path: string;
  desc: string;
  browser: string;
  os: string;
  raw_log?: any;
}

function getLcpColors(p75: number | null | undefined): CwvColors {
  if (p75 == null) {
    return { text: 'text-muted-foreground', bg: 'bg-muted/10', label: 'No Data' };
  }
  if (p75 <= 2.5) {
    return { text: 'text-emerald-500', bg: 'bg-emerald-500/10', label: 'Good' };
  } else if (p75 > 4.0) {
    return { text: 'text-rose-500', bg: 'bg-rose-500/10', label: 'Poor' };
  } else {
    return { text: 'text-amber-500', bg: 'bg-amber-500/10', label: 'Needs Improvement' };
  }
}

function getClsColors(p75: number | null | undefined): CwvColors {
  if (p75 == null) {
    return { text: 'text-muted-foreground', bg: 'bg-muted/10', label: 'No Data' };
  }
  if (p75 <= 0.1) {
    return { text: 'text-emerald-500', bg: 'bg-emerald-500/10', label: 'Good' };
  } else if (p75 > 0.25) {
    return { text: 'text-rose-500', bg: 'bg-rose-500/10', label: 'Poor' };
  } else {
    return { text: 'text-amber-500', bg: 'bg-amber-500/10', label: 'Needs Improvement' };
  }
}

function getInpColors(p75: number | null | undefined): CwvColors {
  if (p75 == null) {
    return { text: 'text-muted-foreground', bg: 'bg-muted/10', label: 'No Data' };
  }
  if (p75 <= 200) {
    return { text: 'text-emerald-500', bg: 'bg-emerald-500/10', label: 'Good' };
  } else if (p75 > 500) {
    return { text: 'text-rose-500', bg: 'bg-rose-500/10', label: 'Poor' };
  } else {
    return { text: 'text-amber-500', bg: 'bg-amber-500/10', label: 'Needs Improvement' };
  }
}

export function RumClient({ serviceId, startTime, endTime, filterPayload }: RumClientProps) {
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
        const b = typeof row.browser === 'object' && row.browser ? ((row.browser as any).name || 'Unknown') : (row.browser || 'Unknown');
        const o = typeof row.os === 'object' && row.os ? ((row.os as any).name || 'Unknown') : (row.os || 'Unknown');
        return `${b} on ${o}`;
      },
      meta: { label: 'Environment' },
      header: 'Environment',
      cell: ({ row }) => {
        const b = typeof row.original.browser === 'object' && row.original.browser ? ((row.original.browser as any).name || 'Unknown') : (row.original.browser || 'Unknown');
        const o = typeof row.original.os === 'object' && row.original.os ? ((row.original.os as any).name || 'Unknown') : (row.original.os || 'Unknown');
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
        return raw.ttfb != null ? parseFloat(raw.ttfb) : null;
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
  const { data: status, isLoading: isStatusLoading } = useQuery({
    queryKey: ['rum-status', serviceId],
    queryFn: async () => {
      if (!serviceId) return null;
      const res = await adminFetch(`/api/services/${serviceId}/rum/status`);
      return res.ok ? res.json() : null;
    },
    enabled: !!serviceId,
  });

  // Fetch analytics
  const { data: analytics, isLoading } = useQuery({
    queryKey: ['rum-analytics', serviceId, startTime, endTime, filterPayload],
    queryFn: async () => {
      if (!serviceId) return null;
      const params = new URLSearchParams();
      if (startTime) params.append('start_time', startTime);
      if (endTime) params.append('end_time', endTime);
      if (filterPayload && Object.keys(filterPayload).length > 0) {
        params.append('filters', JSON.stringify(filterPayload));
      }
      const qs = params.toString();
      const url = `/api/services/${serviceId}/rum/analytics${qs ? `?${qs}` : ''}`;
      const res = await adminFetch(url);
      return res.ok ? res.json() : null;
    },
    enabled: !!serviceId && !!status?.enabled,
    refetchInterval: false,
  });

  const formattedTimestamps = React.useMemo(() => {
    return (analytics?.trends?.timestamps || []).map((t: string) =>
      formatDate(t, timezone, 'yyyy-MM-dd HH:mm:ss')
    );
  }, [analytics?.trends?.timestamps, timezone]);

  // Fetch live ticker
  const { data: liveEvents } = useQuery({
    queryKey: ['rum-live-events', serviceId],
    queryFn: async () => {
      if (!serviceId) return [];
      const res = await adminFetch(`/api/services/${serviceId}/rum/live-events`);
      return res.ok ? res.json() : [];
    },
    enabled: !!serviceId && !!status?.enabled,
    refetchInterval: false,
  });

  if (isStatusLoading || isLoading) {
    return <GenericPageSkeleton />;
  }

  if (!status?.enabled) {
    return <RumStatusPanel />;
  }

  if (!analytics) {
    return <GenericPageSkeleton />;
  }

  if (analytics.no_data) {
    return (
      <div className="space-y-6">
        <div className="flex justify-between items-center bg-muted/30 p-3 rounded-lg border">
          <div className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-muted-foreground" />
            <span className="text-sm font-semibold">Waiting for real-time data</span>
          </div>
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

  const lcpColors = getLcpColors(analytics.vitals.lcp.p75);
  const clsColors = getClsColors(analytics.vitals.cls.p75);
  const inpColors = getInpColors(analytics.vitals.inp.p75);

  // Distribution helper
  const renderDistributionBar = (dist: { good: number; needs_improvement: number; poor: number }) => (
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

  return (
    <div className="space-y-6">


      {/* Summary KPI Cards Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-background/40 backdrop-blur-md border border-muted/50 rounded-xl p-4 flex items-center justify-between shadow-sm">
          <div className="space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Total Beacons</p>
            <p className="text-2xl font-extrabold text-blue-500">{analytics.beacon_count ?? 0}</p>
          </div>
          <div className="p-2.5 bg-blue-500/10 rounded-lg">
            <Activity className="h-5 w-5 text-blue-500" />
          </div>
        </div>

        <div className="bg-background/40 backdrop-blur-md border border-muted/50 rounded-xl p-4 flex items-center justify-between shadow-sm">
          <div className="space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Pageviews</p>
            <p className="text-2xl font-extrabold text-emerald-500">{analytics.pageview_count ?? 0}</p>
          </div>
          <div className="p-2.5 bg-emerald-500/10 rounded-lg">
            <Eye className="h-5 w-5 text-emerald-500" />
          </div>
        </div>

        <div className="bg-background/40 backdrop-blur-md border border-muted/50 rounded-xl p-4 flex items-center justify-between shadow-sm">
          <div className="space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Interactions</p>
            <p className="text-2xl font-extrabold text-amber-500">{analytics.interaction_count ?? 0}</p>
          </div>
          <div className="p-2.5 bg-amber-500/10 rounded-lg">
            <Cpu className="h-5 w-5 text-amber-500" />
          </div>
        </div>

        <div className="bg-background/40 backdrop-blur-md border border-muted/50 rounded-xl p-4 flex items-center justify-between shadow-sm">
          <div className="space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">JavaScript Errors</p>
            <p className="text-2xl font-extrabold text-rose-500">{analytics.error_count ?? 0}</p>
          </div>
          <div className="p-2.5 bg-rose-500/10 rounded-lg">
            <ShieldAlert className="h-5 w-5 text-rose-500" />
          </div>
        </div>
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <AnalyticsCard
          title={<span className={lcpColors.text}>Largest Contentful Paint (LCP)</span>}
          icon={<Activity className={lcpColors.text} />}
          headerAction={
            <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider ${lcpColors.bg} ${lcpColors.text}`}>
              {lcpColors.label}
            </span>
          }
        >
          <div className="space-y-2">
            <p className={`text-3xl font-extrabold ${lcpColors.text}`}>
              {analytics.vitals.lcp.p75 != null ? `${analytics.vitals.lcp.p75.toFixed(2)}s` : '—'}
            </p>
            <p className={`text-xs ${lcpColors.text} font-semibold uppercase tracking-wider`}>75th Percentile</p>
            {renderDistributionBar(analytics.vitals.lcp.distribution)}
          </div>
        </AnalyticsCard>

        <AnalyticsCard
          title={<span className={clsColors.text}>Cumulative Layout Shift (CLS)</span>}
          icon={<Cpu className={clsColors.text} />}
          headerAction={
            <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider ${clsColors.bg} ${clsColors.text}`}>
              {clsColors.label}
            </span>
          }
        >
          <div className="space-y-2">
            <p className={`text-3xl font-extrabold ${clsColors.text}`}>
              {analytics.vitals.cls.p75 != null ? analytics.vitals.cls.p75.toFixed(3) : '—'}
            </p>
            <p className={`text-xs ${clsColors.text} font-semibold uppercase tracking-wider`}>75th Percentile</p>
            {renderDistributionBar(analytics.vitals.cls.distribution)}
          </div>
        </AnalyticsCard>

        <AnalyticsCard
          title={<span className={inpColors.text}>Interaction to Next Paint (INP)</span>}
          icon={<Monitor className={inpColors.text} />}
          headerAction={
            <span className={`px-2 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider ${inpColors.bg} ${inpColors.text}`}>
              {inpColors.label}
            </span>
          }
        >
          <div className="space-y-2">
            <p className={`text-3xl font-extrabold ${inpColors.text}`}>
              {analytics.vitals.inp.p75 != null ? `${analytics.vitals.inp.p75}ms` : '—'}
            </p>
            <p className={`text-xs ${inpColors.text} font-semibold uppercase tracking-wider`}>75th Percentile</p>
            {renderDistributionBar(analytics.vitals.inp.distribution)}
          </div>
        </AnalyticsCard>
      </div>

      {/* Beacons Over Time Row */}
      <div className="mb-6">
        <AnalyticsCard title="Beacons Over Time" icon={<Activity className="text-primary" />}>
          <TimeSeriesChart
            data={[
              {
                x: formattedTimestamps,
                y: analytics.trends.pageviews || [],
                name: 'Pageviews',
                type: 'bar',
                marker: { color: '#3b82f6' },
              },
              {
                x: formattedTimestamps,
                y: analytics.trends.interactions || [],
                name: 'Interactions',
                type: 'bar',
                marker: { color: '#10b981' },
              },
              {
                x: formattedTimestamps,
                y: analytics.trends.errors || [],
                name: 'Errors',
                type: 'bar',
                marker: { color: '#f43f5e' },
              },
            ]}
            layout={{
              barmode: 'stack',
              margin: { t: 30, b: 40, l: 30, r: 10 },
              legend: {
                orientation: 'h',
                y: 1.08,
                x: 0.5,
                xanchor: 'center',
                yanchor: 'bottom',
              },
            }}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height={250}
          />
        </AnalyticsCard>
      </div>

      {/* Trends Row */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard title="Web Vitals Trend" icon={<TrendingUp />}>
          <TimeSeriesChart
            data={[
              {
                x: formattedTimestamps,
                y: analytics.trends.lcp,
                name: 'LCP (s)',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#10b981' },
              },
              {
                x: formattedTimestamps,
                y: analytics.trends.cls,
                name: 'CLS',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#3b82f6' },
              },
            ]}
            layout={{
              margin: { t: 30, b: 40, l: 30, r: 10 },
              legend: {
                orientation: 'h',
                y: 1.08,
                x: 0.5,
                xanchor: 'center',
                yanchor: 'bottom',
              },
            }}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height={250}
          />
        </AnalyticsCard>

        <AnalyticsCard title="JS Error Rate Trend" icon={<TrendingUp className="text-rose-500" />}>
          <TimeSeriesChart
            data={[
              {
                x: formattedTimestamps,
                y: analytics.trends.error_rate,
                name: 'Error Rate (%)',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#f43f5e' },
                fill: 'tozeroy',
              },
            ]}
            layout={{
              margin: { t: 30, b: 40, l: 30, r: 10 },
              legend: {
                orientation: 'h',
                y: 1.08,
                x: 0.5,
                xanchor: 'center',
                yanchor: 'bottom',
              },
            }}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height={250}
          />
        </AnalyticsCard>
      </div>

      {/* Worst Pages & Error tables */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <AnalyticsCard title="Worst Performing Pages" icon={<Monitor />}>
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left border-collapse">
              <thead>
                <tr className="border-b text-muted-foreground font-medium">
                  <th className="py-2">Path</th>
                  <th className="py-2 text-right">Pageviews</th>
                  <th className="py-2 text-right">Avg Load (s)</th>
                  <th className="py-2 text-right">LCP p75</th>
                  <th className="py-2 text-right">CLS p75</th>
                </tr>
              </thead>
              <tbody>
                {analytics.worst_pages.map((p: RumWorstPage) => (
                  <tr key={p.path} className="border-b hover:bg-muted/10">
                    <td className="py-2 font-semibold font-mono text-xs">{p.path}</td>
                    <td className="py-2 text-right">{p.views}</td>
                    <td className="py-2 text-right">{p.avg_load_time != null ? `${p.avg_load_time.toFixed(1)}s` : '—'}</td>
                    <td className="py-2 text-right text-amber-500">{p.lcp_p75 != null ? `${p.lcp_p75.toFixed(2)}s` : '—'}</td>
                    <td className="py-2 text-right">{p.cls_p75 != null ? p.cls_p75.toFixed(2) : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
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
        <AnalyticsCard title="Browser Distribution" icon={<PieChart />}>
          <PlotlyChart
            data={[
              {
                labels: Object.keys(analytics.environments.browsers),
                values: Object.values(analytics.environments.browsers),
                type: 'pie',
                hole: 0.4,
                textinfo: 'label+percent',
              },
            ]}
            layout={{ height: 250, margin: { t: 10, b: 10, l: 10, r: 10 }, showlegend: false }}
            a11yTitle="Browser Distribution Donut Chart"
          />
        </AnalyticsCard>

        <AnalyticsCard title="OS Distribution" icon={<PieChart />}>
          <PlotlyChart
            data={[
              {
                labels: Object.keys(analytics.environments.os),
                values: Object.values(analytics.environments.os),
                type: 'pie',
                hole: 0.4,
                textinfo: 'label+percent',
              },
            ]}
            layout={{ height: 250, margin: { t: 10, b: 10, l: 10, r: 10 }, showlegend: false }}
            a11yTitle="OS Distribution Donut Chart"
          />
        </AnalyticsCard>

        <AnalyticsCard title="Device Distribution" icon={<PieChart />}>
          <PlotlyChart
            data={[
              {
                labels: Object.keys(analytics.environments.devices),
                values: Object.values(analytics.environments.devices),
                type: 'pie',
                hole: 0.4,
                textinfo: 'label+percent',
              },
            ]}
            layout={{ height: 250, margin: { t: 10, b: 10, l: 10, r: 10 }, showlegend: false }}
            a11yTitle="Device Distribution Donut Chart"
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
            isLoading={!liveEvents}
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
            let faroPayload: any = null;
            let edgeMetadata: any = null;

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
                    <span>{selectedEvent.browser} on {selectedEvent.os}</span>
                  </div>
                  <div className="col-span-2">
                    <span className="text-muted-foreground block mb-1">Description</span>
                    <span className={`font-mono block p-2 rounded ${selectedEvent.type === 'error' ? 'bg-rose-500/10 text-rose-500 border border-rose-500/20' : 'bg-muted/40 text-foreground'}`}>
                      {selectedEvent.desc}
                    </span>
                  </div>
                </div>

                {faroPayload && (
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
