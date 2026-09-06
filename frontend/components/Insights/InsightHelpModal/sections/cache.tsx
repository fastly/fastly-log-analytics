import React from 'react'
import {
  Server,
  WifiOff,
  TrendingDown,
  Clock,
  AlertTriangle,
  Database,
} from 'lucide-react'
import type { InsightContent } from '../types'

export function getCacheContent(id: string): InsightContent | null {
  switch (id) {
    case 'cache_collapse':
      return {
        title: 'Cache Efficiency Collapse',
        icon: <WifiOff className="h-5 w-5 text-primary" />,
        fields: ['cache', 'url'],
        description: (
          <div className="space-y-4">
            <p>Detects URLs where the Cache Hit Ratio (CHR) has dropped dramatically, potentially causing an &quot;origin fire.&quot;</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingDown className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Efficiency Drop:</strong> Flags URLs that previously had &gt;80% CHR but have suddenly dropped to &lt;20%.</span>
              </li>
              <li className="flex gap-3">
                <Server className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Origin Impact:</strong> This usually indicates a change in query parameters (cache busting) or a deployment that accidentally disabled caching for a hot route.</span>
              </li>
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-muted-foreground" />
                <span><strong>CHR = HIT / (HIT + MISS).</strong> PASS (uncacheable) requests are excluded — a surge of those is reported separately under <em>Cacheability Regression</em>, not here.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cacheability_regression':
      return {
        title: 'Cacheability Regression',
        icon: <Database className="h-5 w-5 text-primary" />,
        fields: ['cache', 'url'],
        description: (
          <div className="space-y-4">
            <p>Detects URLs that flipped from cacheable to mostly <strong>PASS</strong> — Fastly bypassed the cache entirely, so requests now go straight to origin.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingDown className="h-5 w-5 shrink-0 text-amber-500" />
                <span><strong>PASS surge:</strong> Flags URLs whose PASS share (PASS / all requests) jumped from a low baseline to a majority of recent traffic.</span>
              </li>
              <li className="flex gap-3">
                <Server className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Common causes:</strong> origin started sending <code>Set-Cookie</code> or <code>Cache-Control: private</code>, a newly-varying query param, or a VCL change forcing <code>pass</code>.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Why it&apos;s separate:</strong> PASS was never cacheable, so it isn&apos;t a hit-ratio miss. The fix is to restore cacheability, not to warm the cache.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cache_pressure':
      return {
        title: 'Cache Pressure & Evictions',
        icon: <Database className="h-5 w-5 text-primary" />,
        fields: ['digest', 'ttl', 'age', 'pop', 'cache', 'resp_bytes'],
        description: (
          <div className="space-y-4">
            <p>Detects when objects are being prematurely evicted from the edge cache before their TTL (Time To Live) expires.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Age vs TTL:</strong> We analyze cache misses and compare the object&apos;s expected TTL against the time since it was last fetched.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Capacity Warning:</strong> High rates of premature eviction mean your Fastly service is under &quot;Cache Pressure&quot; and objects are being pushed out of memory to make room for new ones. You may need to increase your Cache Reservation.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cache_hit_cliff':
      return {
        title: 'Cache HIT-Ratio Cliff',
        icon: <TrendingDown className="h-5 w-5 text-primary" />,
        fields: ['cache'],
        description: (
          <div className="space-y-4">
            <p>A single headline card: your whole service&apos;s edge cache HIT ratio fell off a cliff vs baseline. This is the aggregate counterpart to the per-URL Cache Efficiency Collapse.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Cacheable ratio:</strong> We measure HIT / (HIT + MISS) across all traffic — PASS is excluded because it was never eligible to cache — and compare the window to baseline.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>What it catches:</strong> a purge storm, a global TTL change, an origin <code>Cache-Control</code> regression, or a VCL deploy that started passing traffic — each pushes load onto origin and slows delivery site-wide.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'cache_ttl_mismatch':
      return {
        title: 'Cache TTL Inefficiency',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['ttl', 'hits', 'age', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Identifies URLs configured with very high Cache TTL (Time To Live) but receiving extremely low hit counts, which can lead to inefficient cache allocation.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Inefficient Cache Retention:</strong> Objects with long TTLs but very few requests occupy high-value edge memory slots without providing equivalent cache-efficiency returns.
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Wasted Cache Slots:</strong> These items can push out hot, high-demand resources, contributing to overall cache pressure and premature evictions elsewhere.
                </span>
              </li>
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Consider reducing the TTL for low-traffic or cold resources using Cache-Control directives from the origin, or optimize cache key clustering in VCL.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}
