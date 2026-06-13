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
            <p>Detects URLs where the Cache Hit Ratio (CHR) has dropped dramatically, potentially causing an "origin fire."</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingDown className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Efficiency Drop:</strong> Flags URLs that previously had &gt;80% CHR but have suddenly dropped to &lt;20%.</span>
              </li>
              <li className="flex gap-3">
                <Server className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Origin Impact:</strong> This usually indicates a change in query parameters (cache busting) or a deployment that accidentally disabled caching for a hot route.</span>
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
                <span><strong>Age vs TTL:</strong> We analyze cache misses and compare the object's expected TTL against the time since it was last fetched.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Capacity Warning:</strong> High rates of premature eviction mean your Fastly service is under "Cache Pressure" and objects are being pushed out of memory to make room for new ones. You may need to increase your Cache Reservation.</span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}
