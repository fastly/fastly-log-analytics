import React from 'react'
import {
  Globe,
  Activity,
  MapPin,
  AlertTriangle,
  BarChart,
} from 'lucide-react'
import type { InsightContent } from '../types'

export function getErrorsContent(id: string): InsightContent | null {
  switch (id) {
    case 'error_spikes':
      return {
        title: 'Global Error Spikes',
        icon: <AlertTriangle className="h-5 w-5 text-primary" />,
        fields: ['status'],
        description: (
          <div className="space-y-4">
            <p>Detects sudden, dramatic increases in 5xx server errors across your entire service.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <BarChart className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Error Rate Tracking:</strong> We compare the current 5xx error percentage to the historical average.</span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Spike Threshold:</strong> Triggers when the error rate triples the baseline and exceeds a strict minimum threshold, indicating a system-wide incident.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'city_error_spikes':
      return {
        title: 'City-Level Error Spikes',
        icon: <Globe className="h-5 w-5 text-primary" />,
        fields: ['status', 'city'],
        description: (
          <div className="space-y-4">
            <p>Detects localized outages by tracking 5xx error rates segmented by individual cities.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <MapPin className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Geographic Segmentation:</strong> Errors are calculated per city rather than globally, uncovering issues that only affect specific regions.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Routing Issues:</strong> Often indicates a regional routing problem or an origin server in a specific geography failing.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'origin_error_rate':
      return {
        title: 'Origin Error Rate',
        icon: <AlertTriangle className="h-5 w-5 text-primary" />,
        fields: ['ost', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Flags a significant, statistical spike in HTTP 5xx error responses returned directly by your upstream origin application servers.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Backend Application Failures:</strong> Monitors HTTP response statuses (such as 500 Internal Server Error, 502 Bad Gateway, 503 Service Unavailable, or 504 Gateway Timeout) originating from your backend servers.
                </span>
              </li>
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Outlier Detection:</strong> Triggers when the backend error rate rises dramatically above historical baseline levels, signifying an active outage or application crash.
                </span>
              </li>
              <li className="flex gap-3">
                <Globe className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Check your application server logs, verify database connection health, and review CPU/RAM utilization on your backend hosts.
                </span>
              </li>
            </ul>
          </div>
        )
      }

    case 'origin_ip_failure':
      return {
        title: 'Specific Origin IP Failing',
        icon: <Globe className="h-5 w-5 text-primary" />,
        fields: ['oip', 'ost', 'timestamp'],
        description: (
          <div className="space-y-4">
            <p>
              Pinpoints a specific load-balanced backend IP address that is returning a higher rate of errors than other peers in the same pool.
            </p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <MapPin className="h-5 w-5 shrink-0 text-blue-500" />
                <span>
                  <strong>Peer Outlier Analysis:</strong> Groups origin-bound traffic by individual IP addresses (OIP) to isolate failures to specific hosts rather than the entire cluster.
                </span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span>
                  <strong>Deployment or Node Crashes:</strong> Uncovers bad deployments, localized server failures, or configuration drift on a single host behind a global load balancer.
                </span>
              </li>
              <li className="flex gap-3">
                <Globe className="h-5 w-5 shrink-0 text-red-500" />
                <span>
                  <strong>Actionable Mitigation:</strong> Temporarily drain/disable the failing IP in your Fastly backend list or load balancer, then inspect that host for service crashes or configuration mismatches.
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
