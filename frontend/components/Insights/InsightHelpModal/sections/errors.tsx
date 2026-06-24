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

    default:
      return null
  }
}
