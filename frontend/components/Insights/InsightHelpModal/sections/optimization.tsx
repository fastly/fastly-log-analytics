import React from 'react'
import {
  User,
  Zap,
  TrendingDown,
  Info,
} from 'lucide-react'
import type { InsightContent } from '../types'

export function getOptimizationContent(id: string): InsightContent | null {
  switch (id) {
    case 'image_optimization_opportunities':
      return {
        title: 'Image Optimization Opportunities',
        icon: <Zap className="h-5 w-5 text-primary" />,
        fields: ['url', 'resp_bytes', 'ua'],
        description: (
          <div className="space-y-4">
            <p>Identifies images served without optimization parameters, which leads to unnecessarily high bandwidth usage and slower page loads.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <TrendingDown className="h-5 w-5 shrink-0 text-green-500" />
                <span><strong>Byte Savings:</strong> Modern formats like WebP or AVIF can often reduce image sizes by 50-80% without visible quality loss.</span>
              </li>
              <li className="flex gap-3">
                <User className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Mobile Impact:</strong> Large images sent to mobile devices are particularly expensive for users on limited data plans and slow down mobile page performance.</span>
              </li>
              <li className="flex gap-3">
                <Zap className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Easy Win:</strong> Most of these images can be optimized by enabling Fastly Image Optimizer and appending <code>?auto=webp</code> to your image URLs.</span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}

export function getDefaultContent(): InsightContent {
  return {
    title: 'Insight Analysis',
    icon: <Info className="h-5 w-5 text-primary" />,
    fields: [],
    description: (
      <div className="space-y-4">
        <p>This insight is powered by comparing your current traffic patterns against your selected historical baseline.</p>
        <p className="text-sm text-muted-foreground">We look for statistical outliers in volume, error rates, or performance metrics to surface potential issues before they become outages.</p>
      </div>
    )
  }
}
