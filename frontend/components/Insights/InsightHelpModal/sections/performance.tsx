import React from 'react'
import {
  Zap,
  Activity,
  Clock,
  TrendingUp,
  Info,
  Network,
} from 'lucide-react'
import type { InsightContent } from '../types'

export function getPerformanceContent(id: string): InsightContent | null {
  switch (id) {
    case 'city_latency_regressions':
      return {
        title: 'City Latency Regressions',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['city', 'elapsed'],
        description: (
          <div className="space-y-4">
            <p>Detects when specific cities begin experiencing severe latency (slowness) compared to their normal baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>P95 Latency:</strong> We track the 95th percentile response time (`elapsed`) for every city.</span>
              </li>
              <li className="flex gap-3">
                <Clock className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Slowdown Detection:</strong> Triggers when a city's P95 latency doubles or triples, often indicating congestion at a specific edge node or peering point.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'asn_metro_performance':
      return {
        title: 'ASN/Metro Performance Regressions',
        icon: <Network className="h-5 w-5 text-primary" />,
        fields: ['asn', 'metro', 'tcp_rtt'],
        description: (
          <div className="space-y-4">
            <p>Monitors network-level degradation by tracking TCP Round Trip Time (RTT) across specific Internet Service Providers (ASNs) in specific geographic metros.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Zap className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Granular Tracking:</strong> Network performance varies wildly by region. We establish baselines for each ISP in each specific city/metro area.</span>
              </li>
              <li className="flex gap-3">
                <TrendingUp className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>ISP Outages:</strong> A sudden spike in TCP RTT for Comcast users in Chicago indicates a localized ISP peering issue or fiber cut.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'latency_regression':
      return {
        title: 'Global Latency Regression',
        icon: <Clock className="h-5 w-5 text-primary" />,
        fields: ['url', 'elapsed'],
        description: (
          <div className="space-y-4">
            <p>Detects specific URLs or API endpoints that have become significantly slower to process compared to their historical baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Endpoint Profiling:</strong> We track the P95 latency for every unique URL path over the historical baseline.</span>
              </li>
              <li className="flex gap-3">
                <TrendingUp className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Bad Deployments:</strong> Flags URLs where the processing time has doubled or worse, commonly highlighting an unoptimized database query or a regression in a recent code deployment.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'network_asn_health':
      return {
        title: 'Network & ASN Health',
        icon: <Activity className="h-5 w-5 text-primary" />,
        fields: ['asn', 'tcp_rtt', 'ploss', 'rtt_min', 'rtt_var'],
        description: (
          <div className="space-y-4">
            <p>Analyzes the fundamental TCP connection quality between end users and the Fastly edge, segmented by ISP.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Network className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Deep Metrics:</strong> Uses low-level kernel metrics like Packet Loss (`ploss`), Jitter (`rtt_var`), and minimum latency (`rtt_min`).</span>
              </li>
              <li className="flex gap-3">
                <Info className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Video & Gaming:</strong> Essential for highly-sensitive workloads like streaming video or multiplayer gaming where packet loss and jitter impact user experience far more than pure throughput.</span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}
