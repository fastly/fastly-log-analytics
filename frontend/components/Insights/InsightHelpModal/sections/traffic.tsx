import React from 'react'
import {
  ShieldAlert,
  Globe,
  Activity,
  TrendingUp,
  AlertTriangle,
  Building2,
  Database,
  BarChart,
} from 'lucide-react'
import type { InsightContent } from '../types'

export function getTrafficContent(id: string): InsightContent | null {
  switch (id) {
    case 'city_surges':
      return {
        title: 'City Traffic Surges',
        icon: <TrendingUp className="h-5 w-5 text-primary" />,
        fields: ['city'],
        description: (
          <div className="space-y-4">
            <p>Identifies cities experiencing massive, anomalous spikes in traffic volume.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Activity className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Volume Comparison:</strong> We compare current request counts per city to their historical average.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-red-500" />
                <span><strong>Attack Indicator:</strong> A 10x or 100x spike from a single city is a strong indicator of a localized botnet or DDoS attack originating from that region.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'new_city_traffic':
    case 'new_country_traffic':
      return {
        title: id === 'new_city_traffic' ? 'New City Traffic' : 'New Country Traffic',
        icon: <Globe className="h-5 w-5 text-primary" />,
        fields: [id === 'new_city_traffic' ? 'city' : 'country'],
        description: (
          <div className="space-y-4">
            <p>Flags traffic from locations that have had absolute zero presence in your historical baseline.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <Database className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Historical Absence:</strong> The system verifies that this location generated 0 requests over the entire baseline period.</span>
              </li>
              <li className="flex gap-3">
                <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Botnet Shift:</strong> While it could be legitimate new users, sudden high-volume traffic from entirely new regions often indicates a botnet shifting its attack infrastructure.</span>
              </li>
            </ul>
          </div>
        )
      }

    case 'asn_concentration':
      return {
        title: 'ASN Concentration',
        icon: <Building2 className="h-5 w-5 text-primary" />,
        fields: ['asn'],
        description: (
          <div className="space-y-4">
            <p>Detects when a single ISP or Hosting Provider (ASN) begins dominating your traffic volume.</p>
            <ul className="space-y-3 list-none pl-0 text-sm text-muted-foreground">
              <li className="flex gap-3">
                <BarChart className="h-5 w-5 shrink-0 text-blue-500" />
                <span><strong>Traffic Share:</strong> We calculate the percentage of total requests originating from each ASN.</span>
              </li>
              <li className="flex gap-3">
                <ShieldAlert className="h-5 w-5 shrink-0 text-yellow-500" />
                <span><strong>Datacenter Attacks:</strong> Legitimate traffic is usually distributed across consumer ISPs. Heavy concentration in a single hosting ASN (like AWS, DigitalOcean, or Hetzner) strongly suggests a scraper or volumetric attack.</span>
              </li>
            </ul>
          </div>
        )
      }

    default:
      return null
  }
}
