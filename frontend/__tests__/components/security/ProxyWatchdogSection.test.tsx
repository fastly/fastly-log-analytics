import React from 'react'
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import type { components } from '@/types/api.generated'
import { ProxyWatchdogSection } from '@/app/security/_sections/ProxyWatchdogSection'

type SecurityData = components['schemas']['SecurityAggregatesResponse']

function securityData(): SecurityData {
  return {
    _is_cached: false,
    tls_fingerprints: [],
    fingerprint_coverage: {},
    req_size_dist: [],
    top_ips_header: [],
    ipv6_adoption: [],
    proxy_dist: [],
    conn_reuse_dist: [],
    verified_bots_ts: [],
    ngwaf_configured: false,
    ngwaf_verified_bots: [],
    ngwaf_verified_bots_ts: [],
    wellknown_bots: [],
    bot_efficiency: [],
    bots: [],
    ngwaf_bots: [],
    active_proxies_count: 10,
    tunnel_requests_count: 500,
    distance_mismatches_count: 2,
    traffic_quality: [],
    suspicious_isps: [],
    active_clients: [],
  }
}

describe('ProxyWatchdogSection', () => {
  it('renders stats and all card headers correctly', () => {
    const mockData = securityData()
    render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
    expect(screen.getByText('Active VPN & Proxy Users')).toBeInTheDocument()
    expect(screen.getByText('10')).toBeInTheDocument()
    expect(screen.getByText('Tunnel Requests')).toBeInTheDocument()
    expect(screen.getByText('500')).toBeInTheDocument()
    expect(screen.getByText('Impossible Distance Mismatches')).toBeInTheDocument()
    expect(screen.getByText('2')).toBeInTheDocument()

    expect(screen.getByText('Traffic Quality Breakdown')).toBeInTheDocument()
    expect(screen.getByText('Suspicious ISPs & Providers')).toBeInTheDocument()
    expect(screen.getByText('VPN/Proxy Export Center')).toBeInTheDocument()
    expect(screen.getByText('Active Suspicious Clients')).toBeInTheDocument()
  })

  it('enforces zero-CLS layout reservations on stat cards and card containers', () => {
    const mockData = securityData()
    const { container } = render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
    expect(container.querySelectorAll('.\\\[contain-intrinsic-size\\:120px\\\]').length).toBeGreaterThanOrEqual(3)
    expect(container.querySelectorAll('.\\\[contain-intrinsic-size\\:320px\\\]').length).toBeGreaterThanOrEqual(2)
    expect(container.querySelectorAll('.\\\[contain-intrinsic-size\\:140px\\\]').length).toBeGreaterThanOrEqual(1)
    expect(container.querySelectorAll('.\\\[contain-intrinsic-size\\:600px\\\]').length).toBeGreaterThanOrEqual(1)
  })
})
