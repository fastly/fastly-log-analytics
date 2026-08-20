import React from 'react'
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { ProxyWatchdogSection } from '@/app/security/_sections/ProxyWatchdogSection'

describe('ProxyWatchdogSection', () => {
  it('renders stats correctly', () => {
    const mockData = {
      _is_cached: false,
      active_proxies_count: 10,
      tunnel_requests_count: 500,
      distance_mismatches_count: 2,
      traffic_quality: [],
      suspicious_isps: [],
      active_clients: []
    }
    render(<ProxyWatchdogSection data={mockData} isLoading={false} />)
    expect(screen.getByText('Active VPN & Proxy Users')).toBeInTheDocument()
    expect(screen.getByText('10')).toBeInTheDocument()
  })
})
