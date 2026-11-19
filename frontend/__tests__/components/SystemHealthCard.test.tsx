/**
 * @vitest-environment jsdom
 */
import * as React from 'react'
import { screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { renderWithQuery, stubMatchMedia, stubResizeObserver } from '@/__tests__/helpers/page-smoke'
import { server } from '@/__tests__/helpers/msw/handlers'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import {
  DEFAULT_CLOCK_INTERVAL_S,
  SystemHealthCard,
  TICK_BACKEND_CALL_CEILING,
  TICK_VCPU_BUDGET_MS,
  summarizeTickHealth,
} from '@/components/SystemHealthCard'

stubMatchMedia()
stubResizeObserver()

afterEach(() => {
  setCloudModeRuntime(false)
  server.resetHandlers()
  vi.restoreAllMocks()
})

describe('summarizeTickHealth', () => {
  it('returns waiting when there are no cron runs yet', () => {
    const summary = summarizeTickHealth([], Date.UTC(2026, 10, 18, 12, 0, 0))
    expect(summary.tick_health).toBe('waiting')
    expect(summary.last_tick_at).toBeNull()
  })

  it('returns healthy when latest run is fresh and within resource ceilings', () => {
    const now = Date.UTC(2026, 10, 18, 12, 0, 0)
    const summary = summarizeTickHealth(
      [{ started_at: new Date(now - 30_000).toISOString(), backend_calls: 4, vcpu_ms: 120 }],
      now,
    )
    expect(summary.tick_health).toBe('healthy')
  })

  it('returns degraded when latest run is stale or exceeds resource ceilings', () => {
    const now = Date.UTC(2026, 10, 18, 12, 0, 0)
    expect(
      summarizeTickHealth(
        [{ started_at: new Date(now - (DEFAULT_CLOCK_INTERVAL_S * 3 + 10) * 1000).toISOString(), backend_calls: 1, vcpu_ms: 10 }],
        now,
      ).tick_health,
    ).toBe('degraded')
    expect(
      summarizeTickHealth(
        [{ started_at: new Date(now - 10_000).toISOString(), backend_calls: TICK_BACKEND_CALL_CEILING + 1, vcpu_ms: 10 }],
        now,
      ).tick_health,
    ).toBe('degraded')
    expect(
      summarizeTickHealth(
        [{ started_at: new Date(now - 10_000).toISOString(), backend_calls: 1, vcpu_ms: TICK_VCPU_BUDGET_MS + 1 }],
        now,
      ).tick_health,
    ).toBe('degraded')
  })
})

describe('SystemHealthCard in Cloud mode', () => {
  it('shows WAITING (never DEGRADED) when cron_runs is empty even if last_tick_at is stale from an earlier probe', async () => {
    setCloudModeRuntime(true)
    const staleProbeAt = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString()
    server.use(
      http.get('/api/sync-status', () =>
        HttpResponse.json({
          configured: false,
          last_tick_at: staleProbeAt,
          clock_interval_s: 120,
          cron_runs: [],
        }),
      ),
    )

    renderWithQuery(<SystemHealthCard />)
    await waitFor(() => {
      expect(screen.getByText('WAITING')).toBeInTheDocument()
    })
    expect(screen.queryByText('DEGRADED')).not.toBeInTheDocument()
  })

  it('shows HEALTHY when cron_runs is empty and last_tick_at is fresh within 3x clock interval', async () => {
    setCloudModeRuntime(true)
    const freshProbeAt = new Date(Date.now() - 30 * 1000).toISOString()
    server.use(
      http.get('/api/sync-status', () =>
        HttpResponse.json({
          configured: false,
          last_tick_at: freshProbeAt,
          clock_interval_s: 120,
          cron_runs: [],
        }),
      ),
    )

    renderWithQuery(<SystemHealthCard />)
    await waitFor(() => {
      expect(screen.getByText('HEALTHY')).toBeInTheDocument()
    })
    expect(screen.queryByText('DEGRADED')).not.toBeInTheDocument()
  })

  it('shows DEGRADED when cron_runs has a failing/stale run', async () => {
    setCloudModeRuntime(true)
    const staleRunAt = new Date(Date.now() - 60 * 60 * 1000).toISOString()
    server.use(
      http.get('/api/sync-status', () =>
        HttpResponse.json({
          configured: true,
          last_tick_at: staleRunAt,
          clock_interval_s: 120,
          cron_runs: [{ started_at: staleRunAt, backend_calls: 4, vcpu_ms: 50, status: 'ok' }],
        }),
      ),
    )

    renderWithQuery(<SystemHealthCard />)
    await waitFor(() => {
      expect(screen.getByText('DEGRADED')).toBeInTheDocument()
    })
  })
})
