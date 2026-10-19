/**
 * @vitest-environment jsdom
 */
import { render, screen, waitFor } from '@testing-library/react'
import { http, HttpResponse } from 'msw'
import { describe, it, expect } from 'vitest'

import { CorrelatorLeaderboard } from '@/app/control-room/_sections/ControlRoomClient'
import { createTestQueryClient, makeQueryWrapper } from '../../helpers/query'
import { server } from '../../../tests/msw/server'

describe('CorrelatorLeaderboard', () => {
  it('posts the dimension/limit and renders the leaderboard', async () => {
    let body: unknown = null
    server.use(
      http.post('*/api/services/svc-1/control-room/correlate', async ({ request }) => {
        body = await request.json()
        return HttpResponse.json({
          dimension: 'waf_sig',
          top: [{ value: 'sqli', count: 7 }],
          freshness: { latest_log_at: null, lag_seconds: 0 },
        })
      }),
    )
    render(<CorrelatorLeaderboard dimension="waf_sig" serviceId="svc-1" limit={5} />, {
      wrapper: makeQueryWrapper(createTestQueryClient()),
    })
    expect(await screen.findByText('sqli')).toBeInTheDocument()
    expect(body).toEqual({ dimension: 'waf_sig', window_minutes: 60, limit: 5 })
  })

  it('does not fetch without a service id', async () => {
    let calls = 0
    server.use(
      http.post('*/control-room/correlate', () => {
        calls++
        return HttpResponse.json({ dimension: 'waf_sig', top: [], freshness: { latest_log_at: null, lag_seconds: 0 } })
      }),
    )
    render(<CorrelatorLeaderboard dimension="waf_sig" serviceId={null} />, {
      wrapper: makeQueryWrapper(createTestQueryClient()),
    })
    await waitFor(() => expect(calls).toBe(0))
  })
})
