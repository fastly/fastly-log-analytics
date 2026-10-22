/**
 * CreateAlertForm validation + submit contract (characterization tests that
 * must hold across the react-hook-form migration):
 *   - Create is disabled until Name and Threshold are filled.
 *   - Required-field errors appear only after blur (touched) with the exact
 *     copy "Name is required." / "Threshold is required.".
 *   - Create POSTs /api/alerts/ with numeric threshold; edit PUTs by id.
 *   - Z-score alerts send zscore_threshold + baseline_period_days and operator ">".
 *   - Legacy webhook URL is omitted when blank.
 */
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import { stubPointerCaptureAndScroll } from '@/__tests__/helpers/jsdom-shims'
import * as React from 'react'
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClientProvider } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'

import { createTestQueryClient } from '../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { CreateAlertForm } from '@/app/alerts/_sections/AlertEditor'
import type { components } from '@/types/api.generated'

vi.mock('@/app/alerts/_sections/AlertPreview', () => ({ AlertPreview: () => <div data-testid="alert-preview" /> }))
vi.mock('@/hooks/useBootstrap', () => ({ useBootstrap: () => ({ data: undefined }) }))
vi.mock('@/hooks/useIsAnalyst', () => ({ useIsAnalyst: () => false }))

const API_BASE = getApiBase()
const SVC = 'svc-alerts'

type Alert = components['schemas']['Alert']

let posted: Record<string, unknown>[] = []
let putCalls: { id: string; body: Record<string, unknown> }[] = []

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

beforeEach(() => {
  posted = []
  putCalls = []
  useServiceStore.setState({
    activeServiceId: SVC,
    services: [{ id: SVC, name: 'Svc', accessLevel: 'read_write' }],
    isInitialized: true,
  })
  server.use(
    http.post(`${API_BASE}/api/alerts/preview`, () => HttpResponse.json({ data: null })),
    http.post(`${API_BASE}/api/alerts/`, async ({ request }) => {
      posted.push((await request.json()) as Record<string, unknown>)
      return HttpResponse.json({})
    }),
    http.put(`${API_BASE}/api/alerts/:id`, async ({ request, params }) => {
      putCalls.push({ id: String(params.id), body: (await request.json()) as Record<string, unknown> })
      return HttpResponse.json({})
    }),
  )
})

function renderForm(initialAlert?: Alert | null, onSuccess: () => void = () => {}) {
  return render(
    <QueryClientProvider client={createTestQueryClient()}>
      <CreateAlertForm initialAlert={initialAlert} onSuccess={onSuccess} />
    </QueryClientProvider>,
  )
}

describe('CreateAlertForm validation', () => {
  it('disables Create until name and threshold are filled', async () => {
    const user = userEvent.setup()
    renderForm()
    const create = screen.getByRole('button', { name: 'Create Alert' }) as HTMLButtonElement
    expect(create.disabled).toBe(true)
    await user.type(screen.getByLabelText('Alert Name'), 'High 5xx')
    expect(create.disabled).toBe(true)
    await user.type(document.getElementById('threshold') as HTMLInputElement, '100')
    await waitFor(() => expect(create.disabled).toBe(false))
  })

  it('shows required errors only after blur, with the original copy', async () => {
    const user = userEvent.setup()
    renderForm()
    expect(screen.queryByText('Name is required.')).toBeNull()
    expect(screen.queryByText('Threshold is required.')).toBeNull()

    const name = screen.getByLabelText('Alert Name')
    await user.click(name)
    await user.tab()
    expect(await screen.findByText('Name is required.')).toBeTruthy()
    expect(name.getAttribute('aria-invalid')).toBe('true')

    const threshold = document.getElementById('threshold') as HTMLInputElement
    await user.click(threshold)
    await user.tab()
    expect(await screen.findByText('Threshold is required.')).toBeTruthy()

    await user.type(name, 'x')
    await waitFor(() => expect(screen.queryByText('Name is required.')).toBeNull())
  })
})

describe('CreateAlertForm submit', () => {
  it('POSTs a new alert with a numeric threshold and closes', async () => {
    const user = userEvent.setup()
    const onSuccess = vi.fn()
    renderForm(null, onSuccess)
    await user.type(screen.getByLabelText('Alert Name'), 'Traffic spike')
    await user.type(document.getElementById('threshold') as HTMLInputElement, '250.5')
    await user.click(screen.getByRole('button', { name: 'Create Alert' }))
    await waitFor(() => expect(posted.length).toBe(1))
    expect(posted[0]).toMatchObject({
      service_id: SVC,
      name: 'Traffic spike',
      category: 'traffic',
      metric: 'requests',
      evaluation_type: 'absolute',
      evaluation_scope: 'all',
      operator: '>',
      threshold: 250.5,
      window_min: 5,
      enabled: true,
    })
    expect(posted[0].webhook_url).toBeUndefined()
    expect(posted[0].zscore_threshold).toBeUndefined()
    await waitFor(() => expect(onSuccess).toHaveBeenCalled())
  })

  it('PUTs an edited alert by id and keeps its enabled flag', async () => {
    const user = userEvent.setup()
    const alert = {
      id: 'a-1',
      service_id: SVC,
      name: 'Existing',
      category: 'reliability',
      metric: '5xx',
      evaluation_type: 'absolute',
      evaluation_scope: 'edge',
      operator: '>=',
      threshold: 10,
      window_min: 15,
      enabled: false,
      channels: [],
      webhook_url: 'https://hooks.example.test/x',
    } as unknown as Alert
    renderForm(alert)
    const name = screen.getByLabelText('Alert Name') as HTMLInputElement
    expect(name.value).toBe('Existing')
    await user.type(name, ' v2')
    await user.click(screen.getByRole('button', { name: 'Save Changes' }))
    await waitFor(() => expect(putCalls.length).toBe(1))
    expect(putCalls[0].id).toBe('a-1')
    expect(putCalls[0].body).toMatchObject({
      name: 'Existing v2',
      operator: '>=',
      threshold: 10,
      window_min: 15,
      enabled: false,
      webhook_url: 'https://hooks.example.test/x',
    })
  })

  it('z-score alerts send zscore fields and a ">" operator', async () => {
    const user = userEvent.setup()
    const alert = {
      id: 'a-2',
      service_id: SVC,
      name: 'Anomaly',
      category: 'traffic',
      metric: 'requests',
      evaluation_type: 'anomaly_zscore',
      evaluation_scope: 'all',
      operator: '<',
      threshold: 3,
      window_min: 5,
      zscore_threshold: 4.5,
      baseline_period_days: 14,
      enabled: true,
      channels: [],
    } as unknown as Alert
    renderForm(alert)
    await user.click(screen.getByRole('button', { name: 'Save Changes' }))
    await waitFor(() => expect(putCalls.length).toBe(1))
    expect(putCalls[0].body).toMatchObject({
      operator: '>',
      threshold: 4.5,
      zscore_threshold: 4.5,
      baseline_period_days: 14,
    })
  })

  it('does not submit when required fields are empty', async () => {
    const user = userEvent.setup()
    renderForm()
    const form = screen.getByRole('button', { name: 'Create Alert' }).closest('form') as HTMLFormElement
    form.requestSubmit?.()
    await user.click(screen.getByLabelText('Alert Name'))
    await new Promise((r) => setTimeout(r, 50))
    expect(posted.length).toBe(0)
  })
})
