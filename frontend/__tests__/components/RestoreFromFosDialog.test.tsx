import React from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw/http'
import {
  API_BASE,
  createTestQueryClient,
  renderWithQuery,
  stubMatchMedia,
  stubPointerCaptureAndScroll,
} from '@/__tests__/helpers/page-smoke'
import { server } from '@/__tests__/helpers/msw/handlers'
import { RestoreFromFosDialog } from '@/components/RestoreFromFosDialog'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import { subscribeCloudMetadataInvalidation } from '@/lib/cloud-engine/metadata-cache'
import { useServiceStore } from '@/stores/filterStore'

stubMatchMedia()
stubPointerCaptureAndScroll()

describe('RestoreFromFosDialog', () => {
  beforeEach(() => {
    setCloudModeRuntime(false)
    useServiceStore.setState({ activeServiceId: null, isInitialized: true })
  })

  afterEach(() => {
    setCloudModeRuntime(false)
    server.resetHandlers()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('High-Scale mode: restores from FOS with 0 active services, validates half-paired FOS keys, displays summary, clears secrets, and invalidates caches', async () => {
    const user = userEvent.setup()
    let capturedBody: Record<string, unknown> | null = null
    server.use(
      http.post(`${API_BASE}/api/high-scale/restore-from-fos`, async ({ request }) => {
        capturedBody = (await request.json()) as Record<string, unknown>
        return HttpResponse.json({
          status: 'ok',
          service_id: 'svc-cold-1',
          dr_state_restored: true,
          views_restored: 4,
          alerts_restored: 2,
          scoring_labels_restored: 7,
          audit_logs_restored: 12,
          manifests_registered: 9,
          domains_rebuilt: { request: { rows: 1000 }, rum_vitals: { rows: 50 } },
        })
      }),
    )

    const queryClient = createTestQueryClient()
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    const onRestored = vi.fn()
    const onOpenChange = vi.fn()

    renderWithQuery(
      <RestoreFromFosDialog open={true} onOpenChange={onOpenChange} onRestored={onRestored} />,
      { queryClient },
    )

    // High-Scale-specific fields are visible
    expect(screen.getByLabelText('Archive Prefix')).toHaveValue('high-scale/archive')
    expect(screen.getByRole('switch', { name: /Rebuild ClickHouse serving state/i })).toHaveAttribute('aria-checked', 'true')

    fireEvent.change(screen.getByLabelText('Fastly Service ID'), { target: { value: 'svc-cold-1' } })
    fireEvent.change(screen.getByLabelText('FOS Bucket'), { target: { value: 'dr-bucket' } })
    fireEvent.change(screen.getByLabelText('FOS Prefix (optional)'), { target: { value: 'logs/' } })
    fireEvent.change(screen.getByLabelText('Fastly API Key'), { target: { value: 'fastly-secret-token-123' } })

    // Expand explicit FOS disclosure and test half-paired rejection
    const disclosure = screen.getByRole('button', { name: /Explicit FOS Credentials/i })
    expect(disclosure).toHaveAttribute('aria-expanded', 'false')
    await user.click(disclosure)
    expect(disclosure).toHaveAttribute('aria-expanded', 'true')

    fireEvent.change(screen.getByLabelText('FOS Access Key ID'), { target: { value: 'only-access-key' } })
    await user.click(screen.getByRole('button', { name: 'Restore from FOS' }))
    expect(await screen.findByText(/Provide both FOS Access Key and Secret Key/i)).toBeInTheDocument()
    expect(capturedBody).toBeNull()

    // Clear half-paired key and submit with Fastly API key only (activeServiceId is null!)
    fireEvent.change(screen.getByLabelText('FOS Access Key ID'), { target: { value: '' } })
    await user.click(screen.getByRole('button', { name: 'Restore from FOS' }))

    await waitFor(() => {
      expect(screen.getByTestId('restore-fos-success')).toBeInTheDocument()
    })

    expect(capturedBody).toEqual({
      service_id: 'svc-cold-1',
      bucket: 'dr-bucket',
      region: 'us-east-1',
      prefix: 'logs/',
      archive_prefix: 'high-scale/archive',
      restore_serving_state: true,
      fastly_api_key: 'fastly-secret-token-123',
    })
    expect(capturedBody).not.toHaveProperty('fos_access_key')
    expect(capturedBody).not.toHaveProperty('fos_secret_key')

    // Summary shows all High-Scale metrics
    expect(screen.getByText('svc-cold-1')).toBeInTheDocument()
    expect(screen.getByText('request, rum_vitals')).toBeInTheDocument()
    expect(screen.getByText('12')).toBeInTheDocument()
    expect(screen.getByText('9')).toBeInTheDocument()

    // Caches invalidated
    const invalidatedKeys = invalidateSpy.mock.calls.map((c) => JSON.stringify(c[0]?.queryKey))
    expect(invalidatedKeys).toContain(JSON.stringify(['services']))
    expect(invalidatedKeys).toContain(JSON.stringify(['bootstrap']))

    // Clicking Open Dashboard invokes onRestored with service_id
    await user.click(screen.getByRole('button', { name: 'Open Dashboard' }))
    expect(onRestored).toHaveBeenCalledWith('svc-cold-1')
  })

  it('Cloud mode: hides High-Scale-only fields, posts exact RecoverInput payload, invalidates Cloud metadata cache, and renders summary', async () => {
    setCloudModeRuntime(true)
    const user = userEvent.setup()
    let capturedUrl = ''
    let capturedBody: Record<string, unknown> | null = null

    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const req = input instanceof Request ? input : new Request(new URL(String(input), 'http://localhost'), init)
        capturedUrl = new URL(req.url).pathname
        capturedBody = (await req.json()) as Record<string, unknown>
        return Response.json({
          ok: true,
          service_id: 'cloud-svc-9',
          source: 'dr_state',
          views_restored: 3,
          catalog_generation: 42,
          front_synced: true,
        })
      }),
    )

    const metaInvalidated = vi.fn()
    const unsubscribe = subscribeCloudMetadataInvalidation(metaInvalidated)
    const queryClient = createTestQueryClient()
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

    try {
      renderWithQuery(
        <RestoreFromFosDialog open={true} onOpenChange={vi.fn()} />,
        { queryClient },
      )

      // High-Scale-only fields are NOT rendered in Cloud mode
      expect(screen.queryByLabelText('Archive Prefix')).not.toBeInTheDocument()
      expect(screen.queryByRole('switch', { name: /Rebuild ClickHouse serving state/i })).not.toBeInTheDocument()

      fireEvent.change(screen.getByLabelText('Fastly Service ID'), { target: { value: 'cloud-svc-9' } })
      fireEvent.change(screen.getByLabelText('FOS Bucket'), { target: { value: 'cloud-bucket' } })
      fireEvent.change(screen.getByLabelText('Fastly API Token'), { target: { value: 'cloud-tok-secret' } })

      await user.click(screen.getByRole('button', { name: 'Recover from FOS' }))

      await waitFor(() => {
        expect(screen.getByTestId('restore-fos-success')).toBeInTheDocument()
      })

      expect(capturedUrl).toBe('/api/admin/provision/recover')
      expect(capturedBody).toEqual({
        token: 'cloud-tok-secret',
        service_id: 'cloud-svc-9',
        fos_bucket: 'cloud-bucket',
        fos_region: 'us-east-1',
        fos_prefix: '',
      })
      // Must never include High-Scale-only keys or blank optional secrets
      expect(capturedBody).not.toHaveProperty('archive_prefix')
      expect(capturedBody).not.toHaveProperty('restore_serving_state')
      expect(capturedBody).not.toHaveProperty('fos_access_key')
      expect(capturedBody).not.toHaveProperty('fos_secret_key')

      expect(screen.getByText('dr_state')).toBeInTheDocument()
      expect(screen.getByText('42')).toBeInTheDocument()
      expect(screen.getByText('Yes')).toBeInTheDocument()
      expect(metaInvalidated).toHaveBeenCalled()

      const invalidatedKeys = invalidateSpy.mock.calls.map((c) => JSON.stringify(c[0]?.queryKey))
      expect(invalidatedKeys).toContain(JSON.stringify(['services']))
      expect(invalidatedKeys).toContain(JSON.stringify(['bootstrap']))
    } finally {
      unsubscribe()
    }
  })

  it('Cloud mode: displays backend error codes and clears secrets when closed and reopened', async () => {
    setCloudModeRuntime(true)
    const user = userEvent.setup()

    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        Response.json({ detail: 'service_already_configured', error: 'service_already_configured' }, { status: 409 }),
      ),
    )

    const { rerender } = renderWithQuery(
      <RestoreFromFosDialog open={true} onOpenChange={vi.fn()} />,
    )

    fireEvent.change(screen.getByLabelText('Fastly Service ID'), { target: { value: 'svc-dup' } })
    fireEvent.change(screen.getByLabelText('FOS Bucket'), { target: { value: 'dup-bucket' } })
    fireEvent.change(screen.getByLabelText('Fastly API Token'), { target: { value: 'ephemeral-secret' } })

    await user.click(screen.getByRole('button', { name: 'Recover from FOS' }))
    expect(await screen.findByText(/service_already_configured/i)).toBeInTheDocument()

    // Close and reopen -> keyed body resets all state including secret input
    rerender(<RestoreFromFosDialog open={false} onOpenChange={vi.fn()} />)
    rerender(<RestoreFromFosDialog open={true} onOpenChange={vi.fn()} />)
    expect(screen.getByLabelText('Fastly API Token')).toHaveValue('')
    expect(screen.getByLabelText('Fastly Service ID')).toHaveValue('')
  })
})
