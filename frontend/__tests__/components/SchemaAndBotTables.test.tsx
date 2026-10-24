/**
 * SchemaTab and BotSourcesPanel render their record lists through the shared
 * DataTableReadonly. Pins header semantics, stat formatting, that a schema
 * longer than one page is NOT truncated (pager is hidden), and the bot-source
 * Refresh action.
 */
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import * as React from 'react'
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'

import { renderWithQuery } from '../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { SchemaTab } from '@/app/logs/_sections/SchemaTab'
import { BotSourcesPanel } from '@/app/admin/_sections/BotSourcesPanel'
import type { components } from '@/types/api.generated'

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/virtual-mock')).reactVirtualModuleMock())

beforeAll(() => {
  stubResizeObserver()
})

beforeEach(() => {
  useServiceStore.setState({ activeServiceId: 'svc-tables', isInitialized: true })
})

type SchemaResponse = components['schemas']['SchemaResponse']

describe('SchemaTab', () => {
  it('renders every column past 50 rows with formatted stats', () => {
    const schema = Array.from({ length: 80 }, (_, i) => ({
      name: `col_${i}`,
      type: 'VARCHAR',
      null_percentage: i === 0 ? 25 : 0,
      approx_unique: 1234,
      min: 'a',
      max: 'z',
    }))
    render(<SchemaTab schemaData={{ schema } as unknown as SchemaResponse} isLoadingSchema={false} />)
    for (const name of ['Column Name', 'DuckDB Type', 'Populated %', 'Approx Unique', 'Min Value', 'Max Value']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('col_79')).toBeTruthy()
    expect(screen.getByText('75.0%')).toBeTruthy()
  })

  it('shows an em dash for columns without stats', () => {
    const schema = [{ name: 'bare', type: 'INTEGER' }]
    render(<SchemaTab schemaData={{ schema } as unknown as SchemaResponse} isLoadingSchema={false} />)
    const row = screen.getByText('bare').closest('tr') as HTMLElement
    expect(within(row).getAllByText('—').length).toBe(4)
  })
})

describe('BotSourcesPanel', () => {
  it('lists sources and POSTs a refresh for the chosen one', async () => {
    const refreshed: string[] = []
    const API_BASE = getApiBase()
    server.use(
      http.get(`${API_BASE}/api/admin/bot-sources`, () =>
        HttpResponse.json({
          sources: [
            { id: 'src-a', name: 'Alpha Bots', url: 'https://example.test/a', entry_count: 1500, last_updated: null },
          ],
          rdns: { total: 10, pending: 2, last_enrichment_at: null },
        }),
      ),
      http.post(`${API_BASE}/api/admin/bot-sources/:id/refresh`, ({ params }) => {
        refreshed.push(String(params.id))
        return HttpResponse.json({})
      }),
    )
    const user = userEvent.setup()
    renderWithQuery(<BotSourcesPanel />)
    expect(await screen.findByText('Alpha Bots')).toBeTruthy()
    expect(screen.getByText('not cached')).toBeTruthy()
    expect(screen.getByText('1,500')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: /Refresh/ }))
    await waitFor(() => expect(refreshed).toEqual(['src-a']))
  })

  const API_BASE = getApiBase()
  const botSourcesBody = {
    sources: [],
    rdns: { total: 10, pending: 2, last_enrichment_at: null },
  }

  it('Enrich Now POSTs the real rDNS enrich route and refetches the stats', async () => {
    let enrichCalls = 0
    let listCalls = 0
    server.use(
      http.get(`${API_BASE}/api/admin/bot-sources`, () => {
        listCalls += 1
        return HttpResponse.json(botSourcesBody)
      }),
      http.post(`${API_BASE}/api/admin/rdns/enrich`, () => {
        enrichCalls += 1
        return HttpResponse.json({ ok: true, summary: {} })
      }),
    )
    const user = userEvent.setup()
    renderWithQuery(<BotSourcesPanel />)
    await waitFor(() => expect(listCalls).toBe(1))
    await user.click(await screen.findByRole('button', { name: /Enrich Now/ }))
    await waitFor(() => expect(enrichCalls).toBe(1))
    await waitFor(() => expect(listCalls).toBe(2))
  })

  it('shows an inline error when rDNS enrichment fails', async () => {
    server.use(
      http.get(`${API_BASE}/api/admin/bot-sources`, () => HttpResponse.json(botSourcesBody)),
      http.post(`${API_BASE}/api/admin/rdns/enrich`, () =>
        HttpResponse.json({ detail: { error: 'rdns_failed', message: 'enrichment exploded' } }, { status: 500 }),
      ),
    )
    const user = userEvent.setup()
    renderWithQuery(<BotSourcesPanel />)
    await user.click(await screen.findByRole('button', { name: /Enrich Now/ }))
    expect(await screen.findByRole('alert')).toBeTruthy()
  })
})
