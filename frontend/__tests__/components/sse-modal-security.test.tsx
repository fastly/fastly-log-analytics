import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { http, HttpResponse } from 'msw/http'
import { describe, expect, it, vi } from 'vitest'
import React from 'react'
import { SSEProgressView } from '@/components/SSEModal'
import { FileBrowser } from '@/app/logs/_sections/LogsFileBrowser'
import { useServiceStore } from '@/stores/filterStore'
import { renderWithQuery } from '../helpers/page-smoke'
import { server } from '../helpers/msw/handlers'

describe('Campaign B3 frontend DOM / URL hardening', () => {
  it('blocks javascript: and data: URLs in SSEProgressView errorLink while allowing https: URLs with noopener noreferrer', () => {
    const { rerender } = render(
      <SSEProgressView
        status="error"
        error="Stream failed"
        lines={[{ type: 'error', message: 'boom', link: 'javascript:alert(1)' }]}
      />,
    )
    expect(screen.queryByRole('link', { name: /Open Fastly to enable it/i })).toBeNull()

    rerender(
      <SSEProgressView
        status="error"
        error="Stream failed"
        lines={[{ type: 'error', message: 'boom', link: 'data:text/html,<script>alert(1)</script>' }]}
      />,
    )
    expect(screen.queryByRole('link', { name: /Open Fastly to enable it/i })).toBeNull()

    rerender(
      <SSEProgressView
        status="error"
        error="Stream failed"
        lines={[{ type: 'error', message: 'boom', link: 'https://manage.fastly.com/configure/services/abc' }]}
      />,
    )
    const link = screen.getByRole('link', { name: /Open Fastly to enable it/i })
    expect(link).toHaveAttribute('href', 'https://manage.fastly.com/configure/services/abc')
    expect(link).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('opens LogsFileBrowser downloads with noopener,noreferrer and URL-encoded service id', async () => {
    server.use(
      http.get('*/api/admin/raw-tree', () =>
        HttpResponse.json({
          nodes: [
            { name: '2026-11', type: 'directory', prefix: 'raw/2026-11/', size: 100 },
            { name: 'sample.log', type: 'file', key: 'raw/sample.log', size: 42 },
          ],
        }),
      ),
    )
    useServiceStore.setState({ activeServiceId: 'svc&evil=1' })
    const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null)
    try {
      renderWithQuery(<FileBrowser type="raw" />)
      const folderBtn = await screen.findByRole('button', { name: /Download folder as ZIP/i })
      const fileBtn = await screen.findByRole('button', { name: /Download sample\.log/i })

      fireEvent.click(folderBtn)
      fireEvent.click(fileBtn)

      await waitFor(() => expect(openSpy).toHaveBeenCalledTimes(2))
      expect(openSpy).toHaveBeenNthCalledWith(
        1,
        '/api/download-folder?service=svc%26evil%3D1&root=raw&prefix=raw%2F2026-11%2F',
        '_blank',
        'noopener,noreferrer',
      )
      expect(openSpy).toHaveBeenNthCalledWith(
        2,
        '/api/download?service=svc%26evil%3D1&key=raw%2Fsample.log',
        '_blank',
        'noopener,noreferrer',
      )
    } finally {
      openSpy.mockRestore()
    }
  })
})
