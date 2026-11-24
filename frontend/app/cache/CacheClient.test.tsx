import React from 'react'
import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

const mockInspect = vi.fn()
const mockClear = vi.fn()
const mockShowToast = vi.fn()

vi.mock('@/components/AppLayout', () => ({
  AppLayout: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="app-layout">{children}</div>
  ),
}))

vi.mock('@/lib/cloud-engine/mode', () => ({
  IS_CLOUD_MODE: true,
}))

vi.mock('@/lib/utils', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/utils')>()
  return {
    ...actual,
    showToast: (...args: unknown[]) => mockShowToast(...args),
  }
})

vi.mock('@/lib/cloud-engine/dashboard/browser-cache-stats', () => ({
  inspectBrowserCaches: () => mockInspect(),
  clearAllBrowserCaches: () => mockClear(),
}))

import { CacheClient } from './CacheClient'

describe('CacheClient', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('displays cache usage, quota, store breakdown, and last write warning', async () => {
    mockInspect.mockResolvedValue({
      usage: 5 * 1024 * 1024,
      quota: 100 * 1024 * 1024,
      persisted: true,
      indexedFiles: 12,
      indexedBytes: 3 * 1024 * 1024,
      lastError: {
        at: 1700000000000,
        cache: 'flp-parquet-v1',
        key: 'svc/part.parquet',
        bytes: 4096,
        usage: 90_000_000,
        quota: 100_000_000,
        message: 'QuotaExceededError: storage full',
      },
      opfs: { available: true, files: 10, bytes: 3 * 1024 * 1024 },
      caches: [
        {
          name: 'flp-parquet-v1',
          label: 'Request Parquet Cache',
          entries: 4,
          bytes: 1024 * 1024,
        },
      ],
      totalCachedEntries: 14,
      totalCachedBytes: 4 * 1024 * 1024,
      measuredAt: 1700000000000,
    })

    render(<CacheClient />)

    await waitFor(() => {
      expect(screen.getByTestId('browser-cache-total-bytes')).toHaveTextContent(
        '4 MB',
      )
    })
    expect(screen.getByTestId('browser-cache-opfs-bytes')).toHaveTextContent('3 MB')
    expect(screen.getByTestId('browser-cache-storage-bytes')).toHaveTextContent('1 MB')
    expect(screen.getByTestId('browser-cache-last-error')).toHaveTextContent(
      'QuotaExceededError: storage full',
    )
    expect(screen.getByText('Request Parquet Cache')).toBeInTheDocument()
    expect(screen.getByText('Persistent')).toBeInTheDocument()
  })

  it('clears browser caches and updates the displayed usage', async () => {
    mockInspect.mockResolvedValue({
      usage: 2 * 1024 * 1024,
      quota: 100 * 1024 * 1024,
      persisted: false,
      indexedFiles: 2,
      indexedBytes: 2 * 1024 * 1024,
      lastError: null,
      opfs: { available: true, files: 2, bytes: 2 * 1024 * 1024 },
      caches: [],
      totalCachedEntries: 2,
      totalCachedBytes: 2 * 1024 * 1024,
      measuredAt: 1700000000000,
    })
    mockClear.mockResolvedValue({
      usage: 0,
      quota: 100 * 1024 * 1024,
      persisted: false,
      indexedFiles: 0,
      indexedBytes: 0,
      lastError: null,
      opfs: { available: true, files: 0, bytes: 0 },
      caches: [],
      totalCachedEntries: 0,
      totalCachedBytes: 0,
      measuredAt: 1700000001000,
    })

    render(<CacheClient />)
    await waitFor(() => {
      expect(screen.getByTestId('browser-cache-total-bytes')).toHaveTextContent(
        '2 MB',
      )
    })

    fireEvent.click(screen.getByTestId('clear-browser-cache-btn'))

    await waitFor(() => {
      expect(mockClear).toHaveBeenCalledTimes(1)
      expect(screen.getByTestId('browser-cache-total-bytes')).toHaveTextContent(
        '0 B',
      )
    })
    expect(mockShowToast).toHaveBeenCalledWith('Browser cache cleared', 'success')
  })
})
