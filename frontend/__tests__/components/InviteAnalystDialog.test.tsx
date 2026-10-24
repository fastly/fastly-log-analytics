import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'
import React from 'react'

const mockPost = vi.fn()
vi.mock('@/lib/api', () => ({
  client: { GET: vi.fn(), POST: (...a: unknown[]) => mockPost(...a), use: vi.fn() },
  extractApiError: vi.fn((e) => String(e)),
  getApiBase: vi.fn(() => 'http://test'),
}))

import { InviteAnalystDialog } from '@/components/InviteAnalystDialog/InviteAnalystDialog'
import { createTestQueryClient, makeQueryWrapper } from '../helpers/query'
import type { ServiceConfig } from '@/types/api'

const service = { service_id: 'svc-copy', name: 'Copy Svc' } as unknown as ServiceConfig

const invite = {
  name: 'analyst',
  service_id: 'svc-copy',
  fos_bucket: 'b',
  fos_region: 'r',
  fos_endpoint: 'e',
  fos_prefix: 'p',
  access_key_id: 'AKIAEXAMPLE',
  secret_key: 'not-a-real-secret',
}

const openResult = async () => {
  mockPost.mockResolvedValue({ data: invite })
  render(<InviteAnalystDialog service={service} open onOpenChange={vi.fn()} />, {
    wrapper: makeQueryWrapper(createTestQueryClient()),
  })
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: /generate invite/i }))
  })
  return screen.getByRole('button', { name: /copy json/i })
}

beforeEach(() => {
  mockPost.mockReset()
})

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

test('Copy JSON flips to Copied after a successful write and resets', async () => {
  const writeText = vi.fn().mockResolvedValue(undefined)
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
  const btn = await openResult()
  vi.useFakeTimers()
  await act(async () => {
    fireEvent.click(btn)
  })
  expect(writeText).toHaveBeenCalledTimes(1)
  expect(JSON.parse(writeText.mock.calls[0][0]).service_id).toBe('svc-copy')
  expect(screen.getByRole('button', { name: /copied!/i })).toBeInTheDocument()
  await act(async () => {
    vi.advanceTimersByTime(2000)
  })
  expect(screen.getByRole('button', { name: /copy json/i })).toBeInTheDocument()
})

test('Copy JSON does not claim success when the clipboard write is rejected', async () => {
  Object.defineProperty(navigator, 'clipboard', {
    value: { writeText: vi.fn().mockRejectedValue(new Error('denied')) },
    configurable: true,
  })
  const btn = await openResult()
  await act(async () => {
    fireEvent.click(btn)
  })
  expect(screen.queryByRole('button', { name: /copied!/i })).toBeNull()
})
