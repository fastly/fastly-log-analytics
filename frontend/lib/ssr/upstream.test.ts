import { beforeEach, describe, expect, it, vi } from 'vitest'

const { mockSsrUpstreamGet } = vi.hoisted(() => ({
  mockSsrUpstreamGet: vi.fn(),
}))

vi.mock('next/headers', () => ({
  cookies: vi.fn(async () => ({ toString: (): string => '', get: (): undefined => undefined })),
  headers: vi.fn(async () => new Headers()),
}))

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return {
    ...actual,
    ssrUpstreamGet: mockSsrUpstreamGet,
  }
})

import { resolvePageServiceId } from './upstream'

describe('T5-FE-02: SSR resolvePageServiceId service parameter validation', () => {
  beforeEach(() => {
    mockSsrUpstreamGet.mockReset()
    mockSsrUpstreamGet.mockResolvedValue({
      statusCode: 200,
      body: JSON.stringify({
        active_service_id: 'svc_fallback_01',
        services: [{ id: 'svc_first_01' }],
      }),
    })
  })

  it('rejects path-traversal and malformed ?service= parameters and falls back to bootstrap active_service_id', async () => {
    for (const bad of ['..', '../admin/config', 'svc?foo=bar#baz', 'svc/evil', 'a'.repeat(65)]) {
      const resolved = await resolvePageServiceId({ service: bad })
      expect(resolved, bad).toBe('svc_fallback_01')
    }
  })

  it('accepts valid alphanumeric/underscore/hyphen service IDs without extra bootstrap fetch', async () => {
    const resolved = await resolvePageServiceId({ service: 'svc_prod-01' })
    expect(resolved).toBe('svc_prod-01')
    expect(mockSsrUpstreamGet).not.toHaveBeenCalled()
  })
})
