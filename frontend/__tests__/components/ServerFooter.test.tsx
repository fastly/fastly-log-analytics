/**
 * @vitest-environment node
 */
import { beforeEach, describe, expect, it, vi } from 'vitest'

let mockHeaders: Record<string, string> = {}
vi.mock('next/headers', () => ({
  headers: async () => ({ get: (k: string) => mockHeaders[k.toLowerCase()] ?? null }),
}))

import { ServerFooter } from '@/components/ServerFooter'

beforeEach(() => {
  mockHeaders = {}
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, text: async () => 'secret-instance' }))
  process.env.COMMIT_HASH = 'abc1234'
})

describe('ServerFooter', () => {
  it('renders infrastructure details for admin (non-Caddy) requests', async () => {
    const el = await ServerFooter()
    expect(el).not.toBeNull()
    expect(JSON.stringify(el)).toContain('abc1234')
  })

  it('renders nothing for requests proxied through Caddy (analyst / public)', async () => {
    mockHeaders = { 'x-proxied-by-caddy': 'true' }
    const el = await ServerFooter()
    expect(el).toBeNull()
    expect(fetch).not.toHaveBeenCalled()
  })
})
