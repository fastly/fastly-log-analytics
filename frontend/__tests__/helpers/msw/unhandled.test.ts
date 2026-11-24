import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, describe, expect, test, vi } from 'vitest'

// Pins the R-2 contract of vitest.setup.ts's onUnhandledFrame callback: an
// unmatched request to the mocked backend or an external host fails as a
// network error without opening a connection, instead of passing through or
// resolving to a mocked 500, while test-owned loopback servers stay reachable.
describe('MSW unhandled-request policy', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  test.each([
    'http://127.0.0.1:8000/api/not-mocked',
    'https://example.invalid/not-mocked',
  ])('rejects %s before any connection is attempted', async (url) => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const error = await fetch(url).then(
      (res) => { throw new Error(`resolved with ${res.status}`) },
      (err: unknown) => err,
    )
    expect(error).toBeInstanceOf(TypeError)
    // A passed-through request would fail with a socket/DNS error code.
    expect(((error as TypeError).cause as { code?: string } | undefined)?.code).toBeUndefined()
  })

  test('passes requests to a test-owned loopback server through', async () => {
    const server = createServer((_req, res) => res.end('real'))
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    try {
      const { port } = server.address() as AddressInfo
      const res = await fetch(`http://127.0.0.1:${port}/`)
      expect(await res.text()).toBe('real')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
