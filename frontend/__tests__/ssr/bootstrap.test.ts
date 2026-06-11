import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// Mock next/headers BEFORE importing the module under test. The mocks
// are returned from the fetchBootstrapServerSide call below so each
// test can configure them per-case.
const mockCookies = vi.fn()
const mockHeaders = vi.fn()
vi.mock('next/headers', () => ({
  cookies: () => mockCookies(),
  headers: () => mockHeaders(),
}))

const ORIGINAL_FETCH = global.fetch

beforeEach(() => {
  // Default to a populated cookie jar + no Caddy header (admin SSH-tunnel
  // shape). Individual tests override.
  mockCookies.mockReturnValue({ toString: () => 'session=abc123' })
  mockHeaders.mockReturnValue({ get: (_k: string) => null })
})

afterEach(() => {
  global.fetch = ORIGINAL_FETCH
  vi.restoreAllMocks()
  delete process.env.API_PROXY_URL
})

describe('fetchBootstrapServerSide', () => {
  it('returns the parsed JSON body on a 2xx response', async () => {
    process.env.API_PROXY_URL = 'http://backend:8000'
    const payload = { active_service_id: 'svc-1', share_banner: { sharing_active: false } }
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => payload,
    }) as unknown as typeof fetch

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    const out = await fetchBootstrapServerSide()
    expect(out).toEqual(payload)
  })

  it('forwards Cookie + X-Proxied-By-Caddy headers from the inbound request', async () => {
    process.env.API_PROXY_URL = 'http://backend:8000'
    mockCookies.mockReturnValue({ toString: () => 'session=abc; theme=dark' })
    mockHeaders.mockReturnValue({
      get: (k: string) => (k.toLowerCase() === 'x-proxied-by-caddy' ? 'true' : null),
    })
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({}),
    })
    global.fetch = fetchSpy as unknown as typeof fetch

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    await fetchBootstrapServerSide()

    expect(fetchSpy).toHaveBeenCalledTimes(1)
    const [url, opts] = fetchSpy.mock.calls[0]
    expect(url).toBe('http://backend:8000/api/bootstrap')
    expect(opts.headers.Cookie).toBe('session=abc; theme=dark')
    expect(opts.headers['X-Proxied-By-Caddy']).toBe('true')
  })

  it('omits the X-Proxied-By-Caddy header when the inbound request lacks it', async () => {
    // Admin SSH-tunnel shape — no Caddy marker on inbound, so we MUST NOT
    // synthesize one upstream (would mis-classify the request as analyst).
    process.env.API_PROXY_URL = 'http://backend:8000'
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({}),
    })
    global.fetch = fetchSpy as unknown as typeof fetch

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    await fetchBootstrapServerSide()
    const [, opts] = fetchSpy.mock.calls[0]
    expect('X-Proxied-By-Caddy' in opts.headers).toBe(false)
  })

  it('returns null on a non-2xx response', async () => {
    process.env.API_PROXY_URL = 'http://backend:8000'
    global.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
      json: async () => ({}),
    }) as unknown as typeof fetch
    // Silence the expected warn so the test output stays clean.
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalled()
  })

  it('returns null on a network/timeout error', async () => {
    process.env.API_PROXY_URL = 'http://backend:8000'
    global.fetch = vi.fn().mockRejectedValue(new Error('connection refused')) as unknown as typeof fetch
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
    expect(warn).toHaveBeenCalled()
  })

  it('returns null when API_PROXY_URL is unset (pure `next dev` outside docker compose)', async () => {
    // Env var absence is the canonical "no backend reachable from
    // SSR runtime" signal; client fetch picks up.
    const fetchSpy = vi.fn()
    global.fetch = fetchSpy as unknown as typeof fetch

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
    expect(fetchSpy).not.toHaveBeenCalled()
  })
})
