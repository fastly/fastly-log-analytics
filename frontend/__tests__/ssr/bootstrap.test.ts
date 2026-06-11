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

  it('admin SSH-tunnel path: forwards cookies, NO X-Remote-Analyst header', async () => {
    // No X-Proxied-By-Caddy on inbound → must NOT set X-Remote-Analyst
    // upstream. Doing so would mis-classify the admin as a remote
    // analyst (gated by tunnel.is_sharing_active() on the backend but
    // still wrong intent).
    process.env.API_PROXY_URL = 'http://backend:8000'
    mockCookies.mockReturnValue({ toString: () => 'session=abc; theme=dark' })
    mockHeaders.mockReturnValue({ get: (_k: string) => null })
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({}),
    })
    global.fetch = fetchSpy as unknown as typeof fetch

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    await fetchBootstrapServerSide()
    const [, opts] = fetchSpy.mock.calls[0]
    expect(opts.headers.Cookie).toBe('session=abc; theme=dark')
    expect('X-Remote-Analyst' in opts.headers).toBe(false)
    // X-Proxied-By-Caddy must not leak upstream — the backend would
    // ignore it from loopback but forwarding it is a wrong-shape signal.
    expect('X-Proxied-By-Caddy' in opts.headers).toBe(false)
  })

  it('public Caddy path: sets X-Remote-Analyst:1 AND forwards inbound Host', async () => {
    // This is the SECURITY-critical case. The SSR runtime hits the
    // backend over loopback; without X-Remote-Analyst the backend
    // returns a full admin payload to anonymous public visitors. The
    // Host forward is required to pass the backend's
    // _remote_host_allowed gate (remote_access.py:296) — without it
    // the upstream fetch's implicit `Host: backend:8000` triggers a
    // 400 host_not_allowed.
    process.env.API_PROXY_URL = 'http://backend:8000'
    mockHeaders.mockReturnValue({
      get: (k: string) => {
        const norm = k.toLowerCase()
        if (norm === 'x-proxied-by-caddy') return 'true'
        if (norm === 'host') return 'fastly-log-analytics.global.ssl.fastly.net'
        return null
      },
    })
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({}),
    })
    global.fetch = fetchSpy as unknown as typeof fetch

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    await fetchBootstrapServerSide()
    const [, opts] = fetchSpy.mock.calls[0]
    expect(opts.headers['X-Remote-Analyst']).toBe('1')
    expect(opts.headers.Host).toBe('fastly-log-analytics.global.ssl.fastly.net')
  })

  it('returns null on a non-2xx response', async () => {
    process.env.API_PROXY_URL = 'http://backend:8000'
    global.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
      json: async () => ({}),
    }) as unknown as typeof fetch
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
    const fetchSpy = vi.fn()
    global.fetch = fetchSpy as unknown as typeof fetch

    const { fetchBootstrapServerSide } = await import('@/lib/ssr/bootstrap')
    const out = await fetchBootstrapServerSide()
    expect(out).toBeNull()
    expect(fetchSpy).not.toHaveBeenCalled()
  })
})
