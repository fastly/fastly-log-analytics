import {
  createServer,
  type IncomingHttpHeaders,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, beforeEach, vi } from 'vitest'

export const mockCookies = vi.fn()
export const mockHeaders = vi.fn()

vi.mock('next/headers', () => ({
  cookies: () => mockCookies(),
  headers: () => mockHeaders(),
}))

export function headerGetter(entries: Record<string, string>) {
  const normalized: Record<string, string> = {}
  for (const [k, v] of Object.entries(entries)) {
    normalized[k.toLowerCase()] = v
  }
  return {
    get: (k: string) => normalized[k.toLowerCase()] ?? null,
  }
}

export interface SetupNextHeadersOptions {
  cookies?: string
  headers?: Record<string, string>
}

export function setupNextHeadersMock(defaults: SetupNextHeadersOptions = {}) {
  beforeEach(() => {
    mockCookies.mockReturnValue({
      toString: () => defaults.cookies ?? '',
      get: () => undefined,
    })
    mockHeaders.mockReturnValue(headerGetter(defaults.headers ?? {}))
  })

  afterEach(() => {
    mockCookies.mockReset()
    mockHeaders.mockReset()
    delete process.env.API_PROXY_URL
    vi.unstubAllEnvs()
  })

  return {
    mockCookies,
    mockHeaders,
    headerGetter,
    setHeaders: (entries: Record<string, string>) => {
      mockHeaders.mockReturnValue(headerGetter(entries))
    },
    setCookies: (
      cookieStr: string,
      getter?: (name: string) => { value: string } | undefined,
    ) => {
      mockCookies.mockReturnValue({
        toString: () => cookieStr,
        get: getter ?? (() => undefined),
      })
    },
  }
}

export interface CapturedHttpRequest {
  headers: IncomingHttpHeaders
  method: string
  url: string
  body: string
}

export interface TestHttpServerContext {
  port: number
  url: string
  requests: CapturedHttpRequest[]
  readonly lastRequest: CapturedHttpRequest | undefined
  clearRequests: () => void
}

export type TestHttpRequestHandler = (
  req: IncomingMessage,
  res: ServerResponse,
  captured: CapturedHttpRequest,
) => void | Promise<void>

export async function withTestHttpServer<T>(
  handler: TestHttpRequestHandler,
  fn: (ctx: TestHttpServerContext) => Promise<T>,
): Promise<T> {
  const requests: CapturedHttpRequest[] = []
  const server = createServer((req, res) => {
    const chunks: Buffer[] = []
    req.on('data', (chunk: Buffer) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
    })
    req.on('end', () => {
      const captured: CapturedHttpRequest = {
        headers: req.headers,
        method: req.method ?? '',
        url: req.url ?? '',
        body: Buffer.concat(chunks).toString('utf8'),
      }
      requests.push(captured)
      void handler(req, res, captured)
    })
  })

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as AddressInfo).port
  const url = `http://127.0.0.1:${port}`
  process.env.API_PROXY_URL = url

  const ctx: TestHttpServerContext = {
    port,
    url,
    requests,
    get lastRequest() {
      return requests[requests.length - 1]
    },
    clearRequests: () => {
      requests.length = 0
    },
  }

  try {
    return await fn(ctx)
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((err) => (err ? reject(err) : resolve())),
    )
  }
}
