import { afterEach, describe, expect, it, vi } from 'vitest'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { NextRequest } from 'next/server'
import { proxy as middleware } from '@/proxy'
import { proxyUpstreamRequest } from '@/lib/upstream-proxy'
import { GET as apiGet, POST as apiPost } from '@/app/api/[...path]/route'
import { GET as jsGet } from '@/app/js/[...path]/route'

afterEach(() => {
  delete process.env.API_PROXY_URL
  delete process.env.NEXT_PUBLIC_API_PROXY_URL
})

async function withUpstreamServer(
  handler: (req: IncomingMessage, res: ServerResponse) => void,
) {
  const http = await import('node:http')
  const server = http.createServer(handler)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as { port: number }).port
  process.env.API_PROXY_URL = `http://127.0.0.1:${port}`
  return {
    port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  }
}

describe('upstream API/JS proxy route handler (ECONNRESET resilience)', () => {
  it('middleware passes /api/* and /js/* to route handlers instead of NextResponse.rewrite', () => {
    const apiReq = new NextRequest('http://localhost:3002/api/admin/events/stream?service_id=demo')
    const apiRes = middleware(apiReq)
    expect(apiRes.status).toBe(200)
    expect(apiRes.headers.get('x-middleware-rewrite')).toBeNull()

    const jsReq = new NextRequest('http://localhost:3002/js/rum.js')
    const jsRes = middleware(jsReq)
    expect(jsRes.status).toBe(200)
    expect(jsRes.headers.get('x-middleware-rewrite')).toBeNull()
  })

  it('forwards GET and POST requests with query params, headers, body, and Set-Cookie using connection: close', async () => {
    let seenMethod = ''
    let seenUrl = ''
    let seenConn = ''
    let seenBody = ''
    const server = await withUpstreamServer((req, res) => {
      seenMethod = req.method || ''
      seenUrl = req.url || ''
      seenConn = req.headers.connection || ''
      const chunks: Buffer[] = []
      req.on('data', (c: Buffer) => chunks.push(c))
      req.on('end', () => {
        seenBody = Buffer.concat(chunks).toString('utf8')
        res.statusCode = 201
        res.setHeader('Content-Type', 'application/json')
        res.setHeader('Set-Cookie', ['a=1; Path=/', 'b=2; Path=/'])
        res.end(JSON.stringify({ ok: true, echoed: seenBody }))
      })
    })
    try {
      const req = new NextRequest('http://localhost:3002/api/query?range=1h', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-custom': 'val' },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      })
      const res = await apiPost(req)
      expect(res.status).toBe(201)
      expect(seenMethod).toBe('POST')
      expect(seenUrl).toBe('/api/query?range=1h')
      expect(seenConn).toBe('close')
      expect(await res.json()).toEqual({ ok: true, echoed: '{"sql":"SELECT 1"}' })
      expect(res.headers.getSetCookie()).toEqual(['a=1; Path=/', 'b=2; Path=/'])
    } finally {
      await server.close()
    }
  })

  it('retries idempotent GET once when upstream socket resets with ECONNRESET before headers', async () => {
    let attempts = 0
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const server = await withUpstreamServer((req, res) => {
      attempts += 1
      if (attempts === 1) {
        req.socket.destroy()
        return
      }
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.end('{"healed":true}')
    })
    try {
      const req = new NextRequest('http://localhost:3002/api/bootstrap')
      const res = await apiGet(req)
      expect(res.status).toBe(200)
      expect(await res.json()).toEqual({ healed: true })
      expect(attempts).toBe(2)
      expect(errorSpy).not.toHaveBeenCalled()
    } finally {
      errorSpy.mockRestore()
      await server.close()
    }
  })

  it('streams SSE responses and suppresses ECONNRESET logs on client abort or upstream disconnect', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const server = await withUpstreamServer((req, res) => {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
      })
      res.write('data: {"tick":1}\n\n')
      setTimeout(() => {
        req.socket.destroy()
      }, 20)
    })
    try {
      const abortController = new AbortController()
      const req = new Request('http://localhost:3002/api/admin/events/stream', {
        signal: abortController.signal,
      })
      const res = await proxyUpstreamRequest(req)
      expect(res.status).toBe(200)
      expect(res.headers.get('content-type')).toContain('text/event-stream')
      const reader = res.body!.getReader()
      const first = await reader.read()
      expect(new TextDecoder().decode(first.value)).toContain('data: {"tick":1}')
      const second = await reader.read()
      expect(second.done).toBe(true)
      abortController.abort()
      expect(errorSpy).not.toHaveBeenCalled()
    } finally {
      errorSpy.mockRestore()
      await server.close()
    }
  })

  it('proxies /js/* routes via the JS route handler', async () => {
    const server = await withUpstreamServer((_req, res) => {
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/javascript')
      res.end('window.__RUM__=1;')
    })
    try {
      const req = new NextRequest('http://localhost:3002/js/beacon.js')
      const res = await jsGet(req)
      expect(res.status).toBe(200)
      expect(await res.text()).toBe('window.__RUM__=1;')
    } finally {
      await server.close()
    }
  })
})
