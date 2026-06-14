// Server-only SSR fetch for /api/share/tos. Follows the same node:http +
// Host-preserving topology as lib/ssr/bootstrap.ts — see that file for the
// full trust-topology rationale. /api/share/tos doubles as an auth gate:
// 401 means the analyst session cookie is missing or invalid, and the
// caller should bounce to /share-login.
import { request as httpRequest } from 'node:http'
import { request as httpsRequest } from 'node:https'

import { cookies, headers } from 'next/headers'

export type TosPayload = { version: string; text: string }
export type TosResult = TosPayload | 'unauthenticated' | null

const TIMEOUT_MS = 5000

interface RawResponse {
  statusCode: number
  body: string
}

function rawRequest(
  urlStr: string,
  reqHeaders: Record<string, string>,
  timeoutMs: number,
): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr)
    const lib = url.protocol === 'https:' ? httpsRequest : httpRequest
    const req = lib(
      {
        hostname: url.hostname,
        port: url.port || (url.protocol === 'https:' ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method: 'GET',
        headers: reqHeaders,
        timeout: timeoutMs,
      },
      (res) => {
        const chunks: Buffer[] = []
        res.on('data', (c: Buffer) => chunks.push(c))
        res.on('end', () =>
          resolve({ statusCode: res.statusCode ?? 0, body: Buffer.concat(chunks).toString('utf8') }),
        )
      },
    )
    req.on('error', reject)
    req.on('timeout', () => {
      req.destroy(new Error(`SSR upstream timeout after ${timeoutMs}ms`))
    })
    req.end()
  })
}

export async function fetchTosServerSide(): Promise<TosResult> {
  const base = process.env.API_PROXY_URL
  if (!base) return null

  try {
    const [cookieJar, headerBag] = await Promise.all([cookies(), headers()])
    const cookieHeader = cookieJar.toString()
    const proxiedByCaddy = headerBag.get('x-proxied-by-caddy')
    const inboundHost = headerBag.get('host')

    const upstreamHeaders: Record<string, string> = {
      Accept: 'application/json',
    }
    if (cookieHeader) upstreamHeaders.Cookie = cookieHeader
    if (proxiedByCaddy) {
      upstreamHeaders['X-Remote-Analyst'] = '1'
      if (inboundHost) upstreamHeaders.Host = inboundHost
    }

    const { statusCode, body } = await rawRequest(`${base}/api/share/tos`, upstreamHeaders, TIMEOUT_MS)
    if (statusCode === 401) return 'unauthenticated'
    if (statusCode < 200 || statusCode >= 300) {
      console.warn(`[ssr/tos] upstream returned ${statusCode}; falling back to client fetch`)
      return null
    }
    return JSON.parse(body) as TosPayload
  } catch (err) {
    console.warn('[ssr/tos] fetch failed; falling back to client fetch:', err)
    return null
  }
}
