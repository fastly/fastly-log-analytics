import { request as httpRequest, type IncomingMessage } from 'node:http'
import { request as httpsRequest } from 'node:https'

const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailers',
  'transfer-encoding',
  'upgrade',
])

const IDEMPOTENT_METHODS = new Set(['GET', 'HEAD', 'OPTIONS'])

export function apiProxyTarget(): string {
  return (
    process.env.API_PROXY_URL ||
    process.env.NEXT_PUBLIC_API_PROXY_URL ||
    'http://caddy:80'
  ).replace(/\/+$/, '')
}

function isBenignDisconnectError(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false
  const code = (err as NodeJS.ErrnoException).code
  const message = (err as Error).message || ''
  return (
    code === 'ECONNRESET' ||
    code === 'EPIPE' ||
    code === 'ERR_STREAM_PREMATURE_CLOSE' ||
    code === 'ABORT_ERR' ||
    message.includes('aborted') ||
    message.includes('socket hang up')
  )
}

function buildUpstreamHeaders(request: Request, targetUrl: URL, bodyBuffer?: Buffer): Record<string, string> {
  const out: Record<string, string> = {}
  request.headers.forEach((value, key) => {
    const lower = key.toLowerCase()
    if (HOP_BY_HOP_HEADERS.has(lower) || lower === 'host' || lower === 'content-length') {
      return
    }
    out[key] = value
  })
  out['host'] = targetUrl.host
  out['connection'] = 'close'
  if (bodyBuffer && bodyBuffer.byteLength > 0) {
    out['content-length'] = String(bodyBuffer.byteLength)
  }
  return out
}

function sendUpstreamOnce(
  targetUrl: URL,
  method: string,
  headers: Record<string, string>,
  bodyBuffer: Buffer | undefined,
  signal: AbortSignal | undefined,
): Promise<Response> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      resolve(new Response(null, { status: 499 }))
      return
    }

    const lib = targetUrl.protocol === 'https:' ? httpsRequest : httpRequest
    let settledHeaders = false

    const upstreamReq = lib(
      {
        hostname: targetUrl.hostname,
        port: targetUrl.port || (targetUrl.protocol === 'https:' ? 443 : 80),
        path: `${targetUrl.pathname}${targetUrl.search}`,
        method,
        headers,
        // Disable keep-alive socket pooling so Next.js never reuses a socket
        // right as Uvicorn's --timeout-keep-alive closes it on backend-svc:8000.
        agent: false,
      },
      (upstreamRes: IncomingMessage) => {
        settledHeaders = true
        const status = upstreamRes.statusCode || 502
        const resHeaders = new Headers()

        for (const [rawKey, rawVal] of Object.entries(upstreamRes.headers)) {
          if (rawVal === undefined) continue
          const lower = rawKey.toLowerCase()
          if (HOP_BY_HOP_HEADERS.has(lower)) continue
          if (lower === 'set-cookie') {
            const cookies = Array.isArray(rawVal) ? rawVal : [String(rawVal)]
            for (const c of cookies) {
              resHeaders.append('set-cookie', c)
            }
            continue
          }
          resHeaders.set(rawKey, Array.isArray(rawVal) ? rawVal.join(', ') : String(rawVal))
        }

        if (method === 'HEAD' || status === 204 || status === 205 || status === 304) {
          upstreamRes.resume()
          resolve(
            new Response(null, {
              status,
              statusText: upstreamRes.statusMessage,
              headers: resHeaders,
            }),
          )
          return
        }

        let streamClosed = false
        const closeCleanly = (controller: ReadableStreamDefaultController<Uint8Array>) => {
          if (streamClosed) return
          streamClosed = true
          try {
            controller.close()
          } catch {
            // Controller may already be closed/canceled by consumer.
          }
        }

        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            upstreamRes.on('data', (chunk: Buffer) => {
              if (streamClosed) return
              try {
                controller.enqueue(new Uint8Array(chunk))
              } catch {
                streamClosed = true
                upstreamReq.destroy()
              }
            })
            upstreamRes.on('end', () => {
              closeCleanly(controller)
            })
            upstreamRes.on('close', () => {
              closeCleanly(controller)
            })
            upstreamRes.on('error', (err) => {
              if (isBenignDisconnectError(err) || signal?.aborted) {
                closeCleanly(controller)
                return
              }
              if (!streamClosed) {
                streamClosed = true
                try {
                  controller.error(err)
                } catch {
                  // Ignore if already closed.
                }
              }
            })
          },
          cancel() {
            streamClosed = true
            upstreamRes.destroy()
            upstreamReq.destroy()
          },
        })

        resolve(
          new Response(stream, {
            status,
            statusText: upstreamRes.statusMessage,
            headers: resHeaders,
          }),
        )
      },
    )

    const onAbort = () => {
      upstreamReq.destroy()
      if (!settledHeaders) {
        settledHeaders = true
        resolve(new Response(null, { status: 499 }))
      }
    }
    signal?.addEventListener('abort', onAbort, { once: true })
    upstreamReq.on('close', () => {
      signal?.removeEventListener('abort', onAbort)
    })

    upstreamReq.on('error', (err) => {
      if (settledHeaders) {
        // Headers already sent to downstream stream; mid-stream ECONNRESET or
        // client abort is handled by the ReadableStream listeners above.
        return
      }
      settledHeaders = true
      if (signal?.aborted) {
        resolve(new Response(null, { status: 499 }))
        return
      }
      reject(err)
    })

    if (bodyBuffer && bodyBuffer.byteLength > 0) {
      upstreamReq.write(bodyBuffer)
    }
    upstreamReq.end()
  })
}

export async function proxyUpstreamRequest(request: Request): Promise<Response> {
  const reqUrl = new URL(request.url)
  const targetUrl = new URL(`${apiProxyTarget()}${reqUrl.pathname}${reqUrl.search}`)
  const method = (request.method || 'GET').toUpperCase()

  let bodyBuffer: Buffer | undefined
  if (method !== 'GET' && method !== 'HEAD') {
    const ab = await request.arrayBuffer()
    if (ab.byteLength > 0) {
      bodyBuffer = Buffer.from(ab)
    }
  }

  const headers = buildUpstreamHeaders(request, targetUrl, bodyBuffer)

  try {
    return await sendUpstreamOnce(targetUrl, method, headers, bodyBuffer, request.signal)
  } catch (firstErr) {
    const code = (firstErr as NodeJS.ErrnoException | undefined)?.code
    if (
      IDEMPOTENT_METHODS.has(method) &&
      !request.signal?.aborted &&
      (code === 'ECONNRESET' || code === 'EPIPE')
    ) {
      try {
        return await sendUpstreamOnce(targetUrl, method, headers, bodyBuffer, request.signal)
      } catch (retryErr) {
        if (isBenignDisconnectError(retryErr) && request.signal?.aborted) {
          return new Response(null, { status: 499 })
        }
        return Response.json(
          { detail: 'Upstream service unavailable' },
          { status: 502 },
        )
      }
    }
    if (isBenignDisconnectError(firstErr) && request.signal?.aborted) {
      return new Response(null, { status: 499 })
    }
    return Response.json(
      { detail: 'Upstream service unavailable' },
      { status: 502 },
    )
  }
}
