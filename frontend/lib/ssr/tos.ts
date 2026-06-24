// Server-only SSR fetch for /api/share/tos. Trust topology + Caddy-drift
// defense live in ./_transport.ts. /api/share/tos doubles as an auth
// gate: 401 means the analyst session cookie is missing or invalid, and
// the caller should bounce to /share-login — preserved here by mapping
// status 401 to the literal 'unauthenticated' return value.

import { parseSsrJson, ssrUpstreamGet } from './_transport'

export type TosPayload = { version: string; text: string }
export type TosResult = TosPayload | 'unauthenticated' | null

export async function fetchTosServerSide(): Promise<TosResult> {
  // /api/share/tos doubles as an auth gate: 401 means the analyst session
  // cookie is missing/invalid → bounce to /share-login. Keep that branch
  // ahead of the shared 2xx/parse tail so the 'unauthenticated' literal is
  // preserved exactly.
  const resp = await ssrUpstreamGet({ path: '/api/share/tos', logPrefix: 'ssr/tos' })
  if (resp?.statusCode === 401) return 'unauthenticated'
  return parseSsrJson<TosPayload>(resp, 'ssr/tos')
}
