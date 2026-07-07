/**
 * Analyst OAuth/OIDC login — end-to-end against the in-process mock IdP.
 *
 * The browser DOM click-through can't run in this harness: Playwright is on
 * 127.0.0.1, which the backend classifies as admin/loopback, so /share-login
 * redirects to /dashboard and never renders the analyst SSO UI (the same
 * limitation that defers the passcode happy-path browser test — see
 * analyst-share-login.spec.ts). The SSO UI itself is covered by vitest
 * (ShareLoginForm.oauth.test.tsx).
 *
 * What we CAN — and do — assert here is the FULL handshake through the real
 * running stack + mock IdP over HTTP: discovery, /authorize → mock IdP →
 * /callback, joserfc id_token verification, invite lookup, and the analyst
 * session cookie. The oauth_flow_state cookie is threaded manually (the request
 * jar drops Secure cookies over http, mirroring the pytest route tests).
 */
import { expect, test } from '@playwright/test'
import { E2E_BACKEND_PORT } from '../playwright.config'

const MOCK_IDP = `http://127.0.0.1:${E2E_BACKEND_PORT}/mock-idp`
const E2E_EMAIL = 'e2e-analyst@example.com'

function setCookieValues(headers: { name: string; value: string }[], cookieName: string): string | null {
  for (const h of headers) {
    if (h.name.toLowerCase() === 'set-cookie' && h.value.startsWith(`${cookieName}=`)) {
      const val = h.value.split(';', 1)[0].slice(cookieName.length + 1)
      if (val) return val
    }
  }
  return null
}

test.describe('analyst OAuth login', () => {
  test('mock IdP discovery + auth-config expose the configured provider', async ({ request }) => {
    const disco = await request.get(`${MOCK_IDP}/.well-known/openid-configuration`)
    expect(disco.ok()).toBeTruthy()
    expect((await disco.json()).issuer).toBe(MOCK_IDP)

    const cfg = await request.get('/api/share/auth-config')
    expect(cfg.ok()).toBeTruthy()
    const body = (await cfg.json()) as { passcode_enabled: boolean; providers: Array<{ id: string; display_name: string }> }
    expect(body.providers.map((p) => p.id)).toContain('google')
  })

  test('/authorize seals a flow-state cookie and redirects to the IdP with the OIDC params', async ({ request }) => {
    const r = await request.get('/api/share/oauth/authorize?provider=google', { maxRedirects: 0 })
    expect(r.status()).toBe(302)
    const loc = r.headers()['location']
    expect(loc).toContain(`${MOCK_IDP}/authorize`)
    expect(loc).toContain('prompt=select_account')
    expect(loc).toContain('code_challenge_method=S256')
    expect(loc).toContain('state=')
    expect(loc).toContain('nonce=')
    expect(setCookieValues(r.headersArray(), 'oauth_flow_state')).toBeTruthy()
  })

  test('full handshake: authorize → mock IdP → callback issues an analyst session', async ({ request }) => {
    // Seed an OAuth invite for the mock IdP's fixed identity (admin API, loopback).
    const created = await request.post('/api/admin/share/invites', {
      data: {
        name: 'E2E Analyst',
        email: E2E_EMAIL,
        auth_method: 'oauth',
        oauth_provider: 'google',
        service_ids: ['svc-playwright-e2e'],
        duration_hours: 24,
      },
    })
    expect(created.ok(), await created.text()).toBeTruthy()

    // 1. /authorize → capture the sealed flow-state + the IdP authorize URL.
    const auth = await request.get('/api/share/oauth/authorize?provider=google', { maxRedirects: 0 })
    expect(auth.status()).toBe(302)
    const flowState = setCookieValues(auth.headersArray(), 'oauth_flow_state')
    expect(flowState).toBeTruthy()
    const idpUrl = auth.headers()['location']

    // 2. Mock IdP auto-approves → 302 back to the callback with code+state.
    const idp = await request.get(idpUrl, { maxRedirects: 0 })
    expect(idp.status()).toBe(302)
    const callbackUrl = idp.headers()['location']
    expect(callbackUrl).toContain('/api/share/oauth/callback')
    expect(callbackUrl).toContain('code=')

    // 3. Callback (flow-state cookie threaded manually) completes the handshake.
    const cb = await request.get(callbackUrl, {
      maxRedirects: 0,
      headers: { cookie: `oauth_flow_state=${flowState}` },
    })
    expect(cb.status()).toBe(302)
    // Fresh invite → TOS not yet accepted → pending session routed to acknowledge
    // (TOS-after-OAuth), with the pending analyst cookie set. NOT an error redirect.
    expect(cb.headers()['location']).toBe('/share-login/acknowledge')
    expect(cb.headers()['location']).not.toContain('oauth_error')
    expect(setCookieValues(cb.headersArray(), 'analyst_pending_session_id')).toBeTruthy()
  })

  test('callback with no flow-state cookie fails closed to the login error banner', async ({ request }) => {
    const cb = await request.get('/api/share/oauth/callback?code=x&state=y', { maxRedirects: 0 })
    expect(cb.status()).toBe(302)
    expect(cb.headers()['location']).toBe('/share-login?oauth_error=auth_failed')
    expect(setCookieValues(cb.headersArray(), 'analyst_session_id')).toBeNull()
  })
})
