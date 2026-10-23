import { expect, type APIRequestContext } from '@playwright/test'

/**
 * Extract the value of a named cookie from an HTTP response's `headersArray()`.
 */
export function setCookieValues(
  headers: { name: string; value: string }[],
  cookieName: string,
): string | null {
  for (const h of headers) {
    if (h.name.toLowerCase() === 'set-cookie' && h.value.startsWith(`${cookieName}=`)) {
      const val = h.value.split(';', 1)[0].slice(cookieName.length + 1)
      if (val) return val
    }
  }
  return null
}

export interface PasscodeInviteSpec {
  name?: string
  email?: string
  passcode?: string
  durationHours?: number
  serviceIds: string[]
}

export interface SeededPasscodeInvite {
  inviteId: string
  email: string
  passcode: string
}

/**
 * Seed a passcode-authenticated Analyst Path B invite via the admin API.
 */
export async function seedPasscodeAnalystInvite(
  request: APIRequestContext,
  spec: PasscodeInviteSpec,
): Promise<SeededPasscodeInvite> {
  const uniqueSuffix = Date.now()
  const email = spec.email ?? `e2e-analyst-${uniqueSuffix}@example.com`
  const passcode = spec.passcode ?? 'ocean-breeze-cabin-42'
  const createRes = await request.post('/api/admin/share/invites', {
    data: {
      name: spec.name ?? 'E2E Share Analyst',
      email,
      passcode,
      duration_hours: spec.durationHours ?? 2,
      service_ids: spec.serviceIds,
      auth_method: 'passcode',
    },
  })
  expect(createRes.ok(), await createRes.text()).toBeTruthy()
  const inviteData = (await createRes.json()) as { id: string }
  expect(inviteData.id).toBeTruthy()
  return {
    inviteId: inviteData.id,
    email,
    passcode,
  }
}

/**
 * Best-effort cleanup of an analyst invite and any active session.
 */
export async function revokeAnalystInvite(
  request: APIRequestContext,
  inviteId: string | null | undefined,
  options?: { logout?: boolean },
): Promise<void> {
  if (options?.logout) {
    await request.post('/api/share/logout').catch(() => {})
  }
  if (inviteId) {
    await request.delete(`/api/admin/share/invites/${inviteId}`).catch(() => {})
  }
}
