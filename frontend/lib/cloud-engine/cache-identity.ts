// Cloud bootstrap payloads carry the signed-in user, which the shared response type omits.
export function cloudCacheIdentity(bootstrap: unknown): string {
  const payload = bootstrap as
    | {
        user?: { id?: string | null; role?: string | null } | null
        settings?: { needs_login?: boolean | null; mask_ips?: boolean | null } | null
      }
    | null
    | undefined
  if (payload?.settings?.needs_login === true) return 'anonymous'
  const user = payload?.user
  if (!user?.id) return 'anonymous'
  const rawRole = user.role && user.role.trim().length > 0 ? user.role.trim() : 'admin'
  const normalizedRole = rawRole === 'analyst_masked' ? 'analyst-no-pii' : rawRole
  const role =
    payload?.settings?.mask_ips === true && !normalizedRole.endsWith('-no-pii')
      ? `${normalizedRole}-no-pii`
      : normalizedRole
  return `${user.id}:${role}`
}
