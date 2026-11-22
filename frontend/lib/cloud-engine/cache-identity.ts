// Cloud bootstrap payloads carry the signed-in user, which the shared response type omits.
export function cloudCacheIdentity(bootstrap: unknown): string {
  const user = (bootstrap as { user?: { id?: string | null; role?: string | null } | null } | null | undefined)?.user
  return user?.id ? `${user.id}:${user.role ?? 'admin'}` : 'anonymous'
}
