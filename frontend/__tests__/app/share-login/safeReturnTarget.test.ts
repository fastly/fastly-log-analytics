import { describe, expect, it, vi } from 'vitest'

// ShareLoginForm pulls next/navigation at module load; stub it so we can
// import the pure helper without a real router.
vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
  useSearchParams: () => new URLSearchParams(),
}))

import { safeReturnTarget } from '@/app/share-login/ShareLoginForm'

describe('safeReturnTarget (post-login open-redirect guard, finding 008)', () => {
  it('accepts in-app absolute paths', () => {
    expect(safeReturnTarget('/dashboard')).toBe('/dashboard')
    expect(safeReturnTarget('/sessions?x=1')).toBe('/sessions?x=1')
  })

  it('rejects protocol-relative, backslash, and absolute external targets', () => {
    expect(safeReturnTarget('//evil.com')).toBeNull()
    expect(safeReturnTarget('/\\evil.com')).toBeNull()
    expect(safeReturnTarget('https://evil.com')).toBeNull()
    expect(safeReturnTarget('evil.com')).toBeNull()
  })

  it('rejects whitespace-smuggled protocol-relative targets (the finding-008 bypass)', () => {
    // The browser strips embedded tabs/newlines/spaces when resolving a URL,
    // so each of these would resolve to //evil.com despite not literally
    // starting with "//" — the guard must normalize whitespace first.
    expect(safeReturnTarget('/\t/evil.com')).toBeNull()
    expect(safeReturnTarget('/\n/evil.com')).toBeNull()
    expect(safeReturnTarget('/ /evil.com')).toBeNull()
    expect(safeReturnTarget('/\t\\evil.com')).toBeNull()
  })

  it('returns null for empty / nullish input', () => {
    expect(safeReturnTarget(null)).toBeNull()
    expect(safeReturnTarget(undefined)).toBeNull()
    expect(safeReturnTarget('')).toBeNull()
  })
})
