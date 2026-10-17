import { describe, expect, it } from 'vitest'

import { statusBadgeClass } from '@/lib/status-style'

describe('statusBadgeClass', () => {
  it.each([
    [200, 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'],
    [302, 'bg-sky-500/15 text-sky-700 dark:text-sky-400 border-sky-500/20'],
    [404, 'bg-amber-500/15 text-amber-700 dark:text-amber-400 border-amber-500/20'],
    [503, 'bg-destructive/15 text-destructive border-destructive/20 font-bold'],
    ['200', 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'],
    [null, 'bg-muted text-muted-foreground'],
    ['x', 'bg-muted text-muted-foreground'],
  ] as const)('default variant %s', (status, want) => {
    expect(statusBadgeClass(status)).toBe(want)
  })

  // Outputs pinned from the CacheCollapseModal's former local palette.
  it.each([
    [200, 'bg-green-500/10 text-green-500 border-green-500/20'],
    [299, 'bg-green-500/10 text-green-500 border-green-500/20'],
    [301, 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20'],
    [404, 'bg-red-500/10 text-red-500 border-red-500/20'],
    [503, 'bg-red-500/10 text-red-500 border-red-500/20'],
    [100, 'bg-red-500/10 text-red-500 border-red-500/20'],
    [0, 'bg-muted text-muted-foreground'],
    [null, 'bg-muted text-muted-foreground'],
    [undefined, 'bg-muted text-muted-foreground'],
  ] as const)('traffic-light variant %s', (status, want) => {
    expect(statusBadgeClass(status, 'traffic-light')).toBe(want)
  })
})
