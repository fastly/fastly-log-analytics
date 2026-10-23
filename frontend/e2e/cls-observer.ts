import type { Page } from '@playwright/test'

declare global {
  interface Window {
    __clsScore?: number
  }
}

interface LayoutShiftEntry extends PerformanceEntry {
  hadRecentInput?: boolean
  value?: number
}

/**
 * Pass to `page.addInitScript`. Playwright serialises the function body, so it
 * must stay self-contained (no references to module-level values).
 */
export function clsInitScript(): void {
  window.__clsScore = 0
  try {
    const obs = new PerformanceObserver((list) => {
      for (const entry of list.getEntries() as LayoutShiftEntry[]) {
        if (!entry.hadRecentInput && typeof entry.value === 'number') {
          window.__clsScore = (window.__clsScore ?? 0) + entry.value
        }
      }
    })
    obs.observe({ type: 'layout-shift', buffered: true })
  } catch {
    // layout-shift observer unsupported in this browser
  }
}

/**
 * Install the cumulative layout-shift observer onto `page` via `addInitScript`.
 */
export async function installClsObserver(page: Page): Promise<void> {
  await page.addInitScript(clsInitScript)
}

/**
 * Read the accumulated unexpected layout-shift score from `window.__clsScore`.
 */
export async function readClsScore(page: Page): Promise<number> {
  return page.evaluate(() => Number(window.__clsScore ?? 0))
}
