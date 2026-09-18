import { test, expect } from '@playwright/test'
import * as fs from 'fs'
import * as path from 'path'

test.describe('Performance Harness', () => {
  const urls = [
    '/admin', '/admin/queries', '/admin/queue', '/admin/rum',
    '/admin/session-scoring', '/admin/share', '/admin/trends', '/admin/usage-log',
    '/dashboard', '/insights', '/network', '/origin', '/performance', '/security', '/sessions'
  ]

  for (const urlPath of urls) {
    test(`Load time for ${urlPath}`, async ({ page }) => {
      const harDir = path.join(process.cwd(), 'perf-results')
      if (!fs.existsSync(harDir)) fs.mkdirSync(harDir, { recursive: true })

      const safePath = urlPath === '/' ? 'root' : urlPath.replace(/\//g, '_').substring(1)
      await page.routeFromHAR(path.join(harDir, `har-${safePath}.har`), { update: true })

      const start = Date.now()
      await page.goto(urlPath, { waitUntil: 'networkidle' })
      const end = Date.now()

      const lcp = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          try {
            new PerformanceObserver((list) => {
              const entries = list.getEntries()
              const lastEntry = entries[entries.length - 1]
              resolve(lastEntry.startTime)
            }).observe({ type: 'largest-contentful-paint', buffered: true })
            setTimeout(() => resolve(0), 1000)
          } catch(e) {
            resolve(0)
          }
        })
      })

      console.log(`[PERF] ${urlPath} | Total: ${end - start}ms | LCP: ${Math.round(lcp)}ms`)
      expect(end - start).toBeLessThan(15000)
    })
  }
})
