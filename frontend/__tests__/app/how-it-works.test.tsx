/**
 * @vitest-environment jsdom
 */
import { existsSync } from 'node:fs'
import path from 'node:path'
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, test, vi } from 'vitest'
import HowItWorksPage from '@/app/how-it-works/page'

const mode = vi.hoisted(() => ({ cloud: false }))
vi.mock('@/lib/cloud-engine/mode', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/cloud-engine/mode')>()),
  get IS_CLOUD_MODE() { return mode.cloud },
}))

const APP_DIR = path.resolve(__dirname, '../../app')

function routeExists(href: string): boolean {
  const pathname = href.split(/[?#]/)[0].replace(/\/$/, '')
  return existsSync(path.join(APP_DIR, pathname, 'page.tsx'))
}

afterEach(cleanup)

describe.each([
  { name: 'Cloud', cloud: true, deployment: /Fastly Compute/ },
  { name: 'High-Scale', cloud: false, deployment: /Kubernetes/ },
])('/how-it-works in $name mode', ({ cloud, deployment }) => {
  test('renders one "How it works" heading naming the deployment', () => {
    mode.cloud = cloud
    const { container } = render(<HowItWorksPage />)
    const h1s = screen.getAllByRole('heading', { level: 1 })
    expect(h1s).toHaveLength(1)
    expect(h1s[0]).toHaveTextContent(/^How it works$/)
    expect(container.textContent).toMatch(deployment)
  })

  test('every internal link points at an existing route', () => {
    mode.cloud = cloud
    const { container } = render(<HowItWorksPage />)
    const internal = [...container.querySelectorAll('a[href^="/"]')].map(a => a.getAttribute('href') ?? '')
    expect(internal.filter(href => !routeExists(href))).toEqual([])
  })
})

describe('/how-it-works factual accuracy', () => {
  test('Cloud mode describes the 30-second health-check cadence and 6-hour inventory snapshots', () => {
    mode.cloud = true
    const { container } = render(<HowItWorksPage />)
    const text = container.textContent ?? ''
    expect(text).not.toMatch(/once a second/i)
    expect(text).not.toMatch(/every second/i)
    expect(text).toMatch(/30 seconds/)
    expect(text).toMatch(/six-hour inventory snapshot/i)
  })

  test('High-Scale mode describes service-local GB-hours billing from PostgreSQL ledgers', () => {
    mode.cloud = false
    const { container } = render(<HowItWorksPage />)
    const text = container.textContent ?? ''
    expect(text).toMatch(/service-local billed GB-hours/i)
    expect(text).toMatch(/PostgreSQL/)
  })
})
