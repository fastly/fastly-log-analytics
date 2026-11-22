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
