/**
 * @vitest-environment jsdom
 */
import { render, screen, cleanup } from '@testing-library/react'
import { describe, it, expect, afterEach } from 'vitest'
import { UpdatingBadge } from '@/components/UpdatingBadge'

afterEach(() => cleanup())

describe('UpdatingBadge', () => {
  it('renders the "Updating" label text', () => {
    render(<UpdatingBadge />)
    expect(screen.getByText(/updating/i)).toBeInTheDocument()
  })

  it('uses the pulse animation class so it reads as in-flight', () => {
    const { container } = render(<UpdatingBadge />)
    const root = container.firstChild as HTMLElement | null
    expect(root).not.toBeNull()
    expect(root!.className).toMatch(/animate-pulse/)
  })

  it('renders a single root element (no wrapper noise)', () => {
    const { container } = render(<UpdatingBadge />)
    expect(container.children).toHaveLength(1)
  })
})
