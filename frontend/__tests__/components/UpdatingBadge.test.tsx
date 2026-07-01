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

  it('pulses the dot (not the pill) so the label keeps full-opacity AA contrast', () => {
    // animate-pulse on the whole pill drops the text to ~0.5 opacity
    // mid-animation, which blends text-foreground into the primary/10 tint
    // below the 4.5:1 WCAG AA threshold (axe color-contrast). The pulse must
    // live on the decorative dot only; the label stays at full opacity.
    const { container } = render(<UpdatingBadge />)
    const root = container.firstChild as HTMLElement | null
    expect(root).not.toBeNull()
    expect(root!.className).not.toMatch(/animate-pulse/)
    const dot = root!.querySelector('span')
    expect(dot).not.toBeNull()
    expect(dot!.className).toMatch(/animate-pulse/)
  })

  it('renders a single root element (no wrapper noise)', () => {
    const { container } = render(<UpdatingBadge />)
    expect(container.children).toHaveLength(1)
  })
})
