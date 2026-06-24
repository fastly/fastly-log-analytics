/**
 * @vitest-environment jsdom
 */
import { render, screen, cleanup } from '@testing-library/react'
import { describe, it, expect, afterEach } from 'vitest'
import { Server } from 'lucide-react'
import { NoServiceSelected } from '@/components/NoServiceSelected'

afterEach(() => cleanup())

describe('NoServiceSelected', () => {
  it('renders the provided message and default title', () => {
    render(<NoServiceSelected icon={Server} message="Pick one from the picker above" />)
    expect(screen.getByRole('heading', { name: 'No Service Selected' })).toBeInTheDocument()
    expect(screen.getByText('Pick one from the picker above')).toBeInTheDocument()
  })

  it('respects an explicit title override', () => {
    render(<NoServiceSelected icon={Server} message="msg" title="Nothing here yet" />)
    expect(screen.getByRole('heading', { name: 'Nothing here yet' })).toBeInTheDocument()
  })

  it('renders the lucide icon as an svg', () => {
    const { container } = render(<NoServiceSelected icon={Server} message="msg" />)
    const svg = container.querySelector('svg')
    expect(svg).not.toBeNull()
    expect(svg?.classList.contains('text-muted-foreground')).toBe(true)
  })
})
