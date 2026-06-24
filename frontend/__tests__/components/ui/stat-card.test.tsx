import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Activity } from 'lucide-react'
import { StatCard } from '@/components/ui/stat-card'

describe('StatCard', () => {
  it('renders title, value, and sub', () => {
    render(<StatCard title="Requests" value="1,234" sub="last 24h" />)
    expect(screen.getByText('Requests')).toBeInTheDocument()
    expect(screen.getByText('1,234')).toBeInTheDocument()
    expect(screen.getByText('last 24h')).toBeInTheDocument()
  })

  it('renders the icon when provided', () => {
    const { container } = render(
      <StatCard title="Hits" value={42} sub="ok" icon={Activity} />
    )
    expect(container.querySelector('svg')).toBeInTheDocument()
  })

  it('shows a skeleton instead of the value when loading', () => {
    const { container } = render(
      <StatCard title="Loading" value="hidden-value" sub="..." loading />
    )
    expect(screen.queryByText('hidden-value')).not.toBeInTheDocument()
    expect(container.querySelector('[data-slot="skeleton"]')).toBeInTheDocument()
  })

  it('renders the tooltip trigger when tooltip prop is provided', () => {
    const { container } = render(
      <StatCard title="Tipped" value="10" sub="info" tooltip="more info" />
    )
    expect(container.querySelector('svg.lucide-circle-help, svg')).toBeInTheDocument()
  })
})
