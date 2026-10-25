import { describe, it, expect, test } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { axe } from 'vitest-axe'
import React from 'react'
import { Activity, Star } from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import {
  ReviewCard,
  ReviewHeader,
  ReviewContent,
  ReviewItem,
} from '@/components/ui/review-card'
import { SectionHeader } from '@/components/ui/section-header'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { StatCard } from '@/components/ui/stat-card'
import { Switch } from '@/components/ui/switch'

describe('ReviewCard', () => {
  it('renders children inside the card container', () => {
    render(
      <ReviewCard>
        <span data-testid="child">hello</span>
      </ReviewCard>
    )
    expect(screen.getByTestId('child')).toBeInTheDocument()
  })

  it('renders header with optional icon and content text', () => {
    const { container } = render(
      <ReviewCard>
        <ReviewHeader icon={Star}>Summary</ReviewHeader>
        <ReviewContent>body text</ReviewContent>
      </ReviewCard>
    )
    expect(screen.getByText('Summary')).toBeInTheDocument()
    expect(screen.getByText('body text')).toBeInTheDocument()
    expect(container.querySelector('svg')).toBeInTheDocument()
  })

  it('renders a default ReviewItem with label and value stacked', () => {
    render(<ReviewItem label="Service" value="cdn-prod" />)
    expect(screen.getByText('Service')).toBeInTheDocument()
    expect(screen.getByText('cdn-prod')).toBeInTheDocument()
  })

  it('renders the between variant with label and value side by side', () => {
    const { container } = render(
      <ReviewItem label="Status" value="active" variant="between" />
    )
    expect(container.firstChild).toHaveClass('justify-between')
    expect(screen.getByText('active')).toBeInTheDocument()
  })
})

describe('SectionHeader', () => {
  it('renders the title text', () => {
    render(<SectionHeader title="Overview" />)
    expect(screen.getByRole('heading', { name: /overview/i })).toBeInTheDocument()
  })

  it('renders the icon when provided', () => {
    const { container } = render(<SectionHeader title="With Icon" icon={Star} />)
    expect(container.querySelector('svg')).toBeInTheDocument()
  })

  it('renders inline children alongside the title', () => {
    render(
      <SectionHeader title="Details">
        <span data-testid="action-slot"> · extra</span>
      </SectionHeader>
    )
    expect(screen.getByTestId('action-slot')).toBeInTheDocument()
  })

  it('applies the optional className to the root element', () => {
    const { container } = render(
      <SectionHeader title="Styled" className="custom-class" />
    )
    expect(container.firstChild).toHaveClass('custom-class')
  })
})

describe('SkeletonGrid', () => {
  it('renders the configured number of skeleton blocks', () => {
    const { container } = render(<SkeletonGrid count={4} />)
    expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(4)
  })

  it('renders a custom count when provided', () => {
    const { container } = render(<SkeletonGrid count={7} />)
    expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(7)
  })

  it('renders nothing when count is 0', () => {
    const { container } = render(<SkeletonGrid count={0} />)
    expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(0)
  })

  it('applies the default height inline style', () => {
    const { container } = render(<SkeletonGrid count={1} />)
    const block = container.querySelector('[data-slot="skeleton"]') as HTMLElement
    expect(block.style.height).toBe('120px')
  })

  it('honors a custom height prop', () => {
    const { container } = render(<SkeletonGrid count={1} height="200px" />)
    const block = container.querySelector('[data-slot="skeleton"]') as HTMLElement
    expect(block.style.height).toBe('200px')
  })
})

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

describe('Switch', () => {
  test('takes its accessible name from an associated <label htmlFor>', () => {
    render(
      <>
        <label htmlFor="edge-only">Edge-only logging</label>
        <Switch id="edge-only" />
      </>,
    )
    expect(screen.getByRole('switch', { name: 'Edge-only logging' })).toBeInTheDocument()
  })

  test('an explicit aria-label wins', () => {
    render(<Switch aria-label="Background Sync" />)
    expect(screen.getByRole('switch', { name: 'Background Sync' })).toBeInTheDocument()
  })

  test('does not invent a generic name when the call site gives none', () => {
    render(<Switch />)
    expect(screen.queryByRole('switch', { name: 'Toggle' })).not.toBeInTheDocument()
  })
})

describe('AnalyticsCard', () => {
  test('renders AnalyticsCard with children', () => {
    render(
      <AnalyticsCard title="Test Title">
        <div data-testid="test-child">Child Content</div>
      </AnalyticsCard>
    )
    expect(screen.getByText('Test Title')).toBeInTheDocument()
    expect(screen.getByTestId('test-child')).toBeInTheDocument()
    expect(screen.getByText('Child Content')).toBeInTheDocument()
  })

  test('isLoading=true shows Loader2 spinner overlay', () => {
    const { container } = render(
      <AnalyticsCard title="Loading Card" isLoading>
        <div>placeholder</div>
      </AnalyticsCard>
    )
    expect(screen.getByText(/loading data/i)).toBeInTheDocument()
    const spinner = container.querySelector('svg.animate-spin')
    expect(spinner).toBeInTheDocument()
  })

  test('isFetching && !isLoading applies opacity-40 to child container', () => {
    const { container } = render(
      <AnalyticsCard title="Refetching Card" isFetching>
        <div data-testid="child">stale data</div>
      </AnalyticsCard>
    )
    expect(screen.queryByText(/loading data/i)).not.toBeInTheDocument()
    const dimmed = container.querySelector('.opacity-40')
    expect(dimmed).toBeInTheDocument()
    expect(dimmed).toContainElement(screen.getByTestId('child'))
  })

  test('icon prop renders icon node in the header', () => {
    render(
      <AnalyticsCard title="Iconed" icon={<span data-testid="card-icon">ICON</span>}>
        <div>body</div>
      </AnalyticsCard>
    )
    expect(screen.getByTestId('card-icon')).toBeInTheDocument()
  })

  test('headerAction prop renders action node in the header', () => {
    render(
      <AnalyticsCard
        title="With Action"
        headerAction={<button data-testid="header-action">Do Thing</button>}
      >
        <div>body</div>
      </AnalyticsCard>
    )
    expect(screen.getByTestId('header-action')).toBeInTheDocument()
  })

  test('helpContent shows HelpCircle button that opens HelpDialog on click', async () => {
    const user = userEvent.setup()
    render(
      <AnalyticsCard
        title="Helpful"
        helpContent={<p data-testid="help-body">Here is help</p>}
      >
        <div>body</div>
      </AnalyticsCard>
    )
    const helpButton = screen.getByRole('button', { name: /about this chart/i })
    expect(helpButton).toBeInTheDocument()
    expect(screen.queryByTestId('help-body')).not.toBeInTheDocument()

    await user.click(helpButton)

    const dialog = await screen.findByRole('dialog')
    expect(dialog).toBeInTheDocument()
    expect(screen.getByTestId('help-body')).toBeInTheDocument()

    await user.keyboard('{Escape}')
    expect(screen.queryByTestId('help-body')).not.toBeInTheDocument()
  })

  test('footer prop renders footer node beneath the content', () => {
    render(
      <AnalyticsCard title="With Footer" footer={<div data-testid="footer-node">Footer Here</div>}>
        <div>body</div>
      </AnalyticsCard>
    )
    expect(screen.getByTestId('footer-node')).toBeInTheDocument()
    expect(screen.getByText('Footer Here')).toBeInTheDocument()
  })

  test('description prop renders description text under the title', () => {
    render(
      <AnalyticsCard title="With Desc" description="A helpful description">
        <div>body</div>
      </AnalyticsCard>
    )
    expect(screen.getByText('A helpful description')).toBeInTheDocument()
  })

  test('default props render no spinner, no opacity dim, no help button', () => {
    const { container } = render(
      <AnalyticsCard title="Bare">
        <div>body</div>
      </AnalyticsCard>
    )
    expect(screen.queryByText(/loading data/i)).not.toBeInTheDocument()
    expect(container.querySelector('svg.animate-spin')).not.toBeInTheDocument()
    expect(container.querySelector('.opacity-40')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /about this chart/i })).not.toBeInTheDocument()
  })
})

describe('a11y: AnalyticsCard', () => {
  it('has no detectable WCAG violations in its happy-path render', async () => {
    const { container } = render(
      <AnalyticsCard title="Total requests" helpContent={<p>How we count.</p>}>
        <div>
          <p>1,234,567</p>
          <p>over the last hour</p>
        </div>
      </AnalyticsCard>
    )
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })

  it('has no violations while showing the loading overlay', async () => {
    const { container } = render(
      <AnalyticsCard title="Slow Card" isLoading isFetching>
        <p>placeholder</p>
      </AnalyticsCard>
    )
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })
})

describe('a11y: basic form patterns', () => {
  it('a labelled input has no violations', async () => {
    const { container } = render(
      <div>
        <label htmlFor="search">Search</label>
        <input id="search" type="search" placeholder="Type to filter…" />
      </div>
    )
    const results = await axe(container)
    expect(results).toHaveNoViolations()
  })
})
