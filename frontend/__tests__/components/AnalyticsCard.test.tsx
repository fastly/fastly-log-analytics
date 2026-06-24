import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test } from 'vitest'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import React from 'react'

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
  // The "Loading data..." label only renders in the loading overlay.
  expect(screen.getByText(/loading data/i)).toBeInTheDocument()
  // Loader2 from lucide-react renders as an <svg> with the .animate-spin class.
  const spinner = container.querySelector('svg.animate-spin')
  expect(spinner).toBeInTheDocument()
})

test('isFetching && !isLoading applies opacity-40 to child container', () => {
  const { container } = render(
    <AnalyticsCard title="Refetching Card" isFetching>
      <div data-testid="child">stale data</div>
    </AnalyticsCard>
  )
  // No loading overlay when only isFetching is set.
  expect(screen.queryByText(/loading data/i)).not.toBeInTheDocument()
  // The child wrapper div carries opacity-40 when refetching with old data.
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

  // Dialog body is not in the DOM until the button is clicked.
  expect(screen.queryByTestId('help-body')).not.toBeInTheDocument()

  await user.click(helpButton)

  // Dialog now open: role=dialog present and help body rendered.
  const dialog = await screen.findByRole('dialog')
  expect(dialog).toBeInTheDocument()
  expect(screen.getByTestId('help-body')).toBeInTheDocument()

  // Escape dismisses the dialog (Radix Dialog default behavior).
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
