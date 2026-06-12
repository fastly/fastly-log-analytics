import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import React from 'react'

test('renders AnalyticsCard with children', () => {
  const { debug } = render(
    <AnalyticsCard title="Test Title">
      <div data-testid="test-child">Child Content</div>
    </AnalyticsCard>
  )

  // debug() // Use this if still failing
  expect(screen.getByText('Test Title')).toBeInTheDocument()
  expect(screen.getByTestId('test-child')).toBeInTheDocument()
  expect(screen.getByText('Child Content')).toBeInTheDocument()
})
