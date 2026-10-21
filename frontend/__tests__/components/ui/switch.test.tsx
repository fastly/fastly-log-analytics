import { render, screen } from '@testing-library/react'
import { expect, test } from 'vitest'

import { Switch } from '@/components/ui/switch'

// BaseUI >= 1.9 prefers aria-label over an associated <label>. A generic
// fallback label in the wrapper would therefore replace every real accessible
// name, so these pin that the wrapper adds none.
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
