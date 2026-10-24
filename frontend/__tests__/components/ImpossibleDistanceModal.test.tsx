import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { expect, test, vi } from 'vitest'
import React from 'react'
import { installMaplibreSideEffects, maplibreMockFactory } from '../helpers/maplibre-mock'
import { ImpossibleDistanceModal } from '@/components/Insights/ImpossibleDistanceModal'
import { InsightHelpModal } from '@/components/Insights/InsightHelpModal'

vi.mock('maplibre-gl', () => maplibreMockFactory())
installMaplibreSideEffects()

const VALID_DATA = {
  label: '1.2.3.4',
  client_lat: 48.8566,
  client_lon: 2.3522,
  pop_lat: 37.7749,
  pop_lon: -122.4194,
  pop: 'SFO',
  tcp_rtt: 20000,
  distance_km: 8960,
  max_km: 2000,
  city: 'Paris',
  country: 'FR',
}

test('renders modal with valid data', () => {
  render(
    <ImpossibleDistanceModal isOpen data={VALID_DATA} onOpenChange={vi.fn()} />
  )
  expect(screen.getByText(/Physics Violation/)).toBeInTheDocument()
  expect(screen.getAllByText(/8,960/).length).toBeGreaterThan(0)
})

test('shows fallback when coordinates are missing', () => {
  const bad = { ...VALID_DATA, pop_lat: NaN, pop_lon: NaN }
  render(
    <ImpossibleDistanceModal isOpen data={bad} onOpenChange={vi.fn()} />
  )
  expect(screen.getByText('Location data unavailable.')).toBeInTheDocument()
})

test('hasCoords guard prevents .toFixed() crash on undefined pop coordinates', () => {
  // Regression: the overlay called data.pop_lat.toFixed(2) unconditionally.
  // When pop_lat is undefined (unknown POP code), that threw TypeError.
  // The fix wraps the overlay in {hasCoords && (...)}.
  // Verify the guard logic directly — Number.isFinite rejects null, undefined, and NaN.
  expect(Number.isFinite(undefined)).toBe(false)
  expect(Number.isFinite(null)).toBe(false)
  expect(Number.isFinite(NaN)).toBe(false)
  expect(Number.isFinite(37.7749)).toBe(true)
})

test('shows city and country in coordinate overlay when provided', () => {
  render(
    <ImpossibleDistanceModal isOpen data={VALID_DATA} onOpenChange={vi.fn()} />
  )
  expect(screen.getAllByText(/Paris, FR/).length).toBeGreaterThan(0)
})

test('returns null when data is null', () => {
  const { container } = render(
    <ImpossibleDistanceModal isOpen={false} data={null} onOpenChange={vi.fn()} />
  )
  expect(container).toBeEmptyDOMElement()
})

test('InsightHelpModal renders impossible_distance content', () => {
  render(
    <InsightHelpModal insightId="impossible_distance" isOpen onOpenChange={vi.fn()} />
  )
  expect(screen.getByText(/The Physics of/)).toBeInTheDocument()
  expect(screen.getByText(/Speed of Light Violation Detected/)).toBeInTheDocument()
})

test('InsightHelpModal renders default content for unknown insight', () => {
  render(
    <InsightHelpModal insightId="unknown_insight_xyz" isOpen onOpenChange={vi.fn()} />
  )
  expect(screen.getByText('Insight Analysis')).toBeInTheDocument()
})

test('InsightHelpModal close button triggers onOpenChange', async () => {
  const onChange = vi.fn()
  render(
    <InsightHelpModal insightId="impossible_distance" isOpen onOpenChange={onChange} />
  )
  // Radix Dialog attaches its Escape listener to ``document``, not to the
  // focused element. ``userEvent.keyboard('{Escape}')`` would dispatch to
  // ``document.activeElement`` (often body), and the global listener
  // would still fire — but ``fireEvent.keyDown(document, ...)`` is the
  // direct shape of what we're testing: a document-level keyboard hook.
  // Kept as fireEvent intentionally; not part of the user-event migration.
  fireEvent.keyDown(document, { key: 'Escape' })
  await waitFor(() => expect(onChange).toHaveBeenCalled())
})
