import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { CloudCostCalculator } from '@/components/CostCalculator/CloudCostCalculator'
import { DEFAULT_COMPACTION, DEFAULT_INPUTS, DEFAULT_UNITS, LIST_PRICE_RATES, type UnitCosts } from '@/lib/cloud-engine/usage/estimate'

describe('Cloud cost calculator', () => {
  it('lists each line\'s component costs under its total', () => {
    render(<CloudCostCalculator />)
    const delivery = screen.getByText('CDN delivery').closest('[data-estimate-line]') as HTMLElement
    expect(delivery).toHaveTextContent(/Egress\s*\$/)
    expect(delivery).toHaveTextContent(/Requests\s*\$/)
  })

  it('turns measured storage for the selected range into a month at its average size', () => {
    render(<CloudCostCalculator localStorageGbHours={24} localStoragePeriodHours={24} rates={{ storage_rate_per_gb_month: 2 }} />)
    expect(screen.getByText('Measured · this install · 1.00 GB-months')).toBeInTheDocument()
    const storage = screen.getByText('Object Storage').closest('[data-estimate-line]') as HTMLElement
    expect(storage).toHaveTextContent(/Storage\s*\$2\.00/)
    expect(storage).toHaveTextContent(/Class A\s*\$/)
    expect(storage).toHaveTextContent(/Class B\s*\$/)
  })

  it('falls back for a null rate without replacing an explicit zero rate', () => {
    const { rerender } = render(<CloudCostCalculator />)
    const total = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling
    const reference = total?.textContent
    rerender(<CloudCostCalculator rates={{ compute_request_rate_per_million: null }} />)
    expect(total?.textContent).toBe(reference)
    rerender(<CloudCostCalculator rates={{ compute_request_rate_per_million: 0 }} />)
    expect(total?.textContent).not.toBe(reference)
  })

  it('keeps observed zero traffic and zero operation units while applying configured secret rates', () => {
    const units = Object.fromEntries(Object.keys(DEFAULT_UNITS).map((key) => [key, 0])) as unknown as UnitCosts
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, requestsPerDay: 0, edgeRequestsPerDay: 0 },
      units: { ...units, secrets: 3 },
      compaction: { ...DEFAULT_COMPACTION, enabled: false },
      measuredInputs: { requestsPerDay: true, edgeRequestsPerDay: true },
      measuredUnits: { secrets: true },
      rates: { ...LIST_PRICE_RATES, secret_rate_per_month: 7 },
    }} />)
    expect(screen.getByRole('spinbutton', { name: 'Total requests per day' })).toHaveValue(0)
    expect(screen.getByText('0 reqs · 0 vCPU ms')).toBeInTheDocument()
    expect(screen.getAllByText('$21.00')).toHaveLength(2)
    expect(screen.queryByText(/ClickHouse/)).not.toBeInTheDocument()
  })

  it('labels missing measurements as reference assumptions and recalculates what-if traffic', () => {
    render(<CloudCostCalculator />)
    expect(screen.getByText(/Unavailable measurements retain/)).toBeInTheDocument()
    expect(screen.getByText(/meta\/cloud_dr_state\.json/)).toBeInTheDocument()
    expect(screen.getAllByText('Reference')).toHaveLength(5)
    const total = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling
    const initial = total?.textContent
    fireEvent.change(screen.getByRole('spinbutton', { name: 'Total requests per day' }), { target: { value: '0' } })
    expect(screen.getByText('What-if')).toBeInTheDocument()
    expect(total?.textContent).not.toBe(initial)
  })

  it('asks only for what a user knows and derives the rest', () => {
    render(<CloudCostCalculator />)
    const inputs = screen.getAllByRole('spinbutton').map((el) => el.getAttribute('aria-label'))
    expect(inputs).toEqual([
      'Total requests per day',
      'Log sample rate (%)',
      'Log period (seconds)',
      'Log line size (bytes, uncompressed)',
      'Log retention (days)',
    ])
    expect(screen.getByRole('switch', { name: 'Edge-only logging' })).toBeInTheDocument()
    expect(screen.queryByRole('switch', { name: 'Delete raw logs after ingest' })).not.toBeInTheDocument()
  })

  it('lets an edit replace measured storage with the modelled figure', () => {
    render(<CloudCostCalculator localStorageGbHours={24} localStoragePeriodHours={24} />)
    expect(screen.getByText(/Measured · this install/)).toBeInTheDocument()
    fireEvent.change(screen.getByRole('spinbutton', { name: 'Log retention (days)' }), { target: { value: '90' } })
    expect(screen.queryByText(/Measured · this install/)).not.toBeInTheDocument()
  })

  it('scales stored bytes with the uncompressed line size', () => {
    render(<CloudCostCalculator />)
    const storage = () => screen.getByText('Object Storage').closest('[data-estimate-line]')!.querySelector('li')!.textContent
    const field = screen.getByRole('spinbutton', { name: 'Log line size (bytes, uncompressed)' }) as HTMLInputElement
    const before = storage()
    fireEvent.change(field, { target: { value: String(Number(field.value) * 4) } })
    expect(storage()).not.toBe(before)
  })

  it('scales edge requests with total requests for edge-only logging', () => {
    render(<CloudCostCalculator prefillData={{ inputs: { ...DEFAULT_INPUTS, edgeOnly: true } }} />)
    const total = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling
    const before = total?.textContent
    fireEvent.change(screen.getByRole('spinbutton', { name: 'Total requests per day' }), {
      target: { value: String(DEFAULT_INPUTS.requestsPerDay * 10) },
    })
    expect(total?.textContent).not.toBe(before)
  })
  it('converts the line size with the install\'s measured compression ratio', () => {
    const { rerender } = render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, gzBytesPerLine: 100, rawBytesPerGzByte: 12 },
      measuredInputs: { gzBytesPerLine: true, rawBytesPerGzByte: true },
    }} />)
    const field = () => screen.getByRole('spinbutton', { name: 'Log line size (bytes, uncompressed)' })
    expect(field()).toHaveValue(1200)
    expect(field().parentElement).toHaveTextContent('Measured')
    rerender(<CloudCostCalculator key="unmeasured" prefillData={{
      inputs: { ...DEFAULT_INPUTS, gzBytesPerLine: 100 },
      measuredInputs: { gzBytesPerLine: true },
    }} />)
    expect(field().parentElement).toHaveTextContent('Reference')
  })
})
