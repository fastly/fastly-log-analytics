import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { CloudCostCalculator } from '@/components/CostCalculator/CloudCostCalculator'
import { DEFAULT_COMPACTION, DEFAULT_INPUTS, DEFAULT_UNITS, LIST_PRICE_RATES, type UnitCosts } from '@/lib/cloud-engine/usage/estimate'

vi.mock('next/navigation', () => ({
  useRouter: () => ({ push: vi.fn() }),
}))

describe('Cloud cost calculator', () => {
  it('renders the unified layout matching High-Scale plus Cloud platform rows', () => {
    render(<CloudCostCalculator />)
    expect(screen.getByText('Your Traffic & Config')).toBeInTheDocument()
    expect(screen.getByText('Minimum Charge per Object')).toBeInTheDocument()
    expect(screen.getByText('Pricing (per 1,000 ops)')).toBeInTheDocument()
    expect(screen.getByText('Delivery Billing Zone')).toBeInTheDocument()
    expect(screen.getByText('What generates operations')).toBeInTheDocument()
    expect(screen.getByText('Monthly Estimate Breakdown')).toBeInTheDocument()
    expect(screen.getByText('Volume Estimates')).toBeInTheDocument()

    expect(screen.getByText('Object Storage')).toBeInTheDocument()
    const storage = screen.getByText('Object Storage').closest('[data-estimate-line]') as HTMLElement
    expect(storage).toHaveTextContent(/Storage\s*\$/)
    expect(storage).toHaveTextContent(/Class A\s*\$/)
    expect(storage).toHaveTextContent(/Class B\s*\$/)
    const delivery = screen.getByText('CDN delivery').closest('[data-estimate-line]') as HTMLElement
    expect(delivery).toHaveTextContent(/Egress\s*\$/)
    expect(delivery).toHaveTextContent(/Requests\s*\$/)
    expect(screen.getByText('Compute (app + worker)')).toBeInTheDocument()
    expect(screen.getByText('KV Store')).toBeInTheDocument()
    expect(screen.getByText('Secret Store')).toBeInTheDocument()
  })

  it('keeps storage modelled even when localStorageGbHours is passed', () => {
    const { rerender } = render(<CloudCostCalculator rates={{ storage_rate_per_gb_month: 2 }} />)
    const reference = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent
    rerender(<CloudCostCalculator localStorageGbHours={24} localStoragePeriodHours={24} rates={{ storage_rate_per_gb_month: 2 }} />)
    expect(screen.queryByText(/Measured · this install/)).not.toBeInTheDocument()
    expect(screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling?.textContent).toBe(reference)
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

  it('uses live request volume and configured rates while keeping expected canonical unit costs', () => {
    const noisyUnits = Object.fromEntries(Object.keys(DEFAULT_UNITS).map((key) => [key, 999_999])) as unknown as UnitCosts
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, requestsPerDay: 250_000, edgeRequestsPerDay: 200_000 },
      units: { ...noisyUnits, deliveryGbPerDay: 500 },
      compaction: { ...DEFAULT_COMPACTION, enabled: false },
      measuredInputs: { requestsPerDay: true, edgeRequestsPerDay: true },
      measuredUnits: { deliveryGbPerDay: true },
      rates: { ...LIST_PRICE_RATES, secret_rate_per_month: 7 },
    }} />)
    expect(screen.getByRole('spinbutton', { name: 'Total requests per day' })).toHaveValue(250_000)
    expect(screen.getByText(/Pre-filled with this service's live request volume/)).toBeInTheDocument()
    const delivery = screen.getByText('CDN delivery').closest('[data-estimate-line]') as HTMLElement
    expect(delivery).not.toHaveTextContent(/15,0\d\d GB/)
    expect(screen.getByText('$7.00')).toBeInTheDocument()
    expect(screen.queryByText(/ClickHouse/)).not.toBeInTheDocument()
  })

  it('updates egress cost when switching Delivery Billing Zone', () => {
    render(<CloudCostCalculator />)
    const delivery = screen.getByText('CDN delivery').closest('[data-estimate-line]') as HTMLElement
    const before = delivery.textContent
    fireEvent.click(screen.getByRole('button', { name: 'Global' }))
    expect(delivery.textContent).not.toBe(before)
  })

  it('ignores measured line size and compression overrides in favor of canonical defaults', () => {
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, gzBytesPerLine: 100, rawBytesPerGzByte: 12 },
      measuredInputs: { gzBytesPerLine: true, rawBytesPerGzByte: true },
    }} />)
    const field = screen.getByRole('spinbutton', { name: 'Average bytes per log line' })
    expect(field).toHaveValue(Math.round(DEFAULT_INPUTS.gzBytesPerLine * DEFAULT_INPUTS.rawBytesPerGzByte))
  })

  it('scales stored bytes with the uncompressed line size', () => {
    render(<CloudCostCalculator />)
    const storage = () => screen.getByText('Object Storage').closest('[data-estimate-line]')!.querySelector('li')!.textContent
    const field = screen.getByRole('spinbutton', { name: 'Average bytes per log line' }) as HTMLInputElement
    const before = storage()
    fireEvent.change(field, { target: { value: String(Number(field.value) * 4) } })
    expect(storage()).not.toBe(before)
  })

  it('uses configured-fields estimatedBytesPerLine and scales storage accordingly', () => {
    const { rerender } = render(<CloudCostCalculator />)
    const defaultField = screen.getByRole('spinbutton', { name: 'Average bytes per log line' })
    expect(defaultField).toHaveValue(1320)
    const defaultStorage = screen.getByText('Object Storage').closest('[data-estimate-line]')!.textContent

    rerender(<CloudCostCalculator estimatedBytesPerLine={2640} />)
    expect(screen.getByRole('spinbutton', { name: 'Average bytes per log line' })).toHaveValue(2640)
    expect(screen.getByText('Object Storage').closest('[data-estimate-line]')!.textContent).not.toBe(defaultStorage)
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
})
