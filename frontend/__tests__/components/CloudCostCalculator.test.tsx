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
    expect(screen.getByText('Pricing')).toBeInTheDocument()
    expect(screen.queryByText('Pricing (per 1,000 ops)')).not.toBeInTheDocument()
    expect(screen.getByText('Delivery Billing Zone')).toBeInTheDocument()
    expect(screen.getByText('What generates operations')).toBeInTheDocument()
    expect(screen.getByText('Monthly Estimate Breakdown')).toBeInTheDocument()
    expect(screen.getByText('Volume Estimates')).toBeInTheDocument()
    // KV Store Class A is displayed per 100,000 ops ($0.65) and Class B per 1M ops ($0.55), matching fastly.com/pricing.
    expect(screen.getByText('KV Store Class A ($/100,000 ops)').closest('div')!.parentElement).toHaveTextContent('0.65')
    expect(screen.getByText('KV Store Class B ($/1M ops)').closest('div')!.parentElement).toHaveTextContent('0.55')

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
    const prefillData = {
      inputs: {
        ...DEFAULT_INPUTS,
        logPeriodS: 1,
        filesPerPeriod: 109,
        requestsPerDay: 500_000_000,
        edgeRequestsPerDay: 500_000_000,
      },
      measuredInputs: { filesPerPeriod: true as const },
    }
    const { rerender } = render(<CloudCostCalculator prefillData={prefillData} />)
    const total = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling
    const reference = total?.textContent
    rerender(<CloudCostCalculator prefillData={prefillData} rates={{ compute_request_rate_per_million: null }} />)
    expect(total?.textContent).toBe(reference)
    rerender(<CloudCostCalculator prefillData={prefillData} rates={{ compute_request_rate_per_million: 0 }} />)
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
    const secretsRow = screen.getByText('Secret Store').closest('[data-estimate-line]') as HTMLElement
    expect(secretsRow).toHaveTextContent('1 secrets')
    expect(secretsRow).not.toHaveTextContent('$7.00')
    expect(screen.queryByText(/ClickHouse/)).not.toBeInTheDocument()
  })

  it('updates egress cost when switching Delivery Billing Zone', () => {
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, edgeOnly: true, requestsPerDay: 50_000_000, edgeRequestsPerDay: 50_000_000 },
    }} />)
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

  it('hides log nodes / fan-out from the UI while still auto-deriving expected node fan-out from traffic', () => {
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, edgeOnly: true, requestsPerDay: 200_000, edgeRequestsPerDay: 200_000 },
    }} />)
    expect(screen.queryByText('Log nodes / fan-out')).not.toBeInTheDocument()
    const total = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling
    const before = total?.textContent
    fireEvent.change(screen.getByRole('spinbutton', { name: 'Total requests per day' }), { target: { value: '800000' } })
    expect(total?.textContent).not.toBe(before)
  })

  it('exposes Auto-delete raw .zst logs after ingest matching High-Scale and updates storage when toggled', () => {
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, retentionDays: 90, deleteAfter: true },
    }} />)
    const toggle = screen.getByRole('switch', { name: 'Auto-delete raw .zst logs after ingest' })
    expect(toggle).toHaveAttribute('aria-checked', 'true')
    const storage = () => screen.getByText('Object Storage').closest('[data-estimate-line]')!.textContent
    const before = storage()
    fireEvent.click(toggle)
    expect(toggle).toHaveAttribute('aria-checked', 'false')
    expect(storage()).not.toBe(before)
  })

  it('auto-derives Delivery Billing Zone from fosRegion when rate is untouched default', () => {
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, edgeOnly: true, requestsPerDay: 200_000, edgeRequestsPerDay: 200_000 },
      fosRegion: 'us-east-1',
    }} />)
    expect(screen.getByText('(auto: us-east-1)')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /MBZ100/ })).toHaveAttribute('aria-pressed', 'true')
  })

  it('auto-derives MBZ200 for APAC fosRegion', () => {
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, edgeOnly: true, requestsPerDay: 200_000, edgeRequestsPerDay: 200_000 },
      fosRegion: 'jp-central-1',
    }} />)
    expect(screen.getByText('(auto: jp-central-1)')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /MBZ200/ })).toHaveAttribute('aria-pressed', 'true')
  })

  it('respects explicit Admin custom egress rate over fosRegion auto-derivation', () => {
    render(<CloudCostCalculator prefillData={{
      inputs: { ...DEFAULT_INPUTS, edgeOnly: true, requestsPerDay: 200_000, edgeRequestsPerDay: 200_000 },
      rates: { ...LIST_PRICE_RATES, cdn_egress_rate_per_gb: 0.18 },
      fosRegion: 'us-east-1',
    }} />)
    expect(screen.queryByText('(auto: us-east-1)')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Global/ })).toHaveAttribute('aria-pressed', 'true')
  })
})
