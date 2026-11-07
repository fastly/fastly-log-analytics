import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { CloudCostCalculator } from '@/components/CostCalculator/CloudCostCalculator'
import { DEFAULT_INPUTS, DEFAULT_UNITS, LIST_PRICE_RATES, type UnitCosts } from '@/lib/cloud-engine/usage/estimate'

describe('Cloud cost calculator', () => {
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
      measuredInputs: { requestsPerDay: true, edgeRequestsPerDay: true },
      measuredUnits: { secrets: true },
      rates: { ...LIST_PRICE_RATES, secret_rate_per_month: 7 },
    }} />)
    expect(screen.getByRole('spinbutton', { name: 'Edge requests per day' })).toHaveValue(0)
    expect(screen.getByText('0 reqs · 0 vCPU ms')).toBeInTheDocument()
    expect(screen.getAllByText('$21.00')).toHaveLength(2)
    expect(screen.queryByText(/ClickHouse/)).not.toBeInTheDocument()
  })

  it('labels missing measurements as reference assumptions and recalculates what-if traffic', () => {
    render(<CloudCostCalculator />)
    expect(screen.getByText(/Unavailable measurements retain/)).toBeInTheDocument()
    expect(screen.getAllByText('Reference')).toHaveLength(8)
    const total = screen.getByText('Total Estimated Monthly Cost').parentElement?.nextElementSibling
    const initial = total?.textContent
    fireEvent.change(screen.getByRole('spinbutton', { name: 'Edge requests per day' }), { target: { value: '0' } })
    expect(screen.getByText('What-if')).toBeInTheDocument()
    expect(total?.textContent).not.toBe(initial)
  })
})
