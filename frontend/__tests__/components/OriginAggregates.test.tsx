/**
 * @vitest-environment jsdom
 */
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { Aggregates } from '@/app/origin/_sections/Aggregates'

afterEach(cleanup)

describe('origin KPI cards', () => {
  it('show loading, not undefined or zero values, before the summary query has started', () => {
    render(<Aggregates summary={{ data: undefined, isLoading: false, isFetching: false, error: null }} />)
    expect(screen.queryByText(/undefined/)).toBeNull()
    expect(screen.queryByText('0.00%')).toBeNull()
    expect(screen.getByText('Loading Origin TTFB (P50)...')).toBeTruthy()
  })
})
