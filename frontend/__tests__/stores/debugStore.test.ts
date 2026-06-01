/**
 * @vitest-environment jsdom
 *
 * debugStore — gates the floating debug panel and the per-call API call log.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { useDebugStore } from '@/stores/debugStore'

beforeEach(() => {
  useDebugStore.setState({ enabled: false, apiCallsEnabled: false })
})

describe('debug toggles', () => {
  it('setEnabled flips independently of apiCallsEnabled', () => {
    useDebugStore.getState().setEnabled(true)
    expect(useDebugStore.getState().enabled).toBe(true)
    expect(useDebugStore.getState().apiCallsEnabled).toBe(false)
  })

  it('setApiCallsEnabled flips independently of enabled', () => {
    useDebugStore.getState().setApiCallsEnabled(true)
    expect(useDebugStore.getState().apiCallsEnabled).toBe(true)
    expect(useDebugStore.getState().enabled).toBe(false)
  })

  it('persists to fastly-debug-settings', () => {
    useDebugStore.getState().setEnabled(true)
    useDebugStore.getState().setApiCallsEnabled(true)
    const raw = window.localStorage.getItem('fastly-debug-settings')
    expect(raw).toBeTruthy()
    const parsed = JSON.parse(raw!)
    expect(parsed.state.enabled).toBe(true)
    expect(parsed.state.apiCallsEnabled).toBe(true)
  })
})
