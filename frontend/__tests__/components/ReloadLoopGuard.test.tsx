import { describe, expect, test, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import React from 'react'

import { ReloadLoopGuard } from '@/components/ReloadLoopGuard'

const CHILD = 'app-subtree-rendered'

// Each mount of the guard simulates one hard document load (the effect records
// one entry per mount). sessionStorage persists across mounts within a test —
// exactly how it behaves across real reloads in one tab.
function load() {
  return render(
    <ReloadLoopGuard>
      <div>{CHILD}</div>
    </ReloadLoopGuard>,
  )
}

describe('ReloadLoopGuard', () => {
  beforeEach(() => {
    sessionStorage.clear()
  })

  test('renders the app subtree under the reload threshold', () => {
    for (let i = 0; i < 5; i++) {
      const { unmount } = load()
      expect(screen.getByText(CHILD)).toBeInTheDocument()
      unmount()
    }
  })

  test('trips after 6 same-path loads in the window — shows recovery prompt, hides app', () => {
    let last: ReturnType<typeof load> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = load()
    }
    expect(screen.queryByText(CHILD)).not.toBeInTheDocument()
    expect(screen.getByText(/kept reloading/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /refresh now/i })).toBeInTheDocument()
  })

  test('"Continue anyway" dismisses the prompt and renders the app', () => {
    let last: ReturnType<typeof load> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = load()
    }
    expect(screen.getByText(/kept reloading/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /continue anyway/i }))
    expect(screen.getByText(CHILD)).toBeInTheDocument()
    expect(sessionStorage.getItem('__reload_loop_history')).toBeNull()
  })

  test('"Refresh now" clears history and reloads', () => {
    const reloadSpy = vi.spyOn(window.location, 'reload').mockImplementation(() => {})
    let last: ReturnType<typeof load> | null = null
    for (let i = 0; i < 6; i++) {
      last?.unmount()
      last = load()
    }
    fireEvent.click(screen.getByRole('button', { name: /refresh now/i }))
    expect(sessionStorage.getItem('__reload_loop_history')).toBeNull()
    expect(reloadSpy).toHaveBeenCalledTimes(1)
    reloadSpy.mockRestore()
  })

  test('does not trip when loads are spread across different paths', () => {
    const spy = vi.spyOn(window.location, 'pathname', 'get')
    let last: ReturnType<typeof load> | null = null
    for (let i = 0; i < 8; i++) {
      spy.mockReturnValue(`/page-${i}`)
      last?.unmount()
      last = load()
      expect(screen.getByText(CHILD)).toBeInTheDocument()
    }
    spy.mockRestore()
  })
})
