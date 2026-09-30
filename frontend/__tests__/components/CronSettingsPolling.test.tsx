import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'

import { LogSyncSection } from '@/components/CronSettingsModal/Schedule'

test('polling mode selector exposes Adaptive LIST-cost disclosure and updates the mode', async () => {
  const user = userEvent.setup()
  const setPollingMode = vi.fn()
  render(
    <LogSyncSection
      syncEnabled
      setSyncEnabled={vi.fn()}
      pollingMode="regular"
      setPollingMode={setPollingMode}
      deleteAfter={false}
      setDeleteAfter={vi.fn()}
      dataRetention="30"
      setDataRetention={vi.fn()}
      cacheRetention="90"
      setCacheRetention={vi.fn()}
      rollupRetention="12"
      setRollupRetention={vi.fn()}
      commitInterval="5"
      setCommitInterval={vi.fn()}
      syncLogEnabled
      setSyncLogEnabled={vi.fn()}
      syncRetention="7"
      setSyncRetention={vi.fn()}
      syncFreqLabel="every 2 minutes"
      isAnalyst={false}
      syncIntervalNum={120}
      adminSyncSeconds={120}
      rumRetention="30"
      setRumRetention={vi.fn()}
      rumEnabled={false}
      rumSyncIntervalSeconds="60"
      setRumSyncIntervalSeconds={vi.fn()}
      rumDeleteAfter={false}
      setRumDeleteAfter={vi.fn()}
    />,
  )

  expect(screen.getByText(/Adaptive makes up to two short follow-up discovery passes/)).toBeInTheDocument()
  await user.click(screen.getByRole('combobox', { name: 'Discovery Polling Mode' }))
  await user.click(await screen.findByRole('option', { name: 'Adaptive' }))

  expect(setPollingMode).toHaveBeenCalledWith('adaptive')
})
