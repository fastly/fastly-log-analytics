import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'

import { LogSyncSection } from '@/components/CronSettingsModal/Schedule'

test('displays banner with Configure RUM link when RUM is disabled on edge', () => {
  render(
    <LogSyncSection
      syncEnabled
      setSyncEnabled={vi.fn()}
      pollingMode="regular"
      setPollingMode={vi.fn()}
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
      rumSyncEnabled={false}
      setRumSyncEnabled={vi.fn()}
      rumLogPeriod="300"
      setRumLogPeriod={vi.fn()}
      rumDeleteAfter={false}
      setRumDeleteAfter={vi.fn()}
      serviceId="test-service-123"
    />,
  )

  expect(screen.getByText(/Real User Monitoring is not currently enabled on Fastly edge/)).toBeInTheDocument()
  const link = screen.getByRole('link', { name: 'Configure RUM' })
  expect(link).toBeInTheDocument()
  expect(link).toHaveAttribute('href', '/admin/rum?service=test-service-123')
})

test('renders Enable RUM Beacon Sync switch when RUM is enabled on edge and allows toggling', async () => {
  const user = userEvent.setup()
  const setRumSyncEnabled = vi.fn()

  render(
    <LogSyncSection
      syncEnabled
      setSyncEnabled={vi.fn()}
      pollingMode="regular"
      setPollingMode={vi.fn()}
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
      rumEnabled={true}
      rumSyncEnabled={false}
      setRumSyncEnabled={setRumSyncEnabled}
      rumLogPeriod="300"
      setRumLogPeriod={vi.fn()}
      rumDeleteAfter={false}
      setRumDeleteAfter={vi.fn()}
      serviceId="test-service-123"
    />,
  )

  expect(screen.getByText('Enable RUM Beacon Sync')).toBeInTheDocument()
  const rumSwitch = screen.getByRole('switch', { name: 'Enable RUM Beacon Sync' })
  expect(rumSwitch).toBeInTheDocument()
  expect(rumSwitch).toHaveAttribute('aria-checked', 'false')

  await user.click(rumSwitch)
  expect(setRumSyncEnabled).toHaveBeenCalledWith(true, expect.anything())
})
