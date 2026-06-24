export const RETENTION_OPTIONS = [
  { value: '1', label: '1 day' },
  { value: '3', label: '3 days' },
  { value: '7', label: '7 days' },
  { value: '14', label: '14 days' },
  { value: '30', label: '30 days' },
  { value: '90', label: '90 days' },
  { value: '0', label: 'Forever' },
]

export const RETENTION_LABELS: Record<string, string> = Object.fromEntries(
  RETENTION_OPTIONS.map(o => [o.value, o.label])
)

export const COMMIT_INTERVAL_OPTIONS = [
  { value: '1',  label: 'Every 1 min  — most fresh, most snapshots' },
  { value: '2',  label: 'Every 2 min' },
  { value: '3',  label: 'Every 3 min' },
  { value: '5',  label: 'Every 5 min  — recommended' },
  { value: '15', label: 'Every 15 min' },
  { value: '30', label: 'Every 30 min' },
  { value: '60', label: 'Every 60 min — fewest snapshots' },
]

export const SYNC_INTERVAL_OPTIONS = [
  { value: '1',  label: 'Every 1 minute' },
  { value: '2',  label: 'Every 2 minutes' },
  { value: '5',  label: 'Every 5 minutes' },
  { value: '10', label: 'Every 10 minutes' },
  { value: '15', label: 'Every 15 minutes' },
  { value: '30', label: 'Every 30 minutes' },
  { value: '60', label: 'Every 60 minutes' },
]

export const NGWAF_INTERVAL_OPTIONS = [
  { value: '1',  label: 'Every 1 minute' },
  { value: '2',  label: 'Every 2 minutes' },
  { value: '5',  label: 'Every 5 minutes — recommended' },
  { value: '10', label: 'Every 10 minutes' },
  { value: '15', label: 'Every 15 minutes' },
  { value: '30', label: 'Every 30 minutes' },
  { value: '60', label: 'Every 60 minutes' },
]
