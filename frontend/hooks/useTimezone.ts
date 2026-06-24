'use client'

import { useTimezoneStore } from '@/stores/timezoneStore'

/**
 * Active timezone (IANA tz string). Subscribe here when a component formats
 * dates or labels axes; orthogonal to time range and service selection.
 */
export function useTimezone(): string {
  return useTimezoneStore(s => s.timezone)
}
