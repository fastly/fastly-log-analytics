'use client'

import * as React from 'react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { getTimezoneAbbr } from '@/lib/date'

const SYSTEM_TIME_VALUE = '__system__'

const COMMON_TIMEZONES = [
  { label: 'UTC', value: 'UTC' },
  { label: 'US Eastern', value: 'America/New_York' },
  { label: 'US Central', value: 'America/Chicago' },
  { label: 'US Mountain', value: 'America/Denver' },
  { label: 'US Pacific', value: 'America/Los_Angeles' },
  { label: 'London', value: 'Europe/London' },
  { label: 'Berlin', value: 'Europe/Berlin' },
  { label: 'Tokyo', value: 'Asia/Tokyo' },
  { label: 'Sydney', value: 'Australia/Sydney' },
]

export function TimezoneSwitcher() {
  const [mounted, setMounted] = React.useState(false)
  const { mode, timezone, setTimezone, setSystemTimezone } = useTimezoneStore()

  React.useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setMounted(true)
  }, [])

  if (!mounted) {
    return <div className="w-[180px] h-10 border rounded-md" />
  }

  const selectValue = mode === 'system' ? SYSTEM_TIME_VALUE : timezone

  return (
    <Select
      value={selectValue}
      onValueChange={(value) => {
        if (!value) return
        if (value === SYSTEM_TIME_VALUE) {
          try {
            setSystemTimezone(Intl.DateTimeFormat().resolvedOptions().timeZone)
          } catch {
            /* leave the current zone in place */
          }
        } else {
          setTimezone(value)
        }
      }}
    >
      <SelectTrigger className="w-[180px]" aria-label="Display timezone">
        <SelectValue placeholder="Select timezone">
          {mode === 'system' ? `System Time (${getTimezoneAbbr(new Date(), timezone)})` : undefined}
        </SelectValue>
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={SYSTEM_TIME_VALUE}>System Time</SelectItem>
        <SelectSeparator />
        {COMMON_TIMEZONES.map((tz) => (
          <SelectItem key={tz.value} value={tz.value}>
            {tz.label}
          </SelectItem>
        ))}
        {mode === 'manual' && !COMMON_TIMEZONES.find(t => t.value === timezone) && (
          <SelectItem value={timezone}>{timezone}</SelectItem>
        )}
      </SelectContent>
    </Select>
  )
}
