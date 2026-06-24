'use client'

import * as React from 'react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useTimezoneStore } from '@/stores/timezoneStore'

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
  const { timezone, setTimezone } = useTimezoneStore()

  React.useEffect(() => {
    setMounted(true)
  }, [])

  if (!mounted) {
    return <div className="w-[180px] h-10 border rounded-md" />
  }

  return (
    <Select value={timezone} onValueChange={(value) => { if (value) setTimezone(value) }}>
      <SelectTrigger className="w-[180px]" aria-label="Display timezone">
        <SelectValue placeholder="Select timezone" />
      </SelectTrigger>
      <SelectContent>
        {COMMON_TIMEZONES.map((tz) => (
          <SelectItem key={tz.value} value={tz.value}>
            {tz.label}
          </SelectItem>
        ))}
        {!COMMON_TIMEZONES.find(t => t.value === timezone) && (
          <SelectItem value={timezone}>{timezone}</SelectItem>
        )}
      </SelectContent>
    </Select>
  )
}
