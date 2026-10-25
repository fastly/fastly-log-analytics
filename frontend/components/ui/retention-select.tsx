'use client'

import React from 'react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { RETENTION_OPTIONS } from '@/lib/constants'

interface RetentionOption {
  readonly value: string
  readonly label: string
}

interface RetentionSelectProps {
  id?: string
  value: string
  onValueChange: (value: string) => void
  options?: readonly RetentionOption[]
  triggerClassName?: string
  itemClassName?: string
}

export function RetentionSelect({
  id,
  value,
  onValueChange,
  options = RETENTION_OPTIONS,
  triggerClassName = 'h-7 text-[11px]',
  itemClassName = 'text-[11px]',
}: RetentionSelectProps) {
  const labels = React.useMemo(
    () => Object.fromEntries(options.map((o) => [o.value, o.label])),
    [options],
  )
  return (
    <Select value={value} onValueChange={(v) => v && onValueChange(v)}>
      <SelectTrigger id={id} className={triggerClassName}>
        <SelectValue>{(val) => labels[String(val)] || val}</SelectValue>
      </SelectTrigger>
      <SelectContent>
        {options.map((o) => (
          <SelectItem key={o.value} value={o.value} className={itemClassName}>
            {o.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}
