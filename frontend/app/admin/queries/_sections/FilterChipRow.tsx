'use client'

import { Button } from '@/components/ui/button'

/** Generic controlled chip row. The parent owns the selected value; each
 *  option renders as a small toggle Button (default variant when selected,
 *  outline otherwise). `capitalize` is a visual no-op on already-cased
 *  labels. */
export function FilterChipRow<T extends string>({
  value,
  onChange,
  options,
}: {
  value: T
  onChange: (v: T) => void
  options: { value: T; label: string }[]
}) {
  return (
    <div className="flex items-center gap-1">
      {options.map((o) => (
        <Button
          key={o.value}
          variant={value === o.value ? 'default' : 'outline'}
          size="sm"
          className="h-7 px-2 text-xs capitalize"
          onClick={() => onChange(o.value)}
        >
          {o.label}
        </Button>
      ))}
    </div>
  )
}
