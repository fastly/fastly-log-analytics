'use client'

import * as React from 'react'
import { Check, ChevronsUpDown } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { cn } from '@/lib/utils'

export interface PickerOption {
  value: string
  /** Main line. */
  label: string
  /** Secondary line, smaller and muted (a code, a count, a summary). */
  detail?: string
  /** What the closed control shows for this option; defaults to `label`. */
  selectedText?: string
}

interface ValuePickerProps {
  value: string
  options: PickerOption[]
  onChange: (value: string) => void
  /** Name for the control, or the id of the element that labels it. */
  ariaLabel?: string
  labelledBy?: string
  placeholder: string
  searchPlaceholder: string
  emptyText?: string
  /** Lets the user use text that is not in the list: it appears as a "Use “…”" row while typing. */
  allowCustom?: boolean
  /** Monospace text, for ids and other machine values. */
  mono?: boolean
  disabled?: boolean
  className?: string
}

/**
 * A searchable single-choice list. Each option is two lines: the name on top, a muted detail
 * underneath. Search matches the name, the value and the detail.
 */
export function ValuePicker({
  value,
  options,
  onChange,
  ariaLabel,
  labelledBy,
  placeholder,
  searchPlaceholder,
  emptyText = 'Nothing found.',
  allowCustom = false,
  mono = false,
  disabled,
  className,
}: ValuePickerProps) {
  const [open, setOpen] = React.useState(false)
  const [search, setSearch] = React.useState('')
  const selected = options.find((o) => o.value === value)
  const typed = search.trim()
  const offerCustom = allowCustom && typed !== '' && !options.some((o) => o.value.toLowerCase() === typed.toLowerCase())
  const shown = selected ? (selected.selectedText ?? selected.label) : value

  const choose = (next: string) => {
    onChange(next)
    setOpen(false)
    setSearch('')
  }

  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        setOpen(next)
        if (!next) setSearch('')
      }}
    >
      <PopoverTrigger
        aria-label={ariaLabel}
        aria-labelledby={labelledBy}
        disabled={disabled}
        render={
          <Button
            variant="outline"
            role="combobox"
            aria-expanded={open}
            className={cn('h-8 w-full min-w-44 justify-between text-xs font-normal', mono && 'font-mono', className)}
          >
            <span className={cn('truncate', !shown && 'text-muted-foreground')}>{shown || placeholder}</span>
            <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" aria-hidden="true" />
          </Button>
        }
      />
      <PopoverContent className="w-80 p-0" align="start">
        <Command>
          <CommandInput placeholder={searchPlaceholder} value={search} onValueChange={setSearch} />
          <CommandEmpty>{emptyText}</CommandEmpty>
          <CommandList>
            <CommandGroup>
              {offerCustom && (
                <CommandItem forceMount value={`__custom__ ${typed}`} onSelect={() => choose(typed)}>
                  <span className="flex min-w-0 flex-1 flex-col">
                    <span className={cn('truncate text-sm', mono && 'font-mono')}>{`Use “${typed}”`}</span>
                    <span className="text-xs text-muted-foreground">Not in the list</span>
                  </span>
                </CommandItem>
              )}
              {options.map((o) => (
                <CommandItem
                  key={o.value}
                  value={`${o.label} ${o.value} ${o.detail ?? ''}`}
                  onSelect={() => choose(o.value)}
                >
                  <span className="flex min-w-0 flex-1 flex-col">
                    <span className={cn('truncate text-sm', mono && 'font-mono')} title={o.label}>
                      {o.label}
                    </span>
                    {o.detail && <span className="truncate text-xs text-muted-foreground tabular-nums">{o.detail}</span>}
                  </span>
                  <Check className={cn('h-4 w-4 shrink-0', o.value === value ? 'opacity-100' : 'opacity-0')} aria-hidden="true" />
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
