'use client'

import React, { useState } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { Search, Loader2, Check, X } from 'lucide-react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Skeleton } from '@/components/ui/skeleton'
import { formatValue } from '@/lib/format'
import { PopLabel } from '@/components/PopLabel'
import { useDebounce } from '@/hooks/useDebounce'
import { useFieldValues } from '@/hooks/useFieldValues'

interface FieldSearchDialogProps {
  field: string
  title: string
}

export function FieldSearchDialog({ field, title }: FieldSearchDialogProps) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const debouncedSearch = useDebounce(search)

  const addFilter = useFilterStore(s => s.addFilter)
  const removeFilter = useFilterStore(s => s.removeFilter)
  const filters = useFilterStore(s => s.filters)

  const { data, isLoading, isFetching } = useFieldValues({ field, search: debouncedSearch, limit: 100, enabled: open })

  // Filters already active for this field are pinned to the top of the list,
  // so re-opening the dialog shows what's currently selected. They follow the
  // live search text (the pins are client-side, no need to wait for the
  // debounced fetch) and clicking a pin removes that filter without closing
  // the dialog, so several can be cleared in one visit.
  const selectedPills = filters.filter(f =>
    f.column === field &&
    (!search || f.value.toLowerCase().includes(search.toLowerCase()))
  )
  const selectedValues = new Set(selectedPills.map(f => f.value))
  const countByValue = new Map((data?.values ?? []).map(v => [String(v.value), v.count]))
  const unselected = (data?.values ?? []).filter(v => !selectedValues.has(String(v.value)))

  const handleSelect = (value: string | number) => {
    addFilter(field, String(value), 'include')
    setOpen(false)
  }

  const renderValue = (value: string | number, label?: string | null) =>
    field === 'pop'
      ? <PopLabel code={String(value)} />
      : (label || formatValue(field, value))

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="ghost" size="icon" aria-label={`Search ${title}`} className="h-6 w-6 text-muted-foreground hover:text-foreground" />
        }
      >
        <Search className="h-3.5 w-3.5" aria-hidden="true" />
      </DialogTrigger>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle>Search {title}</DialogTitle>
        </DialogHeader>
        <div className="py-2">
          <div className="relative mb-4">
            <Input
              placeholder={`Search for a ${title}...`}
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="pr-9"
              autoFocus
            />
            {isFetching && (
              <div className="absolute right-3 top-1/2 -translate-y-1/2">
                <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
              </div>
            )}
          </div>
          <ScrollArea className="h-[300px] border rounded-md p-2">
            <div className="flex flex-col gap-1">
              {selectedPills.length > 0 && (
                <>
                  <div className="px-3 pt-1 pb-0.5 text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                    Selected
                  </div>
                  {selectedPills.map(pill => (
                    <button
                      key={pill.id}
                      onClick={() => removeFilter(pill.id)}
                      aria-label={`Remove ${pill.mode} filter ${pill.value}`}
                      className="flex items-center justify-between px-3 py-2 text-sm bg-muted/50 hover:bg-muted rounded-sm transition-colors text-left"
                    >
                      <span className="flex items-center gap-2 truncate max-w-[300px]">
                        {pill.mode === 'exclude' ? (
                          <X className="h-3.5 w-3.5 shrink-0 text-red-500" aria-hidden="true" />
                        ) : (
                          <Check className="h-3.5 w-3.5 shrink-0 text-green-500" aria-hidden="true" />
                        )}
                        {renderValue(pill.value)}
                      </span>
                      <span className="text-muted-foreground text-xs">
                        {countByValue.get(pill.value)?.toLocaleString() ?? ''}
                      </span>
                    </button>
                  ))}
                  <div className="border-t my-1" aria-hidden="true" />
                </>
              )}
              {isLoading ? (
                <div className="space-y-2 p-2">
                  <Skeleton className="h-8 w-full" />
                  <Skeleton className="h-8 w-full" />
                  <Skeleton className="h-8 w-full" />
                </div>
              ) : unselected.length === 0 ? (
                <div className="p-4 text-center text-sm text-muted-foreground">
                  {selectedPills.length > 0 ? 'No other values found' : 'No results found'}
                </div>
              ) : (
                unselected.map(v => (
                  <button
                    key={String(v.value)}
                    onClick={() => handleSelect(v.value as string | number)}
                    className="flex items-center justify-between px-3 py-2 text-sm hover:bg-muted rounded-sm transition-colors text-left"
                  >
                    <span className="truncate max-w-[300px]">
                      {renderValue(v.value as string | number, v.label)}
                    </span>
                    <span className="text-muted-foreground text-xs">{v.count.toLocaleString()}</span>
                  </button>
                ))
              )}
            </div>
          </ScrollArea>
        </div>
      </DialogContent>
    </Dialog>
  )
}
