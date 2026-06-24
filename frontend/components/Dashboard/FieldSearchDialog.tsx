'use client'

import React, { useState } from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { Search, Loader2 } from 'lucide-react'
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

  const { data, isLoading, isFetching } = useFieldValues({ field, search: debouncedSearch, limit: 100, enabled: open })

  const handleSelect = (value: string | number) => {
    addFilter(field, String(value), 'include')
    setOpen(false)
  }

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
            {isLoading ? (
              <div className="space-y-2 p-2">
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
              </div>
            ) : data?.values?.length === 0 ? (
              <div className="p-4 text-center text-sm text-muted-foreground">No results found</div>
            ) : (
              <div className="flex flex-col gap-1">
                {data?.values?.map((v, i) => (
                  <button
                    key={i}
                    onClick={() => handleSelect(v.value as any)}
                    className="flex items-center justify-between px-3 py-2 text-sm hover:bg-muted rounded-sm transition-colors text-left"
                  >
                    <span className="truncate max-w-[300px]">
                      {v.label || formatValue(field, v.value as any)}
                    </span>
                    <span className="text-muted-foreground text-xs">{v.count.toLocaleString()}</span>
                  </button>
                ))}
              </div>
            )}
          </ScrollArea>
        </div>
      </DialogContent>
    </Dialog>
  )
}
