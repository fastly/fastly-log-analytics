'use client'

import * as React from 'react'
import { ChevronsUpDown, Trash2 } from 'lucide-react'
import { useRouter } from 'next/navigation'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import { useQuery, useQueryClient } from '@tanstack/react-query'

import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import type { components } from '@/types/api.generated'

type SavedView = components["schemas"]["SavedView"]

export function ViewSelector() {
  const [open, setOpen] = React.useState(false)
  const router = useRouter()
  const { activeServiceId } = useServiceStore()
  const queryClient = useQueryClient()

  // Perf audit Phase D: useBootstrap seeds ['views', service_id] in
  // its queryFn. Gate on bootstrap pending so this query hits the
  // seeded cache on cold load instead of racing the seed.
  const bootstrapState = queryClient.getQueryState(['bootstrap'])
  const bootstrapPending = bootstrapState !== undefined && bootstrapState.status === 'pending'

  const { data: views } = useQuery({
    queryKey: ['views', activeServiceId],
    queryFn: async () => {
      if (!activeServiceId) return []
      const { data } = await client.GET("/api/views/{service_id}", {
        params: { path: { service_id: activeServiceId } }
      })
      return data as any
    },
    enabled: !!activeServiceId && !bootstrapPending,
  })

  const handleDelete = async (e: React.MouseEvent, id: string) => {
    e.stopPropagation()
    if (confirm('Are you sure you want to delete this view?')) {
      await client.DELETE("/api/views/{view_id}", {
        params: { path: { view_id: id } }
      })
      queryClient.invalidateQueries({ queryKey: ['views', activeServiceId] })
      // Bootstrap response also carries seeded views; invalidate so the
      // next bootstrap refetch reflects the deletion.
      queryClient.invalidateQueries({ queryKey: ['bootstrap'] })
    }
  }

  const handleSelect = (view: SavedView) => {
    const url = new URL(window.location.href)
    url.searchParams.set('view', view.id!)
    router.push(url.toString())
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={
          <Button
            variant="outline"
            role="combobox"
            aria-expanded={open}
            className="h-8 justify-between min-w-[140px] text-xs"
          >
            <span className="truncate">Saved Views</span>
            <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
          </Button>
        }
      />
      <PopoverContent className="w-[200px] p-0">
        <Command>
          <CommandInput placeholder="Search views..." />
          <CommandEmpty>No view found.</CommandEmpty>
          <CommandList>
            <CommandGroup>
              {views?.map((view: any) => (
                <CommandItem
                  key={view.id}
                  value={view.name}
                  onSelect={() => handleSelect(view)}
                  className="flex items-center justify-between"
                >
                  <span className="truncate flex-1">{view.name}</span>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 text-muted-foreground hover:text-destructive"
                    onClick={(e) => handleDelete(e, view.id!)}
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
