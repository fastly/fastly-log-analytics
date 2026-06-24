'use client'

import * as React from 'react'
import { Bookmark } from 'lucide-react'
import { useFilterStore } from '@/stores/filterStore'
import { useShallow } from 'zustand/react/shallow'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { useQueryClient } from '@tanstack/react-query'
import { usePathname } from 'next/navigation'

import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

export function SaveViewDialog() {
  const [open, setOpen] = React.useState(false)
  const [name, setName] = React.useState('')
  const [isSaving, setIsSaving] = React.useState(false)

  // Capture the FULL store snapshot so restored views match the original
  // intent — previously edgeOnly / compareMode / relativeRange were silent
  // and a saved rolling-24h view came back as a frozen absolute window.
  const { startTime, endTime, filters, edgeOnly, compareMode, compareStartTime, compareEndTime, relativeRange } = useFilterStore(
    useShallow(s => ({
      startTime: s.startTime,
      endTime: s.endTime,
      filters: s.filters,
      edgeOnly: s.edgeOnly,
      compareMode: s.compareMode,
      compareStartTime: s.compareStartTime,
      compareEndTime: s.compareEndTime,
      relativeRange: s.relativeRange,
    }))
  )
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const pathname = usePathname()
  const queryClient = useQueryClient()

  const handleSave = async () => {
    if (!name || !activeServiceId) return
    setIsSaving(true)
    try {
      // The backend stores filters_json as an opaque string; bundle the
      // extra controls into the same blob under a `_view_extras` key the
      // restore-handler reads back. Older saved views (no _view_extras)
      // still restore unchanged — the keys default to the store-init values.
      const filterPayload = {
        filters,
        _view_extras: {
          edgeOnly,
          compareMode,
          compareStartTime,
          compareEndTime,
          relativeRange,
        },
      }
      await client.POST("/api/views/", {
        body: {
          service_id: activeServiceId,
          name,
          filters_json: JSON.stringify(filterPayload),
          start_time: startTime,
          end_time: endTime,
          page: pathname || "/dashboard"
        }
      })
      queryClient.invalidateQueries({ queryKey: ['views', activeServiceId] })
      // Bootstrap response also carries seeded views; invalidate so the
      // next bootstrap refetch reflects the new view.
      queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
      setOpen(false)
      setName('')
    } catch (error) {
      console.error('Failed to save view', error)
    } finally {
      setIsSaving(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="outline" size="sm" className="h-9 sm:h-8 gap-1.5 text-xs">
            <Bookmark className="h-3.5 w-3.5" />
            Save View
          </Button>
        }
      />
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle>Save Current View</DialogTitle>
          <DialogDescription>
            Save your current filters and time range to access them later.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-4">
          <div className="grid grid-cols-4 items-center gap-4">
            <Label htmlFor="name" className="text-right">
              Name
            </Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. Weekly Error Spikes"
              className="col-span-3"
            />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
          <Button onClick={handleSave} disabled={!name || isSaving}>
            {isSaving ? 'Saving...' : 'Save View'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
