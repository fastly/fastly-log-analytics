'use client'

import * as React from 'react'
import { Bookmark } from 'lucide-react'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'
import { client } from '@/lib/api'
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
  
  const { startTime, endTime, filters } = useFilterStore()
  const { activeServiceId } = useServiceStore()
  const pathname = usePathname()
  const queryClient = useQueryClient()

  const handleSave = async () => {
    if (!name || !activeServiceId) return
    setIsSaving(true)
    try {
      await client.POST("/api/views/", {
        body: {
          service_id: activeServiceId,
          name,
          filters_json: JSON.stringify(filters),
          start_time: startTime,
          end_time: endTime,
          page: pathname || "/dashboard"
        }
      })
      queryClient.invalidateQueries({ queryKey: ['views', activeServiceId] })
      // Bootstrap response also carries seeded views; invalidate so the
      // next bootstrap refetch reflects the new view.
      queryClient.invalidateQueries({ queryKey: ['bootstrap'] })
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
          <Button variant="outline" size="sm" className="h-8 gap-1.5 text-xs">
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
