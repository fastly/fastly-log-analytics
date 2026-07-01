'use client'

import { useState } from 'react'
import { Plus } from 'lucide-react'
import {
  Dialog,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { AddFilterDialogContent } from './AddFilterDialogContent'

/**
 * Add-Filter trigger + Dialog shell. Intentionally hook-light: it renders
 * once for every page that mounts a FilterBar (~13 routes), so it must NOT
 * subscribe a new React Query observer during FilterBar's initial render.
 *
 * The data-bound body (which calls useLogFieldsCatalog / useFieldValues)
 * lives in AddFilterDialogContent and is mounted only while ``open`` —
 * ``{open && <AddFilterDialogContent .../>}``. That moves the first new-key
 * observer subscription into a click-driven render pass, outside the render
 * of sibling components (SyncStatusBadge), which fixes the
 * "Cannot update a component while rendering a different component" warning
 * that React Query's synchronous QueryCache.notify produced when the body
 * subscribed during FilterBar's render. See AddFilterDialogContent for the
 * detailed rationale.
 */
export function AddFilterDialog() {
  const [open, setOpen] = useState(false)

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="outline" size="sm" className="h-9 sm:h-7 gap-1 pr-2.5 sm:pr-2 pl-2 sm:pl-1.5 border-dashed text-xs sm:text-[11px]">
            <Plus className="h-3 w-3" />
            <span>Add Filter</span>
          </Button>
        }
      />
      {open && <AddFilterDialogContent onClose={() => setOpen(false)} />}
    </Dialog>
  )
}
