import React from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { InsightCardData } from '@/types/api'
import { ImpossibleDistanceData } from './types'
import { InsightItemRow } from './InsightItemRow'
import { ScrollArea } from '@/components/ui/scroll-area'

interface InsightDataModalProps {
  insight: InsightCardData
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  onMapClick?: (data: ImpossibleDistanceData) => void
  onCacheCollapseClick?: (url: string) => void
}

export function InsightDataModal({ insight, isOpen, onOpenChange, onMapClick, onCacheCollapseClick }: InsightDataModalProps) {
  if (!insight.items || insight.items.length === 0) return null;

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>All Data: {insight.title}</DialogTitle>
          <DialogDescription className="sr-only">
            Full list of records backing the “{insight.title}” insight.
          </DialogDescription>
        </DialogHeader>

        <ScrollArea className="flex-1 -mx-6 px-6">
          <div className="space-y-1.5 py-4">
            {insight.items.map((item, i) => (
              <InsightItemRow
                key={i}
                item={item}
                insightId={insight.id}
                onMapClick={onMapClick}
                onCacheCollapseClick={onCacheCollapseClick}
              />
            ))}
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  )
}
