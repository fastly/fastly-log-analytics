'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { SSEProgressView } from '@/components/SSEModal'

export function SSEModal({
  isSSEModalOpen,
  setIsSSEModalOpen,
  sseStatus,
  sseTitle,
  sseError,
  sseDescription,
  lines,
  stop,
}: {
  isSSEModalOpen: boolean
  setIsSSEModalOpen: (open: boolean) => void
  sseStatus: any
  sseTitle: string
  sseError: any
  sseDescription: string
  lines: any
  stop: () => void
}) {
  const queryClient = useQueryClient()

  return (
    <Dialog open={isSSEModalOpen} onOpenChange={(open) => {
      if (sseStatus === 'streaming') return
      setIsSSEModalOpen(open)
      if (!open) {
        stop()
        queryClient.invalidateQueries({ queryKey: ['admin'] })
        queryClient.invalidateQueries({ queryKey: ['dashboard'] })
      }
    }}>
      <DialogContent className="sm:max-w-4xl max-h-[85vh] min-h-[50vh] flex flex-col p-0 overflow-hidden" showCloseButton={sseStatus !== 'streaming'}>
        <DialogHeader className="px-6 pt-6 pb-4 border-b shrink-0 bg-background">
          <DialogTitle>{sseTitle}</DialogTitle>
        </DialogHeader>

        <SSEProgressView
          lines={lines}
          status={sseStatus}
          error={sseError}
          description={sseDescription}
          className="flex-1 mx-6 my-4"
        />

        <DialogFooter className="px-6 py-4 bg-muted/10 border-t shrink-0">
          {sseStatus !== 'streaming' && (
             <Button variant="outline" onClick={() => {
               setIsSSEModalOpen(false)
               stop()
               queryClient.invalidateQueries({ queryKey: ['admin'] })
             }}>
               {sseStatus === 'done' ? 'Close' : 'Cancel'}
             </Button>
          )}
          {sseStatus === 'streaming' && (
            <Button variant="outline" onClick={stop}>Stop</Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
