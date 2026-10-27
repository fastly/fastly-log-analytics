'use client'

import React from 'react'
import { ListChecks } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { PerReasonAucHelp, StatusPanelHelp } from './help-content'

export interface ScoringRulesDialogProps {
  open?: boolean
  onOpenChange?: (open: boolean) => void
  trigger?: React.ReactNode
}

export function ScoringRulesDialog({ open, onOpenChange, trigger }: ScoringRulesDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      {trigger ? (
        <DialogTrigger render={trigger as React.ReactElement} />
      ) : (
        <DialogTrigger
          render={
            <Button variant="outline" size="sm" data-testid="scoring-rules-dialog-trigger">
              <ListChecks className="h-3.5 w-3.5 mr-1.5" />
              Scoring Rules
            </Button>
          }
        />
      )}
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto" data-testid="scoring-rules-dialog">
        <DialogHeader>
          <DialogTitle>Session Scoring Rules & Reason Codes</DialogTitle>
          <DialogDescription>
            Reference for edge L1 state-machine rules and L2 route-transition anomaly evaluation.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-6 text-sm">
          <StatusPanelHelp />
          <div className="border-t pt-4">
            <PerReasonAucHelp />
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}
