'use client'

import * as React from "react"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { cn } from "@/lib/utils"

interface ConfirmDialogProps {
  title: string
  description: React.ReactNode
  confirmLabel?: string
  cancelLabel?: string
  isDangerous?: boolean
  isPending?: boolean
  /** For controlled usage — pair with onOpenChange */
  open?: boolean
  onOpenChange?: (open: boolean) => void
  onConfirm: () => void
  /** For uncontrolled usage — trigger label and optional styling */
  triggerLabel?: React.ReactNode
  triggerClassName?: string
  triggerDisabled?: boolean
}

export function ConfirmDialog({
  title,
  description,
  confirmLabel = "Confirm",
  cancelLabel = "Cancel",
  isDangerous = false,
  isPending = false,
  open,
  onOpenChange,
  onConfirm,
  triggerLabel,
  triggerClassName,
  triggerDisabled,
}: ConfirmDialogProps) {
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      {triggerLabel !== undefined && (
        <AlertDialogTrigger disabled={triggerDisabled} className={cn(triggerClassName)}>
          {triggerLabel}
        </AlertDialogTrigger>
      )}
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={isPending}>{cancelLabel}</AlertDialogCancel>
          <AlertDialogAction
            variant={isDangerous ? "destructive" : "default"}
            disabled={isPending}
            onClick={onConfirm}
          >
            {isPending ? "Working…" : confirmLabel}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
