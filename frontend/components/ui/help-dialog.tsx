import * as React from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { cn } from "@/lib/utils"

interface HelpDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  title: React.ReactNode
  icon?: React.ReactNode
  size?: "lg" | "xl"
  titleClassName?: string
  children: React.ReactNode
}

/**
 * Shared help-dialog shell used by AnalyticsCard, StatCard, and other card
 * primitives. Centralises the dialog sizing, padding, header layout, and
 * body typography so consumers only have to pass title + content.
 */
export function HelpDialog({
  open,
  onOpenChange,
  title,
  icon,
  size = "lg",
  titleClassName,
  children,
}: HelpDialogProps) {
  const widthClass = size === "xl" ? "max-w-xl p-6 md:p-8" : "max-w-lg p-6"

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn(widthClass, "overflow-y-auto max-h-[90vh]")}>
        <DialogHeader>
          <DialogTitle className={cn("flex items-center gap-2", size === "xl" && "text-xl", titleClassName)}>
            {icon && <span className="text-primary">{icon}</span>}
            {title}
          </DialogTitle>
        </DialogHeader>
        <div className="text-sm text-muted-foreground leading-relaxed mt-2">
          {children}
        </div>
      </DialogContent>
    </Dialog>
  )
}
