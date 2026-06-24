"use client"

import * as React from "react"
import { Tooltip as TooltipPrimitive } from "@base-ui/react/tooltip"

import { cn } from "@/lib/utils"

// A-9 (a11y, WCAG 1.4.13 Content on Hover or Focus): defaults raised
// from 0/0 so tooltips don't disappear instantly on mouse-leave. Users
// with low-precision pointers (tremor, trackpad, touch+stylus) need
// enough closeDelay to traverse onto the tooltip content before it
// vanishes, and a non-zero open delay keeps incidental hovers from
// flashing tooltips. Callers can still override to a shorter open
// delay for dense admin UIs (see SystemStatus, CronColumns,
// CronScheduleBox), but should NOT set closeDelay to 0.
function TooltipProvider({
  delay = 400,
  closeDelay = 300,
  ...props
}: TooltipPrimitive.Provider.Props) {
  return (
    <TooltipPrimitive.Provider delay={delay} closeDelay={closeDelay} {...props} />
  )
}

function Tooltip({ ...props }: TooltipPrimitive.Root.Props) {
  return <TooltipPrimitive.Root data-slot="tooltip" {...props} />
}

function TooltipTrigger({ asChild, ...props }: TooltipPrimitive.Trigger.Props & { asChild?: boolean }) {
  // Note: Base UI uses `render` prop instead of `asChild`.
  // We spread the props since `render` handles custom tags in Base UI.
  return <TooltipPrimitive.Trigger data-slot="tooltip-trigger" {...props} />
}

function TooltipContent({
  className,
  children,
  sideOffset = 4,
  side,
  ...props
}: TooltipPrimitive.Popup.Props & { sideOffset?: number, side?: "top" | "right" | "bottom" | "left" }) {
  return (
    <TooltipPrimitive.Portal>
      <TooltipPrimitive.Positioner sideOffset={sideOffset} side={side} className="z-[100]">
        <TooltipPrimitive.Popup
          data-slot="tooltip-content"
          className={cn(
            "z-[100] overflow-hidden rounded-md border bg-popover px-3 py-1.5 text-sm text-popover-foreground shadow-md animate-in fade-in-0 zoom-in-95",
            className
          )}
          {...props}
        >
          {children}
        </TooltipPrimitive.Popup>
      </TooltipPrimitive.Positioner>
    </TooltipPrimitive.Portal>
  )
}

export { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider }
