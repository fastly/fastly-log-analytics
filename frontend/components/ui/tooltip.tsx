"use client"

import * as React from "react"
import { Tooltip as TooltipPrimitive } from "@base-ui/react/tooltip"

import { cn } from "@/lib/utils"

function TooltipProvider({
  delay = 0,
  closeDelay = 0,
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
