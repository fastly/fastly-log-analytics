'use client'

import * as React from "react"
import { Info } from "lucide-react"
import { cn } from "@/lib/utils"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
  TooltipProvider,
} from "@/components/ui/tooltip"

interface LabelWithInfoProps {
  label: string
  info: string
  className?: string
  labelClassName?: string
  htmlFor?: string
}

export function LabelWithInfo({ label, info, className, labelClassName, htmlFor }: LabelWithInfoProps) {
  return (
    <div className={cn("flex items-center gap-1", className)}>
      <Label htmlFor={htmlFor} className={cn("text-xs font-semibold", labelClassName)}>
        {label}
      </Label>
      {/* A-9 (a11y, WCAG 1.4.13): inherit the accessible 400/300
          defaults from tooltip.tsx — info-icon tooltips carry the
          explanatory text users may need time to read and reach. */}
      <TooltipProvider>
        <Tooltip>
          {/* A-8 (a11y, WCAG 2.1.1): Button (not span) so keyboard users can
              tab to the info icon and trigger the tooltip on focus. */}
          <TooltipTrigger
            render={
              <Button
                variant="ghost"
                size="icon-xs"
                aria-label={`More info: ${label}`}
                className="flex items-center text-muted-foreground hover:text-foreground"
              />
            }
          >
            <Info className="h-3.5 w-3.5 " />
          </TooltipTrigger>
          <TooltipContent side="right" className="max-w-xs text-xs">
            <p>{info}</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </div>
  )
}
