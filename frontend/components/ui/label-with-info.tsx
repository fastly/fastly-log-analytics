'use client'

import * as React from "react"
import { Info } from "lucide-react"
import { cn } from "@/lib/utils"
import { Label } from "@/components/ui/label"
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
      <TooltipProvider delay={0}>
        <Tooltip>
          <TooltipTrigger render={<span className="flex items-center" />}>
            <Info className="h-3.5 w-3.5 text-muted-foreground " />
          </TooltipTrigger>
          <TooltipContent side="right" className="max-w-xs text-xs">
            <p>{info}</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </div>
  )
}
