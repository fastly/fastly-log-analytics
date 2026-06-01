'use client'

import * as React from "react"
import { cn } from "@/lib/utils"

interface SectionHeaderProps {
  title: string
  icon?: any
  className?: string
  children?: React.ReactNode
}

export function SectionHeader({ title, icon: Icon, className, children }: SectionHeaderProps) {
  return (
    <div className={cn("flex items-center gap-2 pb-2 border-b", className)}>
      {Icon && <Icon className="h-4 w-4 text-primary" />}
      <h4 className="text-xs font-bold uppercase tracking-widest text-muted-foreground">
        {title}
        {children}
      </h4>
    </div>
  )
}
