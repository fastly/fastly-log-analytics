'use client'

import * as React from "react"
import { cn } from "@/lib/utils"

export function ReviewCard({ children, className }: { children: React.ReactNode, className?: string }) {
  return (
    <div className={cn("border rounded-xl p-4 bg-muted/10 flex flex-col gap-4 shadow-sm", className)}>
      {children}
    </div>
  )
}

export function ReviewHeader({ children, icon: Icon, className }: { children: React.ReactNode, icon?: any, className?: string }) {
  return (
    <h4 className={cn("font-semibold text-sm flex items-center gap-2 text-foreground/80 shrink-0", className)}>
      {Icon && <Icon className="h-4 w-4" />}
      {children}
    </h4>
  )
}

export function ReviewContent({ children, className }: { children: React.ReactNode, className?: string }) {
  return (
    <div className={cn("text-xs flex flex-col gap-2 flex-1", className)}>
      {children}
    </div>
  )
}

interface ReviewItemProps {
  label: string
  value?: string | number | React.ReactNode
  children?: React.ReactNode
  className?: string
  variant?: 'default' | 'between'
}

export function ReviewItem({ label, value, children, className, variant = 'default' }: ReviewItemProps) {
  if (variant === 'between') {
    return (
      <div className={cn("flex items-center justify-between", className)}>
        <span>{label}</span>
        <div className="shrink-0">
          {value || children}
        </div>
      </div>
    )
  }

  return (
    <div className={cn("flex flex-col gap-1", className)}>
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium truncate">{value || children}</span>
    </div>
  )
}
