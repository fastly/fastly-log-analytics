'use client'

import * as React from "react"
import { cn } from "@/lib/utils"

interface PageHeaderProps {
  title: string
  description?: string | React.ReactNode
  icon?: React.ElementType
  className?: string
  children?: React.ReactNode
}

export function PageHeader({ 
  title, 
  description, 
  icon: Icon, 
  className, 
  children 
}: PageHeaderProps) {
  return (
    <div className={cn("flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6", className)}>
      <div className="flex items-start gap-4">
        {Icon && (
          <div className="mt-1 p-2 rounded-lg bg-primary/10 text-primary shrink-0">
            <Icon className="h-6 w-6" />
          </div>
        )}
        <div className="space-y-1">
          <h1 className="text-2xl font-bold tracking-tight">{title}</h1>
          {description && (
            typeof description === 'string' ? (
              <p className="text-sm text-muted-foreground max-w-2xl">
                {description}
              </p>
            ) : (
              <div className="text-sm text-muted-foreground max-w-2xl">
                {description}
              </div>
            )
          )}
        </div>
      </div>
      {children && (
        <div className="flex items-center gap-2 shrink-0">
          {children}
        </div>
      )}
    </div>
  )
}
