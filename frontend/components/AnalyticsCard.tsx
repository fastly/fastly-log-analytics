'use client'

import React from 'react'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import { Loader2, HelpCircle } from 'lucide-react'
import { HelpDialog } from '@/components/ui/help-dialog'
import { Button } from '@/components/ui/button'
import { UpdatingBadge } from '@/components/UpdatingBadge'

interface AnalyticsCardProps {
  title: React.ReactNode
  icon?: React.ReactNode
  isLoading?: boolean
  isFetching?: boolean
  children: React.ReactNode
  className?: string
  contentClassName?: string
  headerAction?: React.ReactNode
  description?: string
  footer?: React.ReactNode
  helpContent?: React.ReactNode
  helpTitle?: string
}

export function AnalyticsCard({
  title,
  icon,
  isLoading,
  isFetching,
  children,
  className,
  contentClassName,
  headerAction,
  description,
  footer,
  helpContent,
  helpTitle,
}: AnalyticsCardProps) {
  const [isHelpOpen, setIsHelpOpen] = React.useState(false)

  return (
    <>
    <Card className={cn("flex flex-col overflow-hidden", className)}>
      <CardHeader className="px-4 pt-0 pb-3 border-b space-y-0">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            {icon && <div className="text-primary">{icon}</div>}
            <div>
              <CardTitle className="text-sm font-medium">{title}</CardTitle>
              {description && <p className="text-xs text-muted-foreground mt-0.5">{description}</p>}
            </div>
          </div>
          <div className="flex items-center gap-3">
            {isFetching && !isLoading && <UpdatingBadge />}
            {headerAction}
            {helpContent && (
              <Button
                variant="ghost"
                size="icon"
                aria-label="About this chart"
                className="h-6 w-6 text-muted-foreground hover:text-foreground"
                onClick={() => setIsHelpOpen(true)}
                title="About this chart"
              >
                <HelpCircle className="h-4 w-4" />
              </Button>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent className={cn("p-4 flex-1 relative min-h-0", contentClassName)}>
        {isLoading ? (
          // bg-background (opaque) rather than bg-background/50: when data is
          // undefined the children render their unit suffixes anyway (e.g.
          // `summary.data?.ottfb_p50_ms?.toFixed(1)}ms` becomes literal "ms"),
          // which bled through the half-transparent overlay during cold load
          // and made the card look half-broken. Opaque overlay hides them.
          // The refetch-with-old-data UX is preserved by the separate
          // `isFetching && !isLoading` opacity-40 branch on the children below.
          <div className="absolute inset-0 flex items-center justify-center bg-background z-10">
            <div className="flex flex-col items-center gap-2">
              <Loader2 className="h-6 w-6 animate-spin text-primary" aria-hidden="true" />
              <span className="text-xs text-muted-foreground animate-pulse font-medium">Loading data...</span>
            </div>
          </div>
        ) : null}
        <div className={cn("h-full", isFetching && !isLoading && "opacity-40 pointer-events-none transition-opacity")}>
          {children}
        </div>
      </CardContent>
      {footer && (
        <div className="p-2 border-t bg-muted/30">
          {footer}
        </div>
      )}
    </Card>

    {helpContent && (
      <HelpDialog
        open={isHelpOpen}
        onOpenChange={setIsHelpOpen}
        title={helpTitle ?? title}
        icon={icon}
        size="lg"
      >
        {helpContent}
      </HelpDialog>
    )}
    </>
  )
}
