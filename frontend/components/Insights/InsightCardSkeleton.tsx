'use client'

import React from 'react'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Loader2 } from 'lucide-react'

interface InsightCardSkeletonProps {
  title: string
  description?: string
}

// Loading shell shown while /api/insights is in flight. Same outer Card
// shape as InsightCard so swapping in the real card on data arrival is a
// content swap, not a layout shift.
export function InsightCardSkeleton({ title, description }: InsightCardSkeletonProps) {
  return (
    <Card className="h-full flex flex-col opacity-90">
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <Loader2 className="h-5 w-5 shrink-0 text-muted-foreground animate-spin" />
            <CardTitle className="text-base leading-tight">{title}</CardTitle>
          </div>
        </div>
        {description && (
          <CardDescription className="text-xs mt-1">{description}</CardDescription>
        )}
      </CardHeader>

      <CardContent className="flex-1 flex flex-col items-center justify-center pt-0 pb-6 text-xs text-muted-foreground">
        Loading…
      </CardContent>
    </Card>
  )
}
