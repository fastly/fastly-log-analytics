'use client'

import React, { useState } from 'react'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { InsightCardData } from '@/types/api'
import { cn } from '@/lib/utils'
import { AlertTriangle, Info, CheckCircle, AlertCircle, HelpCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { InsightHelpModal } from './InsightHelpModal'
import { ImpossibleDistanceModal } from './ImpossibleDistanceModal'
import { InsightDataModal } from './InsightDataModal'
import { InsightItemRow } from './InsightItemRow'
import { ImpossibleDistanceData } from './types'

interface InsightCardProps {
  insight: InsightCardData
}

const SEVERITY_ICON = {
  clean: CheckCircle,
  info: Info,
  warning: AlertTriangle,
  critical: AlertCircle,
  error: AlertCircle,
}

const SEVERITY_ICON_COLOR = {
  clean: 'text-green-500',
  info: 'text-blue-500',
  warning: 'text-yellow-500',
  critical: 'text-red-500',
  error: 'text-red-600',
}

const SEVERITY_BADGE_CLASS = {
  clean: 'bg-green-50 text-green-700 border-green-200 dark:bg-green-950/30 dark:text-green-400 dark:border-green-800',
  info: 'bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950/30 dark:text-blue-400 dark:border-blue-800',
  warning: 'bg-yellow-50 text-yellow-700 border-yellow-200 dark:bg-yellow-950/30 dark:text-yellow-400 dark:border-yellow-800',
  critical: 'bg-red-50 text-red-700 border-red-200 dark:bg-red-950/30 dark:text-red-400 dark:border-red-800',
  error: 'bg-red-100 text-red-800 border-red-300 dark:bg-red-950/50 dark:text-red-400 dark:border-red-800',
}

export function InsightCard({ insight }: InsightCardProps) {
  const [isHelpOpen, setIsHelpOpen] = useState(false)
  const [isDataModalOpen, setIsDataModalOpen] = useState(false)
  const [selectedMapItem, setSelectedMapItem] = useState<ImpossibleDistanceData | null>(null)

  const Icon = SEVERITY_ICON[insight.severity as keyof typeof SEVERITY_ICON] || AlertCircle
  const iconColor = SEVERITY_ICON_COLOR[insight.severity as keyof typeof SEVERITY_ICON_COLOR] || 'text-muted-foreground'
  const badgeClass = SEVERITY_BADGE_CLASS[insight.severity as keyof typeof SEVERITY_BADGE_CLASS] || ''

  return (
    <>
      <Card className="h-full flex flex-col">
        <CardHeader className="pb-2">
          <div className="flex items-start justify-between gap-2">
            <div className="flex items-center gap-2 min-w-0">
              <Icon className={cn('h-5 w-5 shrink-0', iconColor)} />
              <CardTitle className="text-base leading-tight">{insight.title}</CardTitle>
            </div>
            <div className="flex items-center gap-2 shrink-0">
              <Badge
                variant="outline"
                className={cn('text-[10px] uppercase tracking-wider border', badgeClass)}
              >
                {insight.severity}
              </Badge>
              <Button
                variant="ghost"
                size="icon"
                aria-label="How this insight works"
                className="h-6 w-6 text-muted-foreground hover:text-foreground"
                onClick={() => setIsHelpOpen(true)}
                title="How this works"
              >
                <HelpCircle className="h-4 w-4" />
              </Button>
            </div>
          </div>
          <CardDescription className="text-xs mt-1">
            {insight.description}
          </CardDescription>
        </CardHeader>

        <CardContent className="flex-1 flex flex-col pt-0">
          <p className="text-sm font-medium my-2 leading-snug">{insight.summary}</p>

          {insight.items && insight.items.length > 0 && (
            <div className="space-y-1.5 mt-1">
              {insight.items.slice(0, 5).map((item, i) => (
                <InsightItemRow
                  key={i}
                  item={item}
                  insightId={insight.id}
                  onMapClick={(data) => setSelectedMapItem(data)}
                />
              ))}
              {insight.items.length > 5 && (
                <div className="flex justify-center py-1">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-[10px] h-6 px-2 text-muted-foreground hover:text-foreground"
                    onClick={() => setIsDataModalOpen(true)}
                  >
                    + Show {insight.items.length - 5} more
                  </Button>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      <InsightHelpModal
        insightId={insight.id}
        isOpen={isHelpOpen}
        onOpenChange={setIsHelpOpen}
      />

      <InsightDataModal
        insight={insight}
        isOpen={isDataModalOpen}
        onOpenChange={setIsDataModalOpen}
        onMapClick={(data) => setSelectedMapItem(data)}
      />

      <ImpossibleDistanceModal
        isOpen={!!selectedMapItem}
        onOpenChange={(open) => !open && setSelectedMapItem(null)}
        data={selectedMapItem}
      />
    </>
  )
}
