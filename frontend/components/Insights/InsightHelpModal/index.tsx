import React from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import { Globe } from 'lucide-react'
import type { InsightContent, InsightHelpModalProps } from './types'
import { getSecurityContent } from './sections/security'
import { getCacheContent } from './sections/cache'
import { getErrorsContent } from './sections/errors'
import { getTrafficContent } from './sections/traffic'
import { getPerformanceContent } from './sections/performance'
import { getOptimizationContent, getDefaultContent } from './sections/optimization'

export type { InsightHelpModalProps } from './types'

function getContent(id: string): InsightContent {
  return (
    getSecurityContent(id) ||
    getCacheContent(id) ||
    getErrorsContent(id) ||
    getTrafficContent(id) ||
    getPerformanceContent(id) ||
    getOptimizationContent(id) ||
    getDefaultContent()
  )
}

export function InsightHelpModal({ insightId, isOpen, onOpenChange }: InsightHelpModalProps) {
  const content = getContent(insightId)
  if (!content) return null

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl p-6 md:p-8 overflow-y-auto max-h-[90vh]">
        <DialogHeader>
          <DialogTitle className="text-xl flex items-center gap-2">
            {content.icon}
            {content.title}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-6 mt-2">
          {content.diagram && content.diagram}

          <div className="text-sm text-muted-foreground leading-relaxed">
            {content.description}
          </div>

          {content.fields.length > 0 && (
            <div className="bg-muted/50 p-4 rounded-lg border">
              <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-2">
                <Globe className="h-4 w-4" /> Required Log Fields
              </h4>
              <div className="flex flex-wrap gap-2">
                {content.fields.map(f => (
                  <Badge key={f} variant="outline" className="font-mono bg-background">{f}</Badge>
                ))}
              </div>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
