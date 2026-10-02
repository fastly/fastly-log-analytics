'use client'

import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { CmcdConfigSection } from '@/components/CmcdConfigSection'
import { TokenConfigSection } from '@/components/TokenConfigSection'

interface StreamingConfigSectionProps {
  cmcdEnabled: boolean
  onCmcdEnabledChange: (enabled: boolean) => void
  cmcdMode: string
  onCmcdModeChange: (mode: string) => void
  cmcdVersion: number
  onCmcdVersionChange: (version: number) => void
  tokenEnabled: boolean
  onTokenEnabledChange: (enabled: boolean) => void
  subscriberIdExpr: string
  onSubscriberIdExprChange: (expr: string) => void
  /** Enables live VCL lint of the subscriber id expression (omit before provisioning). */
  serviceId?: string
  disabled?: boolean
}

/**
 * STREAMING group: a collapsible container (no toggle of its own) holding two
 * independent sub-options, CMCD Metrics and Token. Each deploys its own VCL.
 */
export function StreamingConfigSection(props: StreamingConfigSectionProps) {
  const [isOpen, setIsOpen] = useState(false)
  const enabledCount = Number(props.cmcdEnabled) + Number(props.tokenEnabled)

  return (
    <div className="border border-border/60 rounded-lg overflow-hidden bg-card/50" data-testid="streaming-config-section">
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        aria-expanded={isOpen}
        className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left cursor-pointer border-0"
      >
        <span className="flex items-center gap-2">
          <span className="text-xs font-bold tracking-tight uppercase text-foreground/80">Streaming</span>
          <span className="text-[10px] text-muted-foreground ml-1">{enabledCount} of 2 enabled</span>
        </span>
        <span className="text-muted-foreground">
          {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </span>
      </button>

      {isOpen && (
        <div className="p-3 border-t border-border/40 bg-card space-y-3">
          <CmcdConfigSection
            enabled={props.cmcdEnabled}
            onEnabledChange={props.onCmcdEnabledChange}
            mode={props.cmcdMode}
            onModeChange={props.onCmcdModeChange}
            version={props.cmcdVersion}
            onVersionChange={props.onCmcdVersionChange}
            disabled={props.disabled}
          />
          <TokenConfigSection
            enabled={props.tokenEnabled}
            onEnabledChange={props.onTokenEnabledChange}
            subscriberIdExpr={props.subscriberIdExpr}
            onSubscriberIdExprChange={props.onSubscriberIdExprChange}
            serviceId={props.serviceId}
            disabled={props.disabled}
          />
        </div>
      )}
    </div>
  )
}
