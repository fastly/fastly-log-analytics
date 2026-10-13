'use client'

import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { Checkbox } from '@/components/ui/checkbox'
import { CmcdConfigSection } from '@/components/CmcdConfigSection'
import { ContentSecurityConfigSection } from '@/components/TokenConfigSection'

interface StreamingConfigSectionProps {
  cmcdEnabled: boolean
  onCmcdEnabledChange: (enabled: boolean) => void
  cmcdMode: string
  onCmcdModeChange: (mode: string) => void
  cmcdVersion: number
  onCmcdVersionChange: (version: number) => void
  /** Content Security master switch (was the Token checkbox). */
  tokenEnabled: boolean
  onTokenEnabledChange: (enabled: boolean) => void
  subscriberIdEnabled: boolean
  onSubscriberIdEnabledChange: (enabled: boolean) => void
  subscriberIdExpr: string
  onSubscriberIdExprChange: (expr: string) => void
  contentIdEnabled: boolean
  onContentIdEnabledChange: (enabled: boolean) => void
  contentIdExpr: string
  onContentIdExprChange: (expr: string) => void
  enforcementEnabled: boolean
  onEnforcementEnabledChange: (enabled: boolean) => void
  /** Enables live VCL lint of the token expressions (omit before provisioning). */
  serviceId?: string
  disabled?: boolean
}

/**
 * STREAMING group: two independently toggleable sub-options, CMCD Metrics and
 * Content Security, each deploying its own VCL. The group checkbox shows their combined
 * state (checked = both, indeterminate = one). Checking it enables both and
 * opens any child that still needs input; unchecking disables both.
 */
export function StreamingConfigSection(props: StreamingConfigSectionProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [cmcdOpen, setCmcdOpen] = useState(false)
  const [tokenOpen, setTokenOpen] = useState(false)

  const allEnabled = props.cmcdEnabled && props.tokenEnabled
  const someEnabled = props.cmcdEnabled || props.tokenEnabled
  const enabledCount = Number(props.cmcdEnabled) + Number(props.tokenEnabled)

  const toggleAll = () => {
    if (allEnabled) {
      props.onCmcdEnabledChange(false)
      props.onTokenEnabledChange(false)
      return
    }
    props.onCmcdEnabledChange(true)
    props.onTokenEnabledChange(true)
    // Enabling Content Security checks all three of its options.
    props.onSubscriberIdEnabledChange(true)
    props.onContentIdEnabledChange(true)
    props.onEnforcementEnabledChange(true)
    // CMCD works with its defaults; a checked ID can't deploy without an expression.
    if (props.subscriberIdExpr.trim() === '' || props.contentIdExpr.trim() === '') {
      setIsOpen(true)
      setTokenOpen(true)
    }
  }

  return (
    <div className="border border-border/60 rounded-lg overflow-hidden bg-card/50" data-testid="streaming-config-section">
      <div className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left">
        <div className="flex items-center gap-3">
          <Checkbox
            checked={allEnabled}
            indeterminate={someEnabled && !allEnabled}
            onCheckedChange={toggleAll}
            disabled={props.disabled}
            className="mr-1"
            aria-label="Enable Streaming"
          />
          <button
            type="button"
            onClick={() => setIsOpen(!isOpen)}
            aria-expanded={isOpen}
            className="flex items-center gap-2 text-left cursor-pointer bg-transparent border-0 p-0"
          >
            <h4 className="text-xs font-bold tracking-tight uppercase text-foreground/80">Streaming</h4>
            <span className="text-[10px] text-muted-foreground ml-1">{enabledCount} of 2 enabled</span>
          </button>
        </div>
        <button
          type="button"
          onClick={() => setIsOpen(!isOpen)}
          aria-label={isOpen ? 'Collapse group' : 'Expand group'}
          className="text-muted-foreground bg-transparent border-0 p-0 cursor-pointer"
        >
          {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </button>
      </div>

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
            open={cmcdOpen}
            onOpenChange={setCmcdOpen}
          />
          <ContentSecurityConfigSection
            enabled={props.tokenEnabled}
            onEnabledChange={props.onTokenEnabledChange}
            subscriberIdEnabled={props.subscriberIdEnabled}
            onSubscriberIdEnabledChange={props.onSubscriberIdEnabledChange}
            subscriberIdExpr={props.subscriberIdExpr}
            onSubscriberIdExprChange={props.onSubscriberIdExprChange}
            contentIdEnabled={props.contentIdEnabled}
            onContentIdEnabledChange={props.onContentIdEnabledChange}
            contentIdExpr={props.contentIdExpr}
            onContentIdExprChange={props.onContentIdExprChange}
            enforcementEnabled={props.enforcementEnabled}
            onEnforcementEnabledChange={props.onEnforcementEnabledChange}
            serviceId={props.serviceId}
            disabled={props.disabled}
            open={tokenOpen}
            onOpenChange={setTokenOpen}
          />
        </div>
      )}
    </div>
  )
}
