'use client'

import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { Checkbox } from '@/components/ui/checkbox'
import { CmcdConfigSection } from '@/components/CmcdConfigSection'
import { TokenConfigSection, tokenConfigError } from '@/components/TokenConfigSection'

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
  contentIdExpr: string
  onContentIdExprChange: (expr: string) => void
  /** Enables live VCL lint of the token expressions (omit before provisioning). */
  serviceId?: string
  disabled?: boolean
}

/**
 * STREAMING group: two independently toggleable sub-options, CMCD Metrics and
 * Token, each deploying its own VCL. The group checkbox shows their combined
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
    // CMCD works with its defaults; Token can't deploy without an id expression.
    if (tokenConfigError(props.subscriberIdExpr, props.contentIdExpr) !== null) {
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
          <TokenConfigSection
            enabled={props.tokenEnabled}
            onEnabledChange={props.onTokenEnabledChange}
            subscriberIdExpr={props.subscriberIdExpr}
            onSubscriberIdExprChange={props.onSubscriberIdExprChange}
            contentIdExpr={props.contentIdExpr}
            onContentIdExprChange={props.onContentIdExprChange}
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
