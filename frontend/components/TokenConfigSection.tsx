'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { AlertTriangle, CheckCircle2, ChevronDown, ChevronRight, Loader2 } from 'lucide-react'
import { useDebounce } from '@/hooks/useDebounce'
import { customFieldsApi } from '@/lib/api/custom-fields'

/** Headers the generated VCL sets; promoted into the `cs_sub_id` / `cs_content_id` log fields. */
export const SUBSCRIBER_ID_HEADER = 'req.http.x-cs-sub-id'
export const CONTENT_ID_HEADER = 'req.http.x-cs-content-id'

/**
 * Client-side mirror of the backend's expression guard (backend/core/log_fields.py
 * validate_custom_field) for ONE non-empty expression. The backend re-validates;
 * this only gates the Save button. Empty is allowed per id — see tokenConfigError.
 */
export function tokenExprError(expr: string): string | null {
  const v = expr.trim()
  if (!v) return null
  if (v.length > 512) return 'Must be 512 characters or fewer.'
  if (/[\r\n]/.test(expr)) return 'Must not contain newlines.'
  if (v.includes(';')) return 'Must not contain semicolons (;).'
  if (v.includes('//') || v.includes('/*') || v.includes('#')) return 'Must not contain VCL comments (//, /*, or #).'
  return null
}

/** Whole-section check: at least one id configured, and each configured id safe. */
export function tokenConfigError(subscriberIdExpr: string, contentIdExpr: string): string | null {
  if (!subscriberIdExpr.trim() && !contentIdExpr.trim()) return 'Configure at least one of Subscriber ID or Content ID.'
  const sub = tokenExprError(subscriberIdExpr)
  if (sub) return `Subscriber ID: ${sub}`
  const cid = tokenExprError(contentIdExpr)
  return cid ? `Content ID: ${cid}` : null
}

function TokenExprRow({
  id,
  label,
  header,
  value,
  onChange,
  placeholder,
  enabled,
  disabled,
  serviceId,
}: {
  id: string
  label: string
  header: string
  value: string
  onChange: (expr: string) => void
  placeholder: string
  enabled: boolean
  disabled?: boolean
  serviceId?: string
}) {
  const debounced = useDebounce(value, 500)
  const localError = enabled ? tokenExprError(value) : null
  const canLint = !!serviceId && enabled && !!debounced.trim() && tokenExprError(debounced) === null

  // Advisory server lint against the service's log format; the backend
  // re-validates on save, so a failed lint call just shows nothing.
  const lint = useQuery({
    queryKey: ['token-expr-lint', serviceId, debounced],
    queryFn: () => customFieldsApi.validateCustomVcl(serviceId!, { vcl_log_expression: debounced, collection_stage: 'edge' }),
    enabled: canLint,
    retry: false,
    staleTime: 60_000,
  })
  const lintResult = canLint && !lint.isError ? (lint.data ?? null) : null
  const isLinting = canLint && lint.isFetching

  return (
    <div className="space-y-1.5">
      <div className="grid grid-cols-1 sm:grid-cols-[9rem_1fr] gap-x-4 gap-y-1.5 items-start">
        <Label htmlFor={id} className="text-[11px] font-medium sm:pt-2">
          {label}
        </Label>
        <div className="bg-muted p-3 rounded-md font-mono text-xs border min-w-0">
          <div className="text-muted-foreground mb-2 break-all">
            <code>set {header} =</code>
          </div>
          <Input
            id={id}
            aria-label={`${label} VCL expression`}
            aria-invalid={!!localError || (lintResult ? !lintResult.valid : false)}
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            className="font-mono bg-background"
            disabled={disabled || !enabled}
          />
        </div>
      </div>
      <div className="sm:pl-[10rem]">
        {localError ? (
          <p className="text-[11px] text-destructive flex items-center gap-1.5" role="alert">
            <AlertTriangle className="h-3.5 w-3.5" /> {localError}
          </p>
        ) : isLinting ? (
          <p className="text-[11px] text-muted-foreground flex items-center gap-1.5">
            <Loader2 className="h-3 w-3 animate-spin" /> Validating VCL...
          </p>
        ) : lintResult && !lintResult.valid ? (
          <ul className="text-[11px] text-destructive list-disc pl-5 space-y-0.5" role="alert">
            {lintResult.errors.map((err) => <li key={err}>{err}</li>)}
          </ul>
        ) : lintResult?.valid ? (
          <p className="text-[11px] text-emerald-600 dark:text-emerald-500 flex items-center gap-1.5">
            <CheckCircle2 className="h-3.5 w-3.5" /> Expression looks valid.
          </p>
        ) : null}
      </div>
    </div>
  )
}

interface TokenConfigSectionProps {
  enabled: boolean
  onEnabledChange: (enabled: boolean) => void
  subscriberIdExpr: string
  onSubscriberIdExprChange: (expr: string) => void
  contentIdExpr: string
  onContentIdExprChange: (expr: string) => void
  /** When set, each expression is linted against this service's log format. */
  serviceId?: string
  disabled?: boolean
}

export function TokenConfigSection({
  enabled,
  onEnabledChange,
  subscriberIdExpr,
  onSubscriberIdExprChange,
  contentIdExpr,
  onContentIdExprChange,
  serviceId,
  disabled,
}: TokenConfigSectionProps) {
  const [isOpen, setIsOpen] = useState(false)
  const noneConfigured = enabled && !subscriberIdExpr.trim() && !contentIdExpr.trim()

  return (
    <div className="border border-border/60 rounded-lg overflow-hidden bg-card/50" data-testid="token-config-section">
      <div className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left">
        <div className="flex items-center gap-3">
          <Checkbox
            checked={enabled}
            onCheckedChange={(checked) => onEnabledChange(checked as boolean)}
            disabled={disabled}
            className="mr-1"
            aria-label="Enable Token"
          />
          <button
            type="button"
            onClick={() => setIsOpen(!isOpen)}
            aria-expanded={isOpen}
            className="flex items-center gap-2 text-left cursor-pointer bg-transparent border-0 p-0"
          >
            <h4 className="text-xs font-bold tracking-tight uppercase text-foreground/80">Token</h4>
            <span className="text-[10px] text-muted-foreground ml-1">+~40 bytes per ID</span>
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
        <div className="p-4 pt-2 border-t border-border/40 bg-card space-y-3">
          <p className="text-[11px] text-muted-foreground leading-relaxed">
            Tokens can be provided in HMAC, JWT, or CAT format. Extract the information into headers and it can be used in analysis.
            Configure one or both IDs; an ID left blank is not logged.
            <span className="block mt-1.5 text-amber-600 dark:text-amber-500 font-medium italic">
              ⚠ Enabling this option deploys a VCL snippet to your service to extract token information from HTTP headers.
            </span>
          </p>

          <TokenExprRow
            id="token-subscriber-id-expr"
            label="Subscriber ID"
            header={SUBSCRIBER_ID_HEADER}
            value={subscriberIdExpr}
            onChange={onSubscriberIdExprChange}
            placeholder='regsub(req.http.Authorization, "^Bearer ", "")'
            enabled={enabled}
            disabled={disabled}
            serviceId={serviceId}
          />
          <TokenExprRow
            id="token-content-id-expr"
            label="Content ID"
            header={CONTENT_ID_HEADER}
            value={contentIdExpr}
            onChange={onContentIdExprChange}
            placeholder='subfield(req.http.CAT, "cid", ",")'
            enabled={enabled}
            disabled={disabled}
            serviceId={serviceId}
          />

          {noneConfigured && (
            <p className="text-[11px] text-destructive flex items-center gap-1.5" role="alert">
              <AlertTriangle className="h-3.5 w-3.5" /> Configure at least one of Subscriber ID or Content ID.
            </p>
          )}
        </div>
      )}
    </div>
  )
}
