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
 * this only gates the Save button. Empty is allowed per id (it just means that id is off).
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

/** The three independently-selectable Content Security options. */
export interface ContentSecurityOptions {
  subscriberIdEnabled: boolean
  subscriberIdExpr: string
  contentIdEnabled: boolean
  contentIdExpr: string
  enforcementEnabled: boolean
}

/** A checked id needs an expression, and every checked expression must be safe. */
export function contentSecurityConfigError(o: ContentSecurityOptions): string | null {
  for (const [label, on, expr] of [
    ['Subscriber ID', o.subscriberIdEnabled, o.subscriberIdExpr],
    ['Content ID', o.contentIdEnabled, o.contentIdExpr],
  ] as const) {
    if (!on) continue
    if (!expr.trim()) return `${label}: enter a VCL expression or uncheck it.`
    const err = tokenExprError(expr)
    if (err) return `${label}: ${err}`
  }
  return null
}

/** What the backend is sent: an unchecked id goes as an empty expression. */
export function contentSecurityPayload(master: boolean, o: ContentSecurityOptions) {
  const subscriber = master && o.subscriberIdEnabled ? o.subscriberIdExpr.trim() : ''
  const content = master && o.contentIdEnabled ? o.contentIdExpr.trim() : ''
  const enforcement = master && o.enforcementEnabled
  return { enabled: !!subscriber || !!content || enforcement, subscriber, content, enforcement }
}

function TokenExprRow({
  id,
  label,
  header,
  value,
  onChange,
  placeholder,
  checked,
  onCheckedChange,
  disabled,
  serviceId,
}: {
  id: string
  label: string
  header: string
  value: string
  onChange: (expr: string) => void
  placeholder: string
  checked: boolean
  onCheckedChange: (checked: boolean) => void
  disabled?: boolean
  serviceId?: string
}) {
  const debounced = useDebounce(value, 500)
  const enabled = checked && !disabled
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
        <div className="flex items-center gap-2 sm:pt-2">
          <Checkbox
            id={`${id}-enabled`}
            checked={checked}
            onCheckedChange={(c) => onCheckedChange(c as boolean)}
            disabled={disabled}
            aria-label={`Enable ${label}`}
          />
          <Label htmlFor={id} className="text-[11px] font-medium">
            {label}
          </Label>
        </div>
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
            disabled={!enabled}
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

interface ContentSecurityConfigSectionProps {
  /** Master switch: checking it checks all three options; unchecking it unchecks them. */
  enabled: boolean
  onEnabledChange: (enabled: boolean) => void
  subscriberIdEnabled: boolean
  onSubscriberIdEnabledChange: (enabled: boolean) => void
  subscriberIdExpr: string
  onSubscriberIdExprChange: (expr: string) => void
  contentIdEnabled: boolean
  onContentIdEnabledChange: (enabled: boolean) => void
  contentIdExpr: string
  onContentIdExprChange: (expr: string) => void
  /** Log the block / throttle / rate-limit outcome the operator's VCL sets. */
  enforcementEnabled: boolean
  onEnforcementEnabledChange: (enabled: boolean) => void
  /** When set, each expression is linted against this service's log format. */
  serviceId?: string
  disabled?: boolean
  open?: boolean
  onOpenChange?: (open: boolean) => void
}

/**
 * CONTENT SECURITY: Subscriber ID, Content ID and the enforcement log, each with its own
 * checkbox. Checking the section checks all three; any can then be unchecked.
 */
export function ContentSecurityConfigSection({
  enabled,
  onEnabledChange,
  subscriberIdEnabled,
  onSubscriberIdEnabledChange,
  subscriberIdExpr,
  onSubscriberIdExprChange,
  contentIdEnabled,
  onContentIdEnabledChange,
  contentIdExpr,
  onContentIdExprChange,
  enforcementEnabled,
  onEnforcementEnabledChange,
  serviceId,
  disabled,
  open,
  onOpenChange,
}: ContentSecurityConfigSectionProps) {
  const [innerOpen, setInnerOpen] = useState(false)
  // Controlled when the parent passes `open` (Streaming opens children that need input).
  const isOpen = open ?? innerOpen
  const setIsOpen = (next: boolean) => (onOpenChange ? onOpenChange(next) : setInnerOpen(next))
  const error = enabled
    ? contentSecurityConfigError({ subscriberIdEnabled, subscriberIdExpr, contentIdEnabled, contentIdExpr, enforcementEnabled })
    : null

  const toggleAll = (checked: boolean) => {
    onEnabledChange(checked)
    onSubscriberIdEnabledChange(checked)
    onContentIdEnabledChange(checked)
    onEnforcementEnabledChange(checked)
  }

  return (
    <div className="border border-border/60 rounded-lg overflow-hidden bg-card/50" data-testid="content-security-config-section">
      <div className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left">
        <div className="flex items-center gap-3">
          <Checkbox
            checked={enabled}
            onCheckedChange={(checked) => toggleAll(checked as boolean)}
            disabled={disabled}
            className="mr-1"
            aria-label="Enable Content Security"
          />
          <button
            type="button"
            onClick={() => setIsOpen(!isOpen)}
            aria-expanded={isOpen}
            className="flex items-center gap-2 text-left cursor-pointer bg-transparent border-0 p-0"
          >
            <h4 className="text-xs font-bold tracking-tight uppercase text-foreground/80">Content Security</h4>
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
            Subscriber and content IDs can come from HMAC, JWT, or CAT tokens: extract each into a header to use it in
            analysis. Each option is enabled on its own.
            <span className="block mt-1.5 text-amber-600 dark:text-amber-500 font-medium italic">
              ⚠ Enabling an ID deploys a VCL snippet to your service to extract it from HTTP headers.
            </span>
          </p>

          <TokenExprRow
            id="token-subscriber-id-expr"
            label="Subscriber ID"
            header={SUBSCRIBER_ID_HEADER}
            value={subscriberIdExpr}
            onChange={onSubscriberIdExprChange}
            placeholder='regsub(req.http.Authorization, "^Bearer ", "")'
            checked={subscriberIdEnabled}
            onCheckedChange={onSubscriberIdEnabledChange}
            disabled={disabled || !enabled}
            serviceId={serviceId}
          />
          <TokenExprRow
            id="token-content-id-expr"
            label="Content ID"
            header={CONTENT_ID_HEADER}
            value={contentIdExpr}
            onChange={onContentIdExprChange}
            placeholder='subfield(req.http.CAT, "cid", ",")'
            checked={contentIdEnabled}
            onCheckedChange={onContentIdEnabledChange}
            disabled={disabled || !enabled}
            serviceId={serviceId}
          />

          <div className="flex items-start gap-2 pt-1">
            <Checkbox
              id="content-security-enforcement"
              checked={enforcementEnabled}
              onCheckedChange={(checked) => onEnforcementEnabledChange(checked as boolean)}
              disabled={disabled || !enabled}
              aria-describedby="content-security-enforcement-desc"
              className="mt-0.5"
            />
            <div className="text-[11px] leading-relaxed">
              <label htmlFor="content-security-enforcement" className="font-medium text-foreground/80 cursor-pointer">
                Log enforcement outcome
              </label>
              <span id="content-security-enforcement-desc" className="text-muted-foreground">
                {' '}
                (+~60 bytes) Record whether each request was allowed, blocked, throttled or rate limited, as set by your
                service&apos;s own VCL in <code>req.http.cs-status</code>, <code>cs-mode</code>, <code>cs-rl-mode</code>{' '}
                and <code>cs-pace-kbps</code>. Feeds the Enforcement section of Content Security.
              </span>
            </div>
          </div>

          {error && (
            <p className="text-[11px] text-destructive flex items-center gap-1.5" role="alert">
              <AlertTriangle className="h-3.5 w-3.5" /> {error}
            </p>
          )}
        </div>
      )}
    </div>
  )
}
