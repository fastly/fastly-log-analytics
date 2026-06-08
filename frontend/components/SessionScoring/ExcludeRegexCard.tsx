'use client'

import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, Check, Filter, Loader2, RotateCcw } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'
import { client } from '@/lib/api'

interface ExcludeRegexResponse {
  current: string
  is_default: boolean
  default: string
  effective: string
}

interface ExcludeRegexCardProps {
  serviceId: string
}

/**
 * Operator-facing control for "which URLs bypass the scoring Compute call".
 *
 * The backend uses the result as the regex on the right-hand side of
 * `std.tolower(req.url) !~ "<regex>"` in the scoring recv VCL snippet —
 * a match means the request is NOT routed to Compute (saves cost on
 * static assets / health checks / etc.).
 *
 * The default matches common static-asset file extensions; the operator
 * can override per-service. The override is validated through three
 * layers (input policy + falco static analysis + Fastly VCL compile)
 * before the cloned version is activated.
 */
// Cached on-blur lint result. `regex` records the last value we asked the
// backend about so we can skip the round-trip if the operator blurs out
// after clicking right back to the same content; `error` / `lint_warnings`
// hold the verdict to render inline.
type LintResult =
  | { regex: string; ok: true; lint_warnings: string[] }
  | { regex: string; ok: false; error: string; reason: string }

export function ExcludeRegexCard({ serviceId }: ExcludeRegexCardProps) {
  const queryClient = useQueryClient()
  const [draft, setDraft] = React.useState('')
  const [confirmOpen, setConfirmOpen] = React.useState(false)
  const [lintResult, setLintResult] = React.useState<LintResult | null>(null)
  const [lintPending, setLintPending] = React.useState(false)

  const { data, isLoading } = useQuery({
    queryKey: ['scoring-exclude-regex', serviceId],
    queryFn: async () => {
      const { data, response } = await client.GET(
        '/api/services/{service_id}/scoring/exclude-regex' as any,
        { params: { path: { service_id: serviceId } } } as any,
      )
      if (!response.ok) throw new Error(`status ${response.status}`)
      return data as ExcludeRegexResponse
    },
    staleTime: 60_000,
  })

  // Initialise the draft from the stored value once the fetch completes.
  // ``enable_scoring`` populates cfg.scoring.exclude_url_regex with the
  // literal default on first turn-on, so `data.current` is always a real
  // regex (never empty). Fall back to `data.effective` for legacy services
  // enabled before that change landed (their cfg still has null). Don't
  // reset on re-renders — the operator may have been typing.
  const initialisedRef = React.useRef(false)
  React.useEffect(() => {
    if (data && !initialisedRef.current) {
      setDraft(data.current || data.effective)
      initialisedRef.current = true
    }
  }, [data])

  const isDirty = data ? draft !== (data.current || data.effective) : false

  // "Reset & publish" vs "Save & publish" button label: the operator is
  // resetting iff the draft matches the bundled default AND the current
  // stored value is something else (a custom override). After publish
  // the cfg lands back at the default literal — semantically a reset.
  const isResetToDefault = data ? draft === data.default && !data.is_default : false

  const saveMut = useMutation({
    mutationFn: async (regex: string) => {
      // Omit the ``token`` query param entirely — the backend's
      // ``_resolve_token`` falls back to the cfg-stored ``fastly_api_key``
      // when one isn't supplied, which is the same pattern the
      // enforce-threshold + enforce-status-code endpoints rely on. The
      // operator only needed to type a token here if they wanted to
      // override the stored key, which is almost never the case in
      // practice. If the operator does need to override later, expose a
      // collapsible "advanced" affordance — don't make every edit prompt.
      const { data: resp, response } = await client.PUT(
        '/api/services/{service_id}/scoring/exclude-regex' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { confirm: true },
          },
          body: { regex } as any,
        } as any,
      )
      if (!response.ok) {
        // Surface backend's structured detail.error to the toast/alert.
        const err = (resp as any)?.detail?.error || (resp as any)?.detail || `HTTP ${response.status}`
        throw new Error(typeof err === 'string' ? err : JSON.stringify(err))
      }
      return resp as { ok: true; effective_regex: string; is_default: boolean; message: string; lint_warnings: string[] }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scoring-exclude-regex', serviceId] })
      setConfirmOpen(false)
    },
  })

  const handleSave = () => {
    saveMut.mutate(draft)
  }

  const handleResetToDefault = () => {
    if (data) setDraft(data.default)
  }

  // Pre-publish dry-run lint: fires on textarea blur if the draft has
  // changed since the last check. Hits the dedicated
  // ``/scoring/exclude-regex/validate`` endpoint which runs the same
  // input-policy + falco static-analysis layers the publish flow uses,
  // but without touching cfg or Fastly. Gives the operator immediate
  // feedback BEFORE they commit to the save+publish round-trip.
  const runLintCheck = React.useCallback(
    async (regex: string) => {
      // Skip when nothing's there to lint or we've already validated this
      // exact string — avoids spamming falco when the operator clicks
      // around the card.
      if (regex.trim() === '') {
        setLintResult(null)
        return
      }
      if (lintResult && lintResult.regex === regex) return
      setLintPending(true)
      try {
        const { data: resp, response } = await client.POST(
          '/api/services/{service_id}/scoring/exclude-regex/validate' as any,
          {
            params: { path: { service_id: serviceId } },
            body: { regex } as any,
          } as any,
        )
        if (!response.ok) {
          const err =
            (resp as any)?.detail?.error || (resp as any)?.detail || `HTTP ${response.status}`
          setLintResult({
            regex,
            ok: false,
            error: typeof err === 'string' ? err : JSON.stringify(err),
            reason: 'http_error',
          })
          return
        }
        const r = resp as
          | { ok: true; lint_warnings: string[] }
          | { ok: false; error: string; reason: string }
        if (r.ok) {
          setLintResult({ regex, ok: true, lint_warnings: r.lint_warnings || [] })
        } else {
          setLintResult({ regex, ok: false, error: r.error, reason: r.reason })
        }
      } catch (e) {
        setLintResult({
          regex,
          ok: false,
          error: e instanceof Error ? e.message : String(e),
          reason: 'network_error',
        })
      } finally {
        setLintPending(false)
      }
    },
    [serviceId, lintResult],
  )

  // Clear the cached lint verdict whenever the operator edits the draft —
  // stale "passed" indicators would mislead about the CURRENT value.
  const handleDraftChange = (v: string) => {
    setDraft(v)
    if (lintResult && lintResult.regex !== v) setLintResult(null)
  }

  if (isLoading) {
    return (
      <AnalyticsCard title="URL exclusion regex" icon={<Filter className="h-4 w-4" />}>
        <Skeleton className="h-32 w-full" />
      </AnalyticsCard>
    )
  }

  if (!data) {
    return (
      <AnalyticsCard title="URL exclusion regex" icon={<Filter className="h-4 w-4" />}>
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>Failed to load current exclusion regex.</AlertDescription>
        </Alert>
      </AnalyticsCard>
    )
  }

  return (
    <AnalyticsCard
      title="URL exclusion regex"
      icon={<Filter className="h-4 w-4" />}
      description="Requests whose URL matches this regex are NOT sent to the scoring Compute service. The default skips common static-asset extensions; override to scope scoring to specific traffic patterns."
    >
      <div className="space-y-4">
        <div className="space-y-1.5">
          <Label htmlFor="exclude-regex-textarea" className="text-xs font-semibold">
            Exclusion regex{' '}
            <span className="text-muted-foreground">
              ({data.is_default ? 'currently default' : 'currently custom override'})
            </span>
          </Label>
          <Textarea
            id="exclude-regex-textarea"
            value={draft}
            onChange={(e) => handleDraftChange(e.target.value)}
            onBlur={(e) => runLintCheck(e.target.value)}
            className="font-mono text-xs min-h-24"
            spellCheck={false}
            autoComplete="off"
          />
          <p className="text-[10px] text-muted-foreground">
            VCL pattern compiled by Fastly's regex engine (RE2). No quotes / control chars; max 2 KB.
            Hit <span className="font-semibold">Reset to default</span> to drop any custom override.
            Lint runs on blur and on publish.
          </p>
        </div>

        {/* On-blur falco/input-policy lint verdict — appears as soon as the
            operator tabs out of the textarea, BEFORE they commit to publish. */}
        {lintPending && (
          <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" />
            Linting regex…
          </div>
        )}
        {!lintPending && lintResult && lintResult.regex === draft && !lintResult.ok && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-xs">
              <strong>Lint error ({lintResult.reason}):</strong>
              <div className="font-mono whitespace-pre-wrap mt-1">{lintResult.error}</div>
            </AlertDescription>
          </Alert>
        )}
        {!lintPending &&
          lintResult &&
          lintResult.regex === draft &&
          lintResult.ok &&
          lintResult.lint_warnings.length > 0 && (
            <Alert>
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription className="text-xs">
                <strong>Lint warnings (non-blocking):</strong>
                <ul className="list-disc ml-4 mt-1">
                  {lintResult.lint_warnings.slice(0, 5).map((w, i) => (
                    <li key={i} className="font-mono text-[10px]">
                      {w}
                    </li>
                  ))}
                </ul>
              </AlertDescription>
            </Alert>
          )}
        {!lintPending &&
          lintResult &&
          lintResult.regex === draft &&
          lintResult.ok &&
          lintResult.lint_warnings.length === 0 &&
          isDirty && (
            <div className="flex items-center gap-2 text-[11px] text-emerald-700">
              <Check className="h-3 w-3" />
              Lint clean — ready to publish.
            </div>
          )}

        {saveMut.isError && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-xs whitespace-pre-wrap font-mono">
              {(saveMut.error as Error).message}
            </AlertDescription>
          </Alert>
        )}

        {saveMut.isSuccess && (
          <Alert>
            <Check className="h-4 w-4" />
            <AlertDescription className="text-xs">
              {saveMut.data.message}
              {saveMut.data.lint_warnings && saveMut.data.lint_warnings.length > 0 && (
                <div className="mt-2">
                  <strong>Lint warnings (non-blocking):</strong>
                  <ul className="list-disc ml-4 mt-1">
                    {saveMut.data.lint_warnings.slice(0, 5).map((w, i) => (
                      <li key={i} className="font-mono text-[10px]">{w}</li>
                    ))}
                  </ul>
                </div>
              )}
            </AlertDescription>
          </Alert>
        )}

        <div className="flex items-center justify-end gap-2 pt-2 border-t">
          <Button
            variant="outline"
            size="sm"
            onClick={handleResetToDefault}
            disabled={data.is_default && draft === ''}
            className="text-xs"
          >
            <RotateCcw className="h-3 w-3 mr-1" />
            Reset to default
          </Button>
          <Button
            size="sm"
            onClick={() => setConfirmOpen(true)}
            disabled={!isDirty || saveMut.isPending}
            className="text-xs"
          >
            {saveMut.isPending ? (
              <>
                <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                Publishing…
              </>
            ) : isResetToDefault ? (
              'Reset & publish'
            ) : (
              'Save & publish'
            )}
          </Button>
        </div>
      </div>

      <ConfirmDialog
        open={confirmOpen}
        onOpenChange={setConfirmOpen}
        title="Publish updated scoring exclusion regex?"
        description={
          isResetToDefault
            ? 'This will revert this service to the built-in default static-asset regex and activate a new VCL version on the Fastly service. Effective at the edge after activation propagates (~5-15s).'
            : "This will clone the active Fastly version, swap the scoring recv snippet to use your custom regex, validate via falco + Fastly's compiler, and activate. Effective at the edge after activation propagates (~5-15s). A bad regex could disable scoring entirely or DoS Compute — double-check the value above."
        }
        confirmLabel="Publish"
        isDangerous
        isPending={saveMut.isPending}
        onConfirm={handleSave}
      />
    </AnalyticsCard>
  )
}
