'use client'

import React from 'react'
import { AlertTriangle, Info, RefreshCw } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/confirm-dialog'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

export const REDEPLOY_STEPS = [
  'Checks the live edge first: re-uploads the Wasm only if the build changed',
  'Re-activates the logging VCL only if the scoring snippets/backend changed',
  'Skips no-op version bumps when nothing changed — no scoring gap',
]

export const DISABLE_STEPS = [
  'Removes the 6 VCL snippets (recv / pass / fetch / deliver / miss / enforce) that route requests through the scorer',
  'Removes the 6 scoring custom_fields from the log format',
  'Clones + activates a new VCL version on the customer service',
  'Deletes the scorer Compute service + its ConfigStores and matrix KV Store (a future Enable re-provisions them from scratch)',
]

export const ENABLE_STEPS: React.ReactNode[] = [
  "Provisions the scorer Compute service if it doesn't exist, deploys the trained Wasm scorer with the latest matrix",
  <>
    Adds the <code>session_scorer</code> backend, 6 VCL snippets, and 9 score fields (
    <code>edge_score</code>, session id, reason, L1/L2, latency, …) to the customer service
  </>,
  'Clones + activates a new VCL version',
  'Every request will be routed through the scorer for an L1+L2 score on the response cookie',
]

export function StepListDescription({
  intro,
  serviceId,
  steps,
  header,
  footer,
}: {
  intro: string
  serviceId: string
  steps: React.ReactNode[]
  header?: React.ReactNode
  footer?: React.ReactNode
}) {
  return (
    <div className="space-y-2">
      {header}
      <p>
        {intro} <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>:
      </p>
      <ul className="text-sm text-muted-foreground list-disc pl-5 space-y-1">
        {steps.map((s) => (
          <li key={typeof s === 'string' ? s : String(steps.indexOf(s))}>{s}</li>
        ))}
      </ul>
      {footer}
      <p className="text-xs text-muted-foreground italic">
        Click <strong>Start</strong> to proceed.
      </p>
    </div>
  )
}

export function CalloutBanner({
  tone = 'amber',
  children,
  testId,
}: {
  tone?: 'amber' | 'sky' | 'muted'
  children: React.ReactNode
  testId?: string
}) {
  const Icon = tone === 'amber' ? AlertTriangle : Info
  const cls =
    tone === 'amber'
      ? 'border-amber-300 bg-amber-50 text-amber-900'
      : tone === 'sky'
        ? 'border-sky-300 bg-sky-50 text-sky-900'
        : 'bg-muted/40 text-muted-foreground'
  return (
    <div className={`flex items-start gap-2 rounded border px-2.5 py-1.5 ${cls}`} data-testid={testId}>
      <Icon className="h-3.5 w-3.5 mt-0.5 shrink-0" />
      <div className="text-xs">{children}</div>
    </div>
  )
}

export function PragmaMissingDialog({
  serviceId,
  open,
  onOpenChange,
  isFetching,
  onRecheck,
}: {
  serviceId: string
  open: boolean
  onOpenChange: (open: boolean) => void
  isFetching: boolean
  onRecheck: () => void
}) {
  const accountTeamMessage = `Hi Fastly team — please enable the preflight request body preservation service pragmas on Fastly service ${serviceId} (preserving unsent request bodies across VCL restarts and omitting request bodies when backend requests are rewritten to GET or HEAD) so we can enable our Session Scoring Compute preflight pass.`

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg" data-testid="admin-session-scoring-pragma-modal">
        <DialogHeader>
          <DialogTitle>Preflight Request Body Preservation Required</DialogTitle>
          <DialogDescription>
            Before enabling the Session Scoring Compute scorer on{' '}
            <code className="text-xs bg-muted px-1 rounded">{serviceId}</code>, your Fastly account
            team must enable preflight request body preservation on this service.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3 py-2 text-sm">
          <div className="flex items-start gap-2 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900">
            <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
            <div className="text-xs space-y-1">
              <div className="font-semibold">Why is this required?</div>
              <div>
                Session Scoring routes each request through a lightweight Compute preflight
                sub-request (rewritten to <code>GET</code>) and then restarts VCL to forward the
                original request to your origin. Without service-level preflight body preservation
                enabled, <code>POST</code>/<code>PUT</code>/<code>PATCH</code> request bodies can
                be drained or stall when forwarding to origin after the restart.
              </div>
            </div>
          </div>
          <div className="space-y-1.5">
            <div className="text-xs font-medium text-muted-foreground">
              Send this request to your Fastly account team:
            </div>
            <pre
              className="rounded border bg-muted/60 p-3 text-xs whitespace-pre-wrap break-words font-mono"
              data-testid="admin-session-scoring-pragma-message"
            >
              {accountTeamMessage}
            </pre>
          </div>
        </div>
        <DialogFooter className="gap-2 sm:justify-between">
          <CopyButton
            text={accountTeamMessage}
            label="Copy Message"
            copiedLabel="Copied"
            variant="outline"
            size="sm"
            className=""
            iconClassName="h-3.5 w-3.5"
            data-testid="admin-session-scoring-pragma-copy-btn"
          />
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={onRecheck}
              disabled={isFetching}
              data-testid="admin-session-scoring-pragma-recheck-btn"
            >
              <RefreshCw className={`h-3.5 w-3.5 mr-1 ${isFetching ? 'animate-spin' : ''}`} />{' '}
              Check Again
            </Button>
            <Button
              type="button"
              variant="default"
              size="sm"
              onClick={() => onOpenChange(false)}
            >
              Close
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export function ScoringVclTab({ serviceId }: { serviceId: string }) {
  return (
    <div className="space-y-4">
      <StepListDescription
        intro="VCL & Compute deployment lifecycle for"
        serviceId={serviceId}
        steps={ENABLE_STEPS}
      />
    </div>
  )
}
