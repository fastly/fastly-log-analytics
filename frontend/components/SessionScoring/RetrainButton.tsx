'use client'

import * as React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Brain, KeyRound, Loader2 } from 'lucide-react'

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { CopyButton } from '@/components/ui/copy-button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useAutoFlash } from '@/hooks/useAutoFlash'
import { client, extractApiError } from '@/lib/api'
import type { components } from '@/types/api.generated'

import { formatCount } from '@/lib/format'
import { useOpenGeneration } from '@/hooks/useDerivedState'
import { invalidateServiceScoringQueries, scoringConfigCompositeKey } from './useScorerTimeseries'
type RetrainResult = components['schemas']['ScoringRetrainResponse']
type RotateKeyResponse = components['schemas']['ScoringRotateKeyResponse']

interface RetrainButtonProps {
  serviceId: string
}

/**
 * Trigger a server-side matrix retrain.
 *
 * The pipeline extracts sessions from the last N days of DuckDB traffic,
 * builds a fresh TransitionMatrix, evaluates AUC against the operator's
 * accumulated labels, persists matrix.json locally + publishes to FOS,
 * and reports the result. The actual Wasm rebuild + Compute deploy stays
 * manual (requires Fastly CLI + Rust toolchain on the admin box) — the
 * dialog surfaces the deploy command for the operator to copy.
 */
export function RetrainButton({ serviceId }: RetrainButtonProps) {
  const [open, setOpen] = React.useState(false)
  const generation = useOpenGeneration(open)

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="outline" size="sm">
            <Brain className="h-4 w-4 mr-1" />
            Retrain matrix
          </Button>
        }
      />
      <RetrainDialogBody key={generation} serviceId={serviceId} onClose={() => setOpen(false)} />
    </Dialog>
  )
}

function RetrainDialogBody({ serviceId, onClose }: RetrainButtonProps & { onClose: () => void }) {
  const queryClient = useQueryClient()
  const [sinceDays, setSinceDays] = React.useState(7)
  const [result, setResult] = React.useState<RetrainResult | null>(null)

  const mutation = useMutation({
    mutationFn: async (): Promise<RetrainResult> => {
      const { data, error: errBody, response } = await client.POST(
        '/api/services/{service_id}/scoring/retrain',
        {
          params: {
            path: { service_id: serviceId },
            query: { since_days: sinceDays },
          },
        },
      )
      if (!response.ok || !data) {
        throw new Error(errBody ? extractApiError(errBody) : `status ${response.status}`)
      }
      return data
    },
    onSuccess: (data) => {
      setResult(data)
      // Refresh the AUC field + health card after retrain.
      invalidateServiceScoringQueries(queryClient, serviceId)
    },
  })

  return (
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Brain className="h-4 w-4" />
            Retrain scoring matrix
          </DialogTitle>
          <DialogDescription>
            Builds a fresh transition matrix from the last N days of DuckDB traffic and
            evaluates AUC against your labels. Saves matrix.json + publishes to FOS so
            every backend host sees the new matrix. The Wasm rebuild + Compute deploy
            is a separate step (requires Fastly CLI on your local box).
          </DialogDescription>
        </DialogHeader>

        {!result && (
          <div className="space-y-4 py-2">
            <div className="space-y-1.5">
              <Label htmlFor="since-days" className="text-xs">Training window (days)</Label>
              <Input
                id="since-days"
                type="number"
                min={1}
                max={90}
                value={sinceDays}
                onChange={(e) => setSinceDays(Number(e.target.value) || 7)}
                className="text-sm"
                disabled={mutation.isPending}
              />
              <p className="text-[11px] text-muted-foreground">
                7 days is the default; bump to 30 once you have steady traffic. Larger
                windows take longer but build a more stable matrix.
              </p>
            </div>
            {mutation.isError && (
              <p className="text-sm text-destructive">{(mutation.error as Error).message}</p>
            )}
          </div>
        )}

        {result && (
          <div className="space-y-3 py-2 text-sm">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              <Metric label="Matrix version" value={result.matrix_version} mono />
              <Metric label="Sessions trained" value={formatCount(result.sessions_trained_on ?? 0)} />
              <Metric label="Transitions" value={formatCount(result.transitions ?? 0)} />
              <Metric label="Vocab size" value={formatCount(result.vocab_size ?? 0)} />
            </div>
            {result.rejected && (
              <p className="text-xs text-muted-foreground">
                Kept {formatCount(result.rejected.kept ?? 0)} sessions ·
                {' '}dropped {formatCount(result.rejected.too_few_events ?? 0)} too-short,
                {' '}{formatCount(result.rejected.too_fast ?? 0)} too-fast ·
                {' '}{formatCount(result.rejected.routes_seen ?? 0)} routes seen
              </p>
            )}
            {result.auc_against_labels ? (
              <div className="p-3 border rounded-md bg-muted/40">
                <div className="text-xs uppercase text-muted-foreground mb-1">
                  AUC against your labels
                </div>
                <div className="flex items-baseline gap-2">
                  <span
                    className={`text-2xl font-mono font-semibold tabular-nums ${
                      result.auc_against_labels.passed ? 'text-emerald-600' : 'text-amber-600'
                    }`}
                  >
                    {(result.auc_against_labels.auc ?? 0).toFixed(3)}
                  </span>
                  <span className="text-sm text-muted-foreground">
                    {result.auc_against_labels.passed ? 'PASS' : 'BELOW THRESHOLD'} (threshold{' '}
                    {(result.auc_against_labels.threshold ?? 0).toFixed(2)})
                  </span>
                </div>
                <p className="text-[11px] text-muted-foreground mt-1">
                  n={result.auc_against_labels.n_good} good /{' '}
                  {result.auc_against_labels.n_bad} bad — minimum {result.default_min_auc} to pass.
                </p>
              </div>
            ) : (
              <p className="text-xs text-muted-foreground italic">
                AUC not computed — need at least 3 good + 3 bad labels to evaluate.
              </p>
            )}
            <div className="space-y-1 text-[11px] text-muted-foreground">
              <p>{result.local_matrix_saved ? '✓' : '✗'} matrix.json saved locally</p>
              <p>{result.fos_matrix_published ? '✓' : '✗'} matrix published to FOS</p>
            </div>
            {result.deploy_hint && (
              <details className="text-xs">
                <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
                  Deploy the new matrix to Fastly Compute
                </summary>
                <div className="mt-2 bg-muted rounded border overflow-hidden">
                  <div className="px-2 py-1 border-b bg-muted/50 flex items-center justify-between">
                    <span className="text-[10px] font-mono text-muted-foreground">
                      deploy command
                    </span>
                    <CopyButton
                      text={result.deploy_hint}
                      ariaLabel="Copy deploy hint"
                      copiedAriaLabel="Copied deploy hint"
                      className="h-6 w-6 hover:bg-muted-foreground/10"
                    />
                  </div>
                  <pre className="p-2 text-[11px] font-mono leading-relaxed whitespace-pre-wrap break-all">
                    {result.deploy_hint}
                  </pre>
                </div>
              </details>
            )}
          </div>
        )}

        <DialogFooter>
          {!result && (
            <>
              <Button variant="outline" onClick={onClose} disabled={mutation.isPending}>
                Cancel
              </Button>
              <Button onClick={() => mutation.mutate()} disabled={mutation.isPending}>
                {mutation.isPending ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                    Training…
                  </>
                ) : (
                  <>
                    <Brain className="h-4 w-4 mr-1" />
                    Start retrain
                  </>
                )}
              </Button>
            </>
          )}
          {result && (
            <Button onClick={onClose}>Close</Button>
          )}
        </DialogFooter>
      </DialogContent>
  )
}

function Metric({ label, value, mono = false }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="p-2 border rounded-md">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className={`text-sm ${mono ? 'font-mono' : ''} font-semibold`}>{value}</div>
    </div>
  )
}

interface RotateKeyButtonProps {
  serviceId: string
}

/**
 * Rotate the AES cookie key used to encrypt session scoring cookies.
 *
 * The new key becomes 'current'; the previous 'current' key moves to the
 * 'previous' slot so already-issued cookies stay decodable for one grace
 * cycle. Two rotations back-to-back will drop the original key entirely
 * and mark its cookies as 'tampered' at the edge.
 */
export function RotateKeyButton({ serviceId }: RotateKeyButtonProps) {
  const qc = useQueryClient()
  const [open, setOpen] = React.useState(false)
  const {
    value: status,
    flash: flashStatus,
    set: setStatus,
    clear: clearStatus,
  } = useAutoFlash<{ kind: 'success' | 'error'; message: string }>(4000)

  const mutation = useMutation({
    mutationFn: async (): Promise<RotateKeyResponse> => {
      const { data, error: errBody, response } = await client.POST(
        '/api/services/{service_id}/scoring/rotate-key',
        {
          params: { path: { service_id: serviceId } },
        },
      )
      if (!response.ok || !data) {
        throw new Error(errBody ? extractApiError(errBody) : `status ${response.status}`)
      }
      return data
    },
    onSuccess: () => {
      flashStatus({ kind: 'success', message: 'Key rotated; previous slot updated' })
      setOpen(false)
      qc.invalidateQueries({ queryKey: ['scoring-status', serviceId] })
      qc.invalidateQueries({ queryKey: scoringConfigCompositeKey(serviceId) })
      qc.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
    },
    onError: (err: unknown) => {
      const message = err instanceof Error ? err.message : 'Failed to rotate key'
      setStatus({ kind: 'error', message })
    },
  })

  return (
    <div className="inline-flex flex-col items-end gap-1">
      <Button
        variant="destructive"
        size="sm"
        onClick={() => {
          clearStatus()
          setOpen(true)
        }}
        disabled={mutation.isPending}
        title="Rotate the AES key used to encrypt scoring cookies"
      >
        {mutation.isPending ? (
          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
        ) : (
          <KeyRound className="h-4 w-4 mr-1" />
        )}
        Rotate AES key
      </Button>

      {status && (
        <p
          className={`text-[11px] ${
            status.kind === 'success' ? 'text-emerald-600' : 'text-destructive'
          }`}
          role="status"
        >
          {status.message}
        </p>
      )}

      <ConfirmDialog
        open={open}
        onOpenChange={(o) => {
          if (!mutation.isPending) setOpen(o)
        }}
        isDangerous
        isPending={mutation.isPending}
        title="Rotate AES cookie key?"
        description={
          <div className="space-y-2 text-sm">
            <p>
              Generates a new AES key and promotes it to the <strong>current</strong>{' '}
              slot. The old current key moves to <strong>previous</strong> and is still
              accepted for one grace cycle.
            </p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>In-flight sessions keep working — their cookies decode under the previous-slot key.</li>
              <li>New sessions get cookies encrypted with the new current key.</li>
              <li>
                <strong>Warning:</strong> rotating twice in quick succession discards the
                original key entirely. Cookies issued under it will be treated as{' '}
                <span className="font-mono">tampered</span> at the edge.
              </li>
            </ul>
          </div>
        }
        confirmLabel="Rotate key"
        cancelLabel="Cancel"
        onConfirm={() => mutation.mutate()}
      />
    </div>
  )
}
