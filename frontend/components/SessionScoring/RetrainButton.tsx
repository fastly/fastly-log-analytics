'use client'

import * as React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Brain, Check, Copy, Loader2 } from 'lucide-react'

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
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { client } from '@/lib/api'
import type { components } from '@/types/api.generated'

type RetrainResult = components['schemas']['ScoringRetrainResponse']

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
  const queryClient = useQueryClient()
  const [open, setOpen] = React.useState(false)
  const [sinceDays, setSinceDays] = React.useState(7)
  const [result, setResult] = React.useState<RetrainResult | null>(null)
  const [copied, setCopied] = React.useState(false)

  const mutation = useMutation({
    mutationFn: async (): Promise<RetrainResult> => {
      const { data, response } = await client.POST(
        '/api/services/{service_id}/scoring/retrain' as any,
        {
          params: {
            path: { service_id: serviceId },
            query: { since_days: sinceDays },
          },
        } as any,
      )
      if (!response.ok) {
        const err = (data as any)?.detail?.error ?? `status ${response.status}`
        throw new Error(err)
      }
      return data as RetrainResult
    },
    onSuccess: (data) => {
      setResult(data)
      // Refresh the AUC field + health card after retrain.
      queryClient.invalidateQueries({
        predicate: (q) =>
          Array.isArray(q.queryKey) &&
          typeof q.queryKey[0] === 'string' &&
          (q.queryKey[0] as string).startsWith('scoring-') &&
          q.queryKey[1] === serviceId,
      })
    },
  })

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        setOpen(o)
        if (!o) {
          setResult(null)
          setCopied(false)
          mutation.reset()
        }
      }}
    >
      <DialogTrigger
        render={
          <Button variant="outline" size="sm">
            <Brain className="h-4 w-4 mr-1" />
            Retrain matrix
          </Button>
        }
      />
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
              <Metric label="Sessions trained" value={(result.sessions_trained_on ?? 0).toLocaleString()} />
              <Metric label="Transitions" value={(result.transitions ?? 0).toLocaleString()} />
              <Metric label="Vocab size" value={(result.vocab_size ?? 0).toLocaleString()} />
            </div>
            {result.rejected && (
              <p className="text-xs text-muted-foreground">
                Kept {(result.rejected.kept ?? 0).toLocaleString()} sessions ·
                {' '}dropped {(result.rejected.too_few_events ?? 0).toLocaleString()} too-short,
                {' '}{(result.rejected.too_fast ?? 0).toLocaleString()} too-fast ·
                {' '}{(result.rejected.routes_seen ?? 0).toLocaleString()} routes seen
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
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label={copied ? 'Copied deploy hint' : 'Copy deploy hint'}
                      className="h-6 w-6 hover:bg-muted-foreground/10"
                      onClick={() => {
                        navigator.clipboard.writeText(result.deploy_hint!)
                        setCopied(true)
                        setTimeout(() => setCopied(false), 2000)
                      }}
                    >
                      {copied ? (
                        <Check className="h-3 w-3 text-emerald-500" />
                      ) : (
                        <Copy className="h-3 w-3" />
                      )}
                    </Button>
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
              <Button variant="outline" onClick={() => setOpen(false)} disabled={mutation.isPending}>
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
            <Button onClick={() => setOpen(false)}>Close</Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
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
