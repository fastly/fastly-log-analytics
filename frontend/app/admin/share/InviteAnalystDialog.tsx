"use client"

import { useState } from "react"
import { useMutation } from "@tanstack/react-query"
import { AlertTriangle, Check, Eye, EyeOff, FileJson, History, KeyRound, Loader2, UserPlus } from "lucide-react"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { CopyButton } from "@/components/ui/confirm-dialog"
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import { useCopyToClipboard, useNowMs, useOpenGeneration } from "@/hooks/useSSE"
import { client, unwrapClient } from "@/lib/api"
import { cn, panelDialogContent, panelDialogFooter, panelDialogHeaderSolid } from "@/lib/utils"
import type { ServiceConfig } from "@/types/api"
import type { components } from "@/types/api.generated"

// ── InviteAnalystDialog (Path A standalone viewer key generator) ──────────

type AnalystInvite = components['schemas']['AnalystInvite']

export interface InviteAnalystDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

type InviteStep = 'confirm' | 'creating' | 'result'

const REVEAL_TTL_MS = 5 * 60 * 1000
export const recentInvites = new Map<string, { invite: AnalystInvite; issuedAt: number }>()

export function getRecentInvite(serviceId: string): AnalystInvite | null {
  const entry = recentInvites.get(serviceId)
  if (!entry) return null
  if (Date.now() - entry.issuedAt > REVEAL_TTL_MS) {
    recentInvites.delete(serviceId)
    return null
  }
  return entry.invite
}

export function CopyField({ label, value, secret, multiline = false }: { label: string; value: string; secret?: boolean; multiline?: boolean }) {
  const [revealed, setRevealed] = useState(false)
  const display = secret && !revealed ? '•'.repeat(Math.min(value.length, 24)) : value

  return (
    <div className="space-y-1.5">
      <Label className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">{label}</Label>
      <div className="flex items-start gap-2">
        <div className={`flex-1 font-mono text-sm bg-muted/50 border rounded-md px-3 py-2 min-w-0 ${multiline ? 'break-all overflow-hidden max-h-32 overflow-y-auto' : 'flex items-center gap-2 truncate'}`}>
          <span className="flex-1">{display}</span>
        </div>
        <div className="flex items-center gap-1">
          {secret && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={revealed ? 'Hide value' : 'Reveal value'}
              className="h-9 w-9 shrink-0 text-muted-foreground hover:text-foreground"
              onClick={() => setRevealed((r) => !r)}
              title={revealed ? 'Hide' : 'Reveal'}
            >
              {revealed ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
            </Button>
          )}
          <CopyButton
            text={value}
            ariaLabel="Copy to clipboard"
            title="Copy"
            className="h-9 w-9 shrink-0 text-muted-foreground hover:text-foreground"
            iconClassName="h-4 w-4"
          />
        </div>
      </div>
    </div>
  )
}

export function CachedInviteBanner({ serviceId, onReveal }: { serviceId: string; onReveal: () => void }) {
  const nowMs = useNowMs()
  const entry = recentInvites.get(serviceId)
  if (!entry || !getRecentInvite(serviceId)) return null
  const ageMs = nowMs ? nowMs - entry.issuedAt : 0
  const remainingMin = Math.max(1, Math.ceil(Math.max(0, REVEAL_TTL_MS - ageMs) / 60000))
  return (
    <Alert className="bg-primary/5 border-primary/20">
      <History className="h-4 w-4" />
      <AlertDescription className="text-sm ml-1 flex items-center justify-between gap-3">
        <span>A key was issued for this service in the last {remainingMin} min — still available in this tab.</span>
        <Button variant="outline" size="sm" onClick={onReveal} className="h-8 shrink-0">
          <Eye className="h-3.5 w-3.5 mr-1.5" />
          Reveal
        </Button>
      </AlertDescription>
    </Alert>
  )
}

export function InviteAnalystDialog(props: InviteAnalystDialogProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <InviteAnalystDialogBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function InviteAnalystDialogBody({ service, open, onOpenChange }: InviteAnalystDialogProps) {
  const [result, setResult] = useState<AnalystInvite | null>(null)
  const { copied: jsonCopied, copy: copyJson } = useCopyToClipboard(2000)

  const createInviteMutation = useMutation({
    mutationFn: async (serviceId: string) => {
      const data = await unwrapClient(client.POST('/api/services/{service_id}/generate-viewer-key', {
        params: { path: { service_id: serviceId } },
      }))
      return data as AnalystInvite
    },
    onSuccess: (invite, serviceId) => {
      recentInvites.set(serviceId, { invite, issuedAt: Date.now() })
      setResult(invite)
    },
  })

  if (!service) return null

  let step: InviteStep = 'confirm'
  if (result) {
    step = 'result'
  } else if (createInviteMutation.isPending) {
    step = 'creating'
  }

  let error = ''
  if (createInviteMutation.error) {
    if (createInviteMutation.error instanceof Error && createInviteMutation.error.message) {
      error = createInviteMutation.error.message
    } else {
      error = 'Failed to create analyst key'
    }
  }

  const handleCopyJson = () => {
    if (!result) return
    const config: Record<string, string> = {
      name: result.name, service_id: result.service_id, fos_bucket: result.fos_bucket,
      fos_region: result.fos_region, fos_endpoint: result.fos_endpoint, fos_prefix: result.fos_prefix,
      access_key_id: result.access_key_id, secret_key: result.secret_key,
    }
    if (result.cdn_url) config.cdn_url = result.cdn_url
    if (result.cdn_service_id) config.cdn_service_id = result.cdn_service_id
    if (result.cdn_secret) config.cdn_secret = result.cdn_secret
    if (result.iceberg_metadata_location) config.iceberg_metadata_location = result.iceberg_metadata_location
    void copyJson(JSON.stringify(config, null, 2))
  }

  return (
    <Dialog open={open} onOpenChange={(isOpen) => { if (step !== 'creating') onOpenChange(isOpen) }}>
      <DialogContent className={cn('sm:max-w-xl', panelDialogContent)} showCloseButton={step !== 'creating'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <div className="flex items-center justify-between mb-1">
            <DialogTitle className="flex items-center gap-2 text-xl font-bold">
              <UserPlus className="h-5 w-5" />
              Invite Analyst
            </DialogTitle>
            <div className="flex items-center gap-1.5 mr-6">
              <div className={`h-1.5 w-6 rounded-full transition-colors ${step === 'confirm' ? 'bg-primary' : 'bg-muted'}`} />
              <div className={`h-1.5 w-6 rounded-full transition-colors ${step === 'result' ? 'bg-primary' : 'bg-muted'}`} />
            </div>
          </div>
          <Badge variant="secondary" className="w-fit font-mono text-[10px] font-normal tracking-tight uppercase">
            {service.name}
          </Badge>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {step === 'confirm' && (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-right-4 duration-300">
              <CachedInviteBanner serviceId={service.service_id} onReveal={() => {
                const cached = getRecentInvite(service.service_id)
                if (cached) setResult(cached)
              }} />
              <div className="rounded-lg border bg-muted/30 p-4 space-y-3">
                <div className="flex items-center gap-2 text-sm font-semibold">
                  <KeyRound className="h-4 w-4 text-primary" />
                  What will happen
                </div>
                <ul className="text-sm text-muted-foreground space-y-1.5 ml-6 list-disc">
                  <li>A new <strong className="text-foreground">read-only</strong> Fastly Object Storage access key will be created, scoped to this service&apos;s bucket.</li>
                  <li>Your stored Fastly API token will be used — no re-entry needed.</li>
                  <li>You&apos;ll receive a JSON config to send to the analyst.</li>
                </ul>
              </div>
              {error && (
                <Alert variant="destructive" className="bg-destructive/5 border-destructive/20">
                  <AlertTriangle className="h-4 w-4" />
                  <AlertDescription className="text-sm ml-1">{error}</AlertDescription>
                </Alert>
              )}
            </div>
          )}

          {step === 'creating' && (
            <div className="p-8 flex flex-col items-center justify-center gap-4 text-center min-h-[300px]">
              <Loader2 className="h-10 w-10 animate-spin text-primary/50" />
              <div className="space-y-1">
                <p className="font-bold text-lg tracking-tight">Generating Analyst Access…</p>
                <p className="text-sm text-muted-foreground">Provisioning read-only keys via Fastly API.</p>
              </div>
            </div>
          )}

          {step === 'result' && result && (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
              <Alert className="bg-amber-500/10 border-amber-500/30 text-amber-700 dark:text-amber-400">
                <AlertTriangle className="h-4 w-4" />
                <AlertDescription className="text-sm ml-1 font-medium">
                  Save the secret key now — available in this browser tab for {REVEAL_TTL_MS / 60000} min, then unrecoverable.
                </AlertDescription>
              </Alert>
              <div className="space-y-4">
                <CopyField label="Display Name" value={result.name} />
                <CopyField label="Fastly Service ID" value={result.service_id} />
                <CopyField label="FOS Bucket" value={result.fos_bucket} />
                <CopyField label="FOS Region" value={result.fos_region} />
                <CopyField label="Access Key ID" value={result.access_key_id} />
                <CopyField label="Secret Key" value={result.secret_key} secret />
                {result.cdn_url && <CopyField label="CDN URL" value={result.cdn_url} />}
                {result.cdn_secret && <CopyField label="CDN Secret" value={result.cdn_secret} secret />}
              </div>
            </div>
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
          {step === 'confirm' && (
            <Button onClick={() => createInviteMutation.mutate(service.service_id)} className="h-10 px-8 font-bold">
              <UserPlus className="h-4 w-4 mr-2" />
              Generate Invite
            </Button>
          )}
          {step === 'result' && (
            <>
              <Button variant="outline" onClick={handleCopyJson} className="h-10 px-6 gap-2">
                {jsonCopied ? <Check className="h-4 w-4 text-emerald-500" /> : <FileJson className="h-4 w-4" />}
                {jsonCopied ? 'Copied!' : 'Copy JSON'}
              </Button>
              <Button onClick={() => onOpenChange(false)} className="h-10 px-8">Done</Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
