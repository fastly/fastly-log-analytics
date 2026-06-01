'use client'

import React, { useState, useEffect } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  UserPlus,
  Copy,
  Check,
  AlertTriangle,
  Loader2,
  FileJson,
  Eye,
  EyeOff,
  KeyRound,
} from 'lucide-react'
import { Label } from '@/components/ui/label'
import { client } from '@/lib/api'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'
import type { components } from '@/types/api.generated'

type ServiceConfig = components["schemas"]["ServiceConfig"]
type AnalystInvite = components["schemas"]["AnalystInvite"]

interface InviteAnalystDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

type Step = 'confirm' | 'creating' | 'result'

function CopyField({ label, value, secret, multiline = false }: { label: string; value: string; secret?: boolean; multiline?: boolean }) {
  const [copied, setCopied] = useState(false)
  const [revealed, setRevealed] = useState(false)

  const copy = () => {
    navigator.clipboard.writeText(value)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

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
              className="h-9 w-9 shrink-0 text-muted-foreground hover:text-foreground"
              onClick={() => setRevealed(r => !r)}
              title={revealed ? 'Hide' : 'Reveal'}
            >
              {revealed ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
            </Button>
          )}
          <Button
            variant="ghost"
            size="icon"
            className="h-9 w-9 shrink-0 text-muted-foreground hover:text-foreground"
            onClick={copy}
            title="Copy"
          >
            {copied ? <Check className="h-4 w-4 text-emerald-500" /> : <Copy className="h-4 w-4" />}
          </Button>
        </div>
      </div>
    </div>
  )
}

export function InviteAnalystDialog({ service, open, onOpenChange }: InviteAnalystDialogProps) {
  const [step, setStep] = useState<Step>('confirm')
  const [error, setError] = useState('')
  const [result, setResult] = useState<AnalystInvite | null>(null)
  const [jsonCopied, setJsonCopied] = useState(false)

  useEffect(() => {
    if (!open) {
      setTimeout(() => {
        setStep('confirm')
        setError('')
        setResult(null)
        setJsonCopied(false)
      }, 300)
    }
  }, [open])

  if (!service) return null

  const handleCreate = async () => {
    setError('')
    setStep('creating')
    try {
      const { data } = await client.POST("/api/services/{service_id}/generate-viewer-key", {
        params: { path: { service_id: service.service_id } },
      })
      setResult(data as any)
      setStep('result')
    } catch (e: any) {
      setError(e.message || 'Failed to create analyst key')
      setStep('confirm')
    }
  }

  const handleCopyJson = () => {
    if (!result) return
    const config: Record<string, string> = {
      name: result.name,
      service_id: result.service_id,
      fos_bucket: result.fos_bucket,
      fos_region: result.fos_region,
      fos_endpoint: result.fos_endpoint,
      fos_prefix: result.fos_prefix,
      access_key_id: result.access_key_id,
      secret_key: result.secret_key,
    }
    if (result.cdn_url) config.cdn_url = result.cdn_url
    if (result.cdn_service_id) config.cdn_service_id = result.cdn_service_id
    if (result.cdn_secret) config.cdn_secret = result.cdn_secret
    if (result.iceberg_metadata_location) config.iceberg_metadata_location = result.iceberg_metadata_location
    navigator.clipboard.writeText(JSON.stringify(config, null, 2))
    setJsonCopied(true)
    setTimeout(() => setJsonCopied(false), 2000)
  }

  return (
    <Dialog open={open} onOpenChange={(isOpen) => {
      if (step === 'creating') return
      onOpenChange(isOpen)
    }}>
      <DialogContent className={cn("sm:max-w-xl", panelDialogContent)} showCloseButton={step !== 'creating'}>
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
          {service && (
            <Badge variant="secondary" className="w-fit font-mono text-[10px] font-normal tracking-tight uppercase">
              {service.name}
            </Badge>
          )}
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {step === 'confirm' && (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-right-4 duration-300">
              <div className="rounded-lg border bg-muted/30 p-4 space-y-3">
                <div className="flex items-center gap-2 text-sm font-semibold">
                  <KeyRound className="h-4 w-4 text-primary" />
                  What will happen
                </div>
                <ul className="text-sm text-muted-foreground space-y-1.5 ml-6 list-disc">
                  <li>A new <strong className="text-foreground">read-only</strong> Fastly Object Storage access key will be created, scoped to this service's bucket.</li>
                  <li>Your stored Fastly API token will be used — no re-entry needed.</li>
                  <li>You'll receive a JSON config to send to the analyst.</li>
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
                  Save the secret key now — it cannot be retrieved again.
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
            <Button
              onClick={handleCreate}
              className="h-10 px-8 font-bold"
            >
              <UserPlus className="h-4 w-4 mr-2" />
              Generate Invite
            </Button>
          )}

          {step === 'result' && (
            <>
              <Button
                variant="outline"
                onClick={handleCopyJson}
                className="h-10 px-6 gap-2"
              >
                {jsonCopied ? <Check className="h-4 w-4 text-emerald-500" /> : <FileJson className="h-4 w-4" />}
                {jsonCopied ? 'Copied!' : 'Copy JSON'}
              </Button>
              <Button onClick={() => onOpenChange(false)} className="h-10 px-8">
                Done
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
