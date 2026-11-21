'use client'

import React, { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { AlertCircle, CheckCircle2, ChevronDown, ChevronRight, Loader2, RotateCcw } from 'lucide-react'
import { adminFetch, client, extractApiError, queryKeys, unwrapClient } from '@/lib/api'
import { IS_CLOUD_MODE, isCloudMode } from '@/lib/cloud-engine/mode'
import { invalidateCloudConfigMetadata, resetCloudMetadataCache } from '@/lib/cloud-engine/metadata-cache'
import type { components as CloudComponents } from '@/lib/cloud-engine/cloud-api.generated'
import { REGION_LABELS } from '@/lib/format'
import { cn, panelDialogContent, panelDialogFooter, panelDialogHeaderSolid } from '@/lib/utils'
import { useOpenGeneration } from '@/hooks/useSSE'
import type { components as ApiComponents } from '@/types/api.generated'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'

type ElevationRestoreRequest = ApiComponents['schemas']['HighScaleRestoreFromFosRequest']
type ElevationRestoreResponse = ApiComponents['schemas']['HighScaleRestoreFromFosResponse']
type CloudRecoverRequest = CloudComponents['schemas']['ProvisionRecoverInput']
type CloudRecoverResponse = CloudComponents['schemas']['ProvisionRecoverResponse']

type RestoreResult =
  | { edition: 'elevation'; data: ElevationRestoreResponse }
  | { edition: 'cloud'; data: CloudRecoverResponse }

const SERVICE_ID_RE = /^[A-Za-z0-9_-]+$/

export interface RestoreFromFosDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onRestored?: (serviceId: string) => void
}

export function RestoreFromFosDialog(props: RestoreFromFosDialogProps) {
  const generation = useOpenGeneration(props.open)
  return <RestoreFromFosDialogBody key={generation} {...props} />
}

function RestoreFromFosDialogBody({ open, onOpenChange, onRestored }: RestoreFromFosDialogProps) {
  const cloudMode = IS_CLOUD_MODE || isCloudMode()
  const queryClient = useQueryClient()

  const [serviceId, setServiceId] = useState('')
  const [bucket, setBucket] = useState('')
  const [region, setRegion] = useState('us-east-1')
  const [prefix, setPrefix] = useState('')
  const [archivePrefix, setArchivePrefix] = useState('high-scale/archive')
  const [restoreServingState, setRestoreServingState] = useState(true)
  const [fastlyApiKey, setFastlyApiKey] = useState('')
  const [showExplicitFos, setShowExplicitFos] = useState(false)
  const [fosAccessKey, setFosAccessKey] = useState('')
  const [fosSecretKey, setFosSecretKey] = useState('')
  const [validationError, setValidationError] = useState<string | null>(null)
  const [result, setResult] = useState<RestoreResult | null>(null)

  const restoreMutation = useMutation<RestoreResult, Error, void>({
    mutationFn: async () => {
      const trimmedServiceId = serviceId.trim()
      const trimmedBucket = bucket.trim()
      const trimmedPrefix = prefix.trim()
      const trimmedApiKey = fastlyApiKey.trim()
      const trimmedFosAccessKey = fosAccessKey.trim()
      const trimmedFosSecretKey = fosSecretKey.trim()
      const hasExplicitFos = Boolean(trimmedFosAccessKey && trimmedFosSecretKey)

      if (cloudMode) {
        const payload: CloudRecoverRequest = {
          token: trimmedApiKey,
          service_id: trimmedServiceId,
          fos_bucket: trimmedBucket,
          fos_region: region as CloudRecoverRequest['fos_region'],
          fos_prefix: trimmedPrefix,
          ...(hasExplicitFos
            ? { fos_access_key: trimmedFosAccessKey, fos_secret_key: trimmedFosSecretKey }
            : {}),
        }
        const res = await adminFetch('/api/admin/provision/recover', {
          method: 'POST',
          credentials: 'include',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(payload),
        })
        let parsed: unknown = null
        try {
          parsed = await res.json()
        } catch {
          parsed = null
        }
        if (!res.ok) {
          throw new Error(extractApiError(parsed) || `Recovery failed (${res.status})`)
        }
        return { edition: 'cloud', data: parsed as CloudRecoverResponse }
      }

      const body: ElevationRestoreRequest = {
        service_id: trimmedServiceId,
        bucket: trimmedBucket,
        region,
        prefix: trimmedPrefix,
        archive_prefix: archivePrefix.trim() || 'high-scale/archive',
        restore_serving_state: restoreServingState,
        ...(trimmedApiKey ? { fastly_api_key: trimmedApiKey } : {}),
        ...(hasExplicitFos
          ? { fos_access_key: trimmedFosAccessKey, fos_secret_key: trimmedFosSecretKey }
          : {}),
      }
      const data = await unwrapClient(client.POST('/api/high-scale/restore-from-fos', { body }))
      return { edition: 'elevation', data }
    },
    onSuccess: (res) => {
      // Clear secret inputs immediately after restore succeeds.
      setFastlyApiKey('')
      setFosAccessKey('')
      setFosSecretKey('')
      setValidationError(null)
      setResult(res)

      if (cloudMode) {
        resetCloudMetadataCache()
        invalidateCloudConfigMetadata()
      }
      for (const queryKey of [
        queryKeys.services(),
        queryKeys.bootstrap(),
        queryKeys.admin.overviewAll(),
        queryKeys.admin.healthSnapshot(),
        queryKeys.syncStatus(res.data.service_id),
      ]) {
        void queryClient.invalidateQueries({ queryKey })
      }
    },
  })

  const handleClose = () => {
    setFastlyApiKey('')
    setFosAccessKey('')
    setFosSecretKey('')
    setValidationError(null)
    restoreMutation.reset()
    onOpenChange(false)
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    setValidationError(null)
    restoreMutation.reset()

    const trimmedServiceId = serviceId.trim()
    const trimmedBucket = bucket.trim()
    const trimmedApiKey = fastlyApiKey.trim()
    const trimmedFosAccessKey = fosAccessKey.trim()
    const trimmedFosSecretKey = fosSecretKey.trim()

    if (!trimmedServiceId || !SERVICE_ID_RE.test(trimmedServiceId)) {
      setValidationError('Enter a valid Fastly Service ID (letters, numbers, hyphens, or underscores).')
      return
    }
    if (!trimmedBucket) {
      setValidationError('Enter the Fastly Object Storage bucket name.')
      return
    }
    if (!region) {
      setValidationError('Select the Fastly Object Storage region.')
      return
    }
    if (Boolean(trimmedFosAccessKey) !== Boolean(trimmedFosSecretKey)) {
      setValidationError('Provide both FOS Access Key and Secret Key, or leave both blank.')
      return
    }
    if (cloudMode && !trimmedApiKey) {
      setValidationError('Fastly API token is required to recover a Cloud service.')
      return
    }
    if (!cloudMode && !trimmedApiKey && !(trimmedFosAccessKey && trimmedFosSecretKey)) {
      setValidationError('Provide a Fastly API key or both explicit FOS credentials.')
      return
    }

    restoreMutation.mutate()
  }

  const titleText = cloudMode ? 'Recover Service from FOS' : 'Restore Service from FOS'
  const submitLabel = cloudMode ? 'Recover from FOS' : 'Restore from FOS'

  return (
    <Dialog open={open} onOpenChange={(nextOpen) => { if (!nextOpen) handleClose() }}>
      <DialogContent className={cn('sm:max-w-lg', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2">
            <RotateCcw className="h-5 w-5 text-primary" />
            {titleText}
          </DialogTitle>
          <DialogDescription>
            {cloudMode
              ? 'Re-hydrate service settings, saved views, and historical catalog pointers from Fastly Object Storage into a fresh Compute KV Store.'
              : 'Recover service configuration, saved views, alerts, archive manifests, and ClickHouse serving state from Fastly Object Storage.'}
          </DialogDescription>
        </DialogHeader>

        {result ? (
          <>
            <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1" data-testid="restore-fos-success">
              <Alert>
                <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                <AlertTitle>{cloudMode ? 'Service recovered from FOS' : 'Service restored from FOS'}</AlertTitle>
                <AlertDescription className="text-xs">
                  Service <span className="font-mono font-semibold">{result.data.service_id}</span> is now registered and ready.
                </AlertDescription>
              </Alert>

              {result.edition === 'cloud' ? (
                <div className="rounded-md border bg-muted/20 p-3 space-y-2 text-xs">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Recovery source</span>
                    <span className="font-mono font-semibold">{result.data.source}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Views restored</span>
                    <span className="font-mono font-semibold">{result.data.views_restored}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Catalog generation</span>
                    <span className="font-mono font-semibold">
                      {result.data.catalog_generation === null ? 'None' : String(result.data.catalog_generation)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Front dictionaries synced</span>
                    <span className="font-mono font-semibold">{result.data.front_synced ? 'Yes' : 'No'}</span>
                  </div>
                  {!result.data.front_synced && (
                    <p className="text-[11px] text-amber-600 dark:text-amber-400 pt-1">
                      Front VCL dictionary sync did not complete automatically. Re-run recovery or check Front service permissions.
                    </p>
                  )}
                </div>
              ) : (
                <div className="rounded-md border bg-muted/20 p-3 space-y-2 text-xs">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">DR state restored</span>
                    <span className="font-mono font-semibold">{result.data.dr_state_restored ? 'Yes' : 'No'}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Views restored</span>
                    <span className="font-mono font-semibold">{result.data.views_restored}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Alerts restored</span>
                    <span className="font-mono font-semibold">{result.data.alerts_restored}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Scoring labels restored</span>
                    <span className="font-mono font-semibold">{result.data.scoring_labels_restored}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Audit logs restored</span>
                    <span className="font-mono font-semibold">{result.data.audit_logs_restored}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Archive manifests registered</span>
                    <span className="font-mono font-semibold">{result.data.manifests_registered}</span>
                  </div>
                  <div className="flex justify-between gap-4">
                    <span className="text-muted-foreground">Domains rebuilt</span>
                    <span className="font-mono font-semibold text-right">
                      {Object.keys(result.data.domains_rebuilt).length > 0
                        ? Object.keys(result.data.domains_rebuilt).join(', ')
                        : 'None'}
                    </span>
                  </div>
                </div>
              )}
            </div>
            <DialogFooter className={panelDialogFooter}>
              <Button type="button" variant="outline" onClick={handleClose}>
                Close
              </Button>
              <Button
                type="button"
                onClick={() => {
                  const id = result.data.service_id
                  handleClose()
                  onRestored?.(id)
                }}
              >
                Open Dashboard
              </Button>
            </DialogFooter>
          </>
        ) : (
          <form onSubmit={handleSubmit} className="flex flex-col flex-1 min-h-0">
            <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1">
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-1.5">
                  <Label htmlFor="restore-service-id" className="text-xs font-semibold">Fastly Service ID</Label>
                  <Input
                    id="restore-service-id"
                    placeholder="e.g. 5aBcDeFgHiJkLmNoPqRsTu"
                    value={serviceId}
                    onChange={(e) => setServiceId(e.target.value)}
                    className="h-8 font-mono text-xs"
                    autoComplete="off"
                    disabled={restoreMutation.isPending}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="restore-fos-region" className="text-xs font-semibold">FOS Region</Label>
                  <Select
                    value={region}
                    onValueChange={(v) => { if (v) setRegion(v) }}
                    disabled={restoreMutation.isPending}
                  >
                    <SelectTrigger id="restore-fos-region" aria-label="FOS Region" className="h-8 text-xs">
                      <SelectValue>{(val) => REGION_LABELS[String(val)] || String(val)}</SelectValue>
                    </SelectTrigger>
                    <SelectContent>
                      {Object.entries(REGION_LABELS).map(([k, label]) => (
                        <SelectItem key={k} value={k} className="text-xs">
                          {label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-1.5">
                  <Label htmlFor="restore-fos-bucket" className="text-xs font-semibold">FOS Bucket</Label>
                  <Input
                    id="restore-fos-bucket"
                    placeholder="e.g. my-logs-bucket"
                    value={bucket}
                    onChange={(e) => setBucket(e.target.value)}
                    className="h-8 font-mono text-xs"
                    autoComplete="off"
                    disabled={restoreMutation.isPending}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="restore-fos-prefix" className="text-xs font-semibold">FOS Prefix (optional)</Label>
                  <Input
                    id="restore-fos-prefix"
                    placeholder="e.g. logs/ or leave blank"
                    value={prefix}
                    onChange={(e) => setPrefix(e.target.value)}
                    className="h-8 font-mono text-xs"
                    autoComplete="off"
                    disabled={restoreMutation.isPending}
                  />
                </div>
              </div>

              <div className="space-y-1.5">
                <Label htmlFor="restore-fastly-api-key" className="text-xs font-semibold">
                  {cloudMode ? 'Fastly API Token' : 'Fastly API Key'}
                </Label>
                <p className="text-[11px] text-muted-foreground leading-tight">
                  {cloudMode
                    ? 'Used to mint a bucket-scoped FOS access key (unless explicit FOS keys are provided below) and sync Front dictionaries.'
                    : 'Mints a fresh bucket-scoped FOS access key automatically. Optional if you supply explicit FOS credentials below.'}
                </p>
                <Input
                  id="restore-fastly-api-key"
                  type="password"
                  placeholder="Fastly API token"
                  value={fastlyApiKey}
                  onChange={(e) => setFastlyApiKey(e.target.value)}
                  className="h-8 font-mono text-xs"
                  autoComplete="off"
                  disabled={restoreMutation.isPending}
                />
              </div>

              {!cloudMode && (
                <div className="space-y-3 rounded-md border bg-muted/10 p-3">
                  <div className="space-y-1.5">
                    <Label htmlFor="restore-archive-prefix" className="text-xs font-semibold">Archive Prefix</Label>
                    <Input
                      id="restore-archive-prefix"
                      value={archivePrefix}
                      onChange={(e) => setArchivePrefix(e.target.value)}
                      className="h-8 font-mono text-xs"
                      autoComplete="off"
                      disabled={restoreMutation.isPending}
                    />
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <div className="space-y-0.5">
                      <Label htmlFor="restore-serving-state" className="text-xs font-semibold cursor-pointer">
                        Rebuild ClickHouse serving state &amp; dashboard projections
                      </Label>
                      <p className="text-[10px] text-muted-foreground leading-tight">
                        Replays archived Parquet manifests across request, RUM vitals, and RUM errors domains.
                      </p>
                    </div>
                    <Switch
                      id="restore-serving-state"
                      checked={restoreServingState}
                      onCheckedChange={setRestoreServingState}
                      disabled={restoreMutation.isPending}
                    />
                  </div>
                </div>
              )}

              <div className="rounded-md border bg-muted/5">
                <button
                  type="button"
                  aria-expanded={showExplicitFos}
                  onClick={() => setShowExplicitFos((prev) => !prev)}
                  className="w-full flex items-center justify-between px-3 py-2 text-xs font-semibold text-foreground/80 hover:bg-muted/20 transition-colors"
                >
                  <span>Explicit FOS Credentials (optional)</span>
                  {showExplicitFos ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                </button>
                {showExplicitFos && (
                  <div className="px-3 pb-3 pt-1 space-y-3 border-t">
                    <p className="text-[10px] text-muted-foreground leading-tight">
                      Leave both blank to mint a fresh bucket-scoped FOS key from your Fastly API key.
                    </p>
                    <div className="space-y-1.5">
                      <Label htmlFor="restore-fos-access-key" className="text-xs">FOS Access Key ID</Label>
                      <Input
                        id="restore-fos-access-key"
                        type="password"
                        placeholder="Optional FOS access key ID"
                        value={fosAccessKey}
                        onChange={(e) => setFosAccessKey(e.target.value)}
                        className="h-8 font-mono text-xs"
                        autoComplete="off"
                        disabled={restoreMutation.isPending}
                      />
                    </div>
                    <div className="space-y-1.5">
                      <Label htmlFor="restore-fos-secret-key" className="text-xs">FOS Secret Access Key</Label>
                      <Input
                        id="restore-fos-secret-key"
                        type="password"
                        placeholder="Optional FOS secret access key"
                        value={fosSecretKey}
                        onChange={(e) => setFosSecretKey(e.target.value)}
                        className="h-8 font-mono text-xs"
                        autoComplete="off"
                        disabled={restoreMutation.isPending}
                      />
                    </div>
                  </div>
                )}
              </div>

              {(validationError || restoreMutation.isError) && (
                <Alert variant="destructive" data-testid="restore-fos-error">
                  <AlertCircle className="h-4 w-4" />
                  <AlertTitle>Restore failed</AlertTitle>
                  <AlertDescription className="text-xs">
                    {validationError ?? restoreMutation.error?.message ?? 'An unexpected error occurred.'}
                  </AlertDescription>
                </Alert>
              )}
            </div>

            <DialogFooter className={panelDialogFooter}>
              <Button type="button" variant="outline" onClick={handleClose} disabled={restoreMutation.isPending}>
                Cancel
              </Button>
              <Button type="submit" disabled={restoreMutation.isPending}>
                {restoreMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                {restoreMutation.isPending ? 'Restoring…' : submitLabel}
              </Button>
            </DialogFooter>
          </form>
        )}
      </DialogContent>
    </Dialog>
  )
}
