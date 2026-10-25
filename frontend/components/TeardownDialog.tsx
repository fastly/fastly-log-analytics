'use client'

import { useQuery } from '@tanstack/react-query'
import { useDerivedState, useOpenGeneration } from '@/hooks/useSSE'
import React, { useState } from 'react'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import {
  AlertTriangle,
  Trash2,
  AlertCircle,
  HardDrive,
  Cloud,
} from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEActionDialogShell, SSEExecutionSection } from '@/components/SSEModal'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { formatBytes, formatCount } from '@/lib/format'
import { queryKeys } from '@/lib/query-keys'
import { cn } from '@/lib/utils'
import type { ServiceConfig } from '@/types/api'

interface DeleteDataDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}

export function DeleteDataDialog(props: DeleteDataDialogProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <DeleteDataDialogBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function DeleteDataDialogBody({ service, open, onOpenChange, onComplete }: DeleteDataDialogProps) {
  const [isExecuting, setIsExecuting] = useState(false)
  const [confirmText, setConfirmText] = useState('')
  const { lines, status, error: sseError, start, stop } = useSSE()

  const serviceId = service?.service_id
  const { data: icebergInfo, isLoading: icebergLoading } = useQuery({
    queryKey: queryKeys.admin.icebergInfo(serviceId),
    queryFn: async () => {
      return unwrapClient(client.GET('/api/admin/iceberg-info', {
        params: { query: { service_id: serviceId } as Record<string, string> },
      }))
    },
    enabled: open && !isExecuting && !!serviceId,
    staleTime: 30_000,
  })

  if (!service) return null

  const canExecute = confirmText.trim() === service.name

  const handleExecute = () => {
    if (!canExecute) return
    setIsExecuting(true)
    start(`/api/admin/reset-logs?service_id=${encodeURIComponent(service.service_id)}`, {
      confirm: service.service_id,
    })
  }

  return (
    <SSEActionDialogShell
      open={open}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
      status={status}
      onStop={stop}
      isExecuting={isExecuting}
      maxWidthClassName="sm:max-w-lg"
      title={<><AlertTriangle className="h-6 w-6" />Delete Data: {service.name}</>}
      executionSection={
        <SSEExecutionSection
          title="Deleting Log Data"
          subtitle="Please do not close this window until the process is complete."
          lines={lines}
          status={status}
          error={sseError}
          heightClassName="h-[320px]"
          progressLabel="Progress"
          doneMessage="Log data deleted. You may now close this window."
        />
      }
      confirmButton={
        <Button
          variant="destructive"
          className="h-10 px-8 font-bold"
          onClick={handleExecute}
          disabled={!canExecute}
          title={!canExecute ? `Type "${service.name}" to confirm` : undefined}
        >
          Delete Data
        </Button>
      }
    >
      <div className="p-8 space-y-6">
        <Alert variant="destructive" className="bg-destructive/5 text-destructive border-destructive/20">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription className="text-[13px] ml-1 font-medium">
            {service.is_high_scale
              ? "This permanently deletes all of this service's log data from the high-scale ClickHouse cluster. New logs from the live edge will start populating immediately afterward. This cannot be undone."
              : "This permanently deletes all of this service's log data — both local (the analytical database and cache) and cloud-stored (the Iceberg log table in Fastly Object Storage). New logs from the live edge will start populating immediately afterward. This cannot be undone."
            }
          </AlertDescription>
        </Alert>

        {!service.is_high_scale ? (
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-lg border border-destructive/20 bg-destructive/5 p-3 space-y-1">
              <div className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                <HardDrive className="h-3 w-3" /> Local
              </div>
              <div className="text-lg font-mono font-bold tracking-tight">
                {formatBytes(service.duckdb_size_bytes ?? 0)}
              </div>
              <div className="text-[11px] text-muted-foreground">
                {formatCount(service.cache_file_count ?? 0)} file{service.cache_file_count === 1 ? '' : 's'}
              </div>
            </div>

            <div className="rounded-lg border border-destructive/20 bg-destructive/5 p-3 space-y-1">
              <div className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                <Cloud className="h-3 w-3" /> Cloud
              </div>
              {icebergLoading ? (
                <div className="text-lg font-mono font-bold tracking-tight text-muted-foreground">…</div>
              ) : (
                <>
                  <div className="text-lg font-mono font-bold tracking-tight">
                    {formatBytes(icebergInfo?.size_bytes ?? 0)}
                  </div>
                  <div className="text-[11px] text-muted-foreground">
                    {formatCount(icebergInfo?.data_files ?? 0)} file{icebergInfo?.data_files === 1 ? '' : 's'}
                  </div>
                </>
              )}
            </div>
          </div>
        ) : (
          <div className="grid gap-3">
            <div className="rounded-lg border border-destructive/20 bg-destructive/5 p-3 space-y-1">
              <div className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                <HardDrive className="h-3 w-3" /> Analytics Cluster
              </div>
              <div className="text-sm font-medium tracking-tight">
                High-Scale ClickHouse Backend
              </div>
              <div className="text-[11px] text-muted-foreground">
                Data deletion is synchronous and cluster-wide.
              </div>
            </div>
          </div>
        )}

        <p className="text-xs text-muted-foreground">
          Preserved: saved views, alerts, source configuration, audit history, and scoring labels.
        </p>

        <div className="space-y-2">
          <Label htmlFor="delete-data-confirm" className="text-sm font-medium">
            Type <span className="font-mono font-bold text-destructive">{service.name}</span> to confirm
          </Label>
          <Input
            id="delete-data-confirm"
            value={confirmText}
            onChange={(e) => setConfirmText(e.target.value)}
            placeholder={service.name}
            className="font-mono text-sm"
            autoComplete="off"
            autoCorrect="off"
            autoCapitalize="off"
            spellCheck={false}
          />
        </div>
      </div>
    </SSEActionDialogShell>
  )
}


interface TeardownDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}

function isAnalyst(service: ServiceConfig | null) {
  return service?.access_level === 'read_only'
}

export function TeardownDialog(props: TeardownDialogProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <TeardownDialogBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function TeardownDialogBody({ service, open, onOpenChange, onComplete }: TeardownDialogProps) {
  const [removeLogging, setRemoveLogging] = useState(true)
  // Follows the service's RUM flag until the operator toggles it.
  const [removeRum, setRemoveRum] = useDerivedState(service?.rum_enabled || false)
  const [removeBucket, setRemoveBucket] = useState(true)
  const [removeCloudFiles, setRemoveCloudFiles] = useState(true)
  const [removeCache, setRemoveCache] = useState(true)
  const [isExecuting, setIsExecuting] = useState(false)
  // Security: backend now requires a caller-supplied Fastly token with the
  // `global` scope for any teardown that touches Fastly (logging/CDN/bucket).
  // Server-stored credentials are no longer accepted as a fallback. The admin
  // must paste their own token each time so a CSRF or stolen-session attacker
  // cannot trigger a customer outage without also having a valid Fastly token.
  const [apiToken, setApiToken] = useState('')

  const { lines, status, error: sseError, start, stop } = useSSE()

  if (!service) return null

  const analyst = isAnalyst(service)
  const removeCdn = removeLogging
  const bucketDisabled = !removeLogging || !removeRum
  const effectiveRemoveBucket = bucketDisabled ? false : removeBucket

  // Security: teardown is now POST-only (CSRF defense). The token, service
  // id and removal flags travel in the request body, not the URL — keeps
  // the Fastly API token out of browser history, proxy access logs, and
  // the Referer header on any subsequent navigation.
  const teardownUrl = '/api/provision/teardown'
  const teardownBody: Record<string, unknown> = analyst
    ? {
        service_id: service.service_id,
        remove_logging: false,
        remove_rum: false,
        remove_cdn: false,
        remove_bucket: false,
        remove_cache: removeCache,
      }
    : {
        service_id: service.service_id,
        remove_logging: removeLogging,
        remove_rum: removeRum,
        remove_cdn: removeCdn,
        remove_bucket: effectiveRemoveBucket,
        remove_cloud_files: removeCloudFiles,
        remove_cache: removeCache,
        token: apiToken,
      }

  const tokenRequired = !analyst && (removeLogging || removeRum || removeCdn || effectiveRemoveBucket)
  const hasSelectedAnyAction = removeLogging || removeRum || removeCache
  const canExecute = (!tokenRequired || apiToken.trim().length > 0) && hasSelectedAnyAction

  const handleExecute = () => {
    if (!canExecute) return
    setIsExecuting(true)
    start(teardownUrl, teardownBody)
  }

  return (
    <SSEActionDialogShell
      open={open}
      onOpenChange={onOpenChange}
      onComplete={onComplete}
      status={status}
      onStop={stop}
      isExecuting={isExecuting}
      maxWidthClassName="sm:max-w-2xl"
      title={<><AlertTriangle className="h-6 w-6" />Teardown: {service.name}</>}
      executionSection={
        <SSEExecutionSection
          title="Executing Teardown Actions"
          subtitle="Please do not close this window until the process is complete."
          lines={lines}
          status={status}
          error={sseError}
          heightClassName="h-[400px]"
          progressLabel="Progress"
          doneMessage="Teardown completed successfully! You may now close this window."
        />
      }
      confirmButton={
        <Button
          variant="destructive"
          className="h-10 px-8 font-bold"
          onClick={handleExecute}
          disabled={!canExecute}
          title={!canExecute ? 'Enter your Fastly API token to proceed' : undefined}
        >
          Execute Teardown
        </Button>
      }
    >
      {analyst ? (
        <div className="p-8 space-y-8">
          <Alert variant="destructive" className="bg-destructive/5 text-destructive border-destructive/20">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription className="text-[13px] ml-1 font-medium">
              This will remove your local analyst connection to this service. It does not affect the shared cloud data or the admin&apos;s configuration.
            </AlertDescription>
          </Alert>

          <div className="space-y-4">
            <div className="flex items-center gap-2 mb-1">
              <Trash2 className="h-4 w-4 text-muted-foreground" />
              <h4 className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Local Data</h4>
            </div>
            <div className="space-y-3 bg-destructive/5 border border-destructive/10 rounded-lg p-4">
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <Label htmlFor="analyst-rem-cache" className="text-sm font-medium cursor-pointer">Purge local database and cache</Label>
                  <p className="text-[10px] text-muted-foreground">Removes the local DuckDB file and Parquet cache. The cloud data is unaffected.</p>
                </div>
                <Switch id="analyst-rem-cache" checked={removeCache} onCheckedChange={setRemoveCache} />
              </div>
            </div>
          </div>
        </div>
      ) : (
        <div className="p-8 space-y-8">
          <Alert variant="destructive" className="bg-destructive/5 text-destructive border-destructive/20">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription className="text-[13px] ml-1 font-medium">
              This will remove the log ingestion pipeline for this service. Select the resources you want to permanently delete.
            </AlertDescription>
          </Alert>

          {/* What to Remove Questionnaire - Only shown if RUM is enabled */}
          {service.rum_enabled && (
            <div className="space-y-4">
              <div className="flex items-center gap-2 mb-1">
                <Trash2 className="h-4 w-4 text-muted-foreground" />
                <h4 className="text-xs font-bold uppercase tracking-widest text-muted-foreground">What to remove from Fastly</h4>
              </div>
              <div className="space-y-4 bg-muted/40 border rounded-lg p-4">
                <div className="flex items-center justify-between group">
                  <div className="space-y-0.5">
                    <Label htmlFor="toggle-remove-logging" className="text-sm font-semibold cursor-pointer">Remove Request Logging</Label>
                    <p className="text-[11px] text-muted-foreground">Endpoints and VCL capture snippets for request logs.</p>
                  </div>
                  <Switch
                    id="toggle-remove-logging"
                    checked={removeLogging}
                    onCheckedChange={(checked) => {
                      setRemoveLogging(checked)
                      if (!checked) {
                        setRemoveBucket(false)
                      }
                    }}
                  />
                </div>

                <div className="flex items-center justify-between group">
                  <div className="space-y-0.5">
                    <Label htmlFor="toggle-remove-rum" className="text-sm font-semibold cursor-pointer">Remove Real User Monitoring (RUM)</Label>
                    <p className="text-[11px] text-muted-foreground">Tracking snippets and collector endpoints for Faro RUM.</p>
                  </div>
                  <Switch
                    id="toggle-remove-rum"
                    checked={removeRum}
                    onCheckedChange={(checked) => {
                      setRemoveRum(checked)
                      if (!checked) {
                        setRemoveBucket(false)
                      }
                    }}
                  />
                </div>
              </div>
            </div>
          )}

          {/* Resource Removal Options */}
          <div className="space-y-4">
            <div className="flex items-center gap-2 mb-1">
              <Trash2 className="h-4 w-4 text-muted-foreground" />
              <h4 className="text-xs font-bold uppercase tracking-widest text-muted-foreground">Resource Removal</h4>
            </div>

            <div className="grid grid-cols-1 gap-4">
              <div className="space-y-3 bg-destructive/5 border border-destructive/10 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-2">
                   <AlertCircle className="h-4 w-4 text-destructive" />
                   <span className="text-xs font-bold text-destructive uppercase tracking-wide">Danger Zone</span>
                </div>

                <div className="space-y-3">
                  <div className={cn("flex items-center justify-between group", !removeLogging && "opacity-50")}>
                    <div className="space-y-0.5">
                      <Label htmlFor="rem-logging" className="text-sm font-medium">Delete Fastly logging endpoint</Label>
                      <p className="text-[10px] text-muted-foreground">Removes the S3 logging configuration from the service.</p>
                    </div>
                    <Switch id="rem-logging" checked={removeLogging} disabled={true} className="opacity-50" />
                  </div>

                  <div className={cn("flex items-center justify-between group", !removeLogging && "opacity-50")}>
                    <div className="space-y-0.5">
                      <Label htmlFor="rem-cdn" className="text-sm font-medium">Delete CDN VCL proxy service</Label>
                      <p className="text-[10px] text-muted-foreground">Permanently removes the secondary CDN service.</p>
                    </div>
                    <Switch id="rem-cdn" checked={removeLogging} disabled={true} className="opacity-50" />
                  </div>

                  <div className={cn("flex items-center justify-between group", !removeLogging && "opacity-50")}>
                    <div className="space-y-0.5">
                      <Label htmlFor="rem-cloud-files" className="text-sm font-medium cursor-pointer">Delete standard logs from cloud storage</Label>
                      <p className="text-[10px] text-muted-foreground">Deletes raw logs under standard prefix, preserving RUM logs.</p>
                    </div>
                    <Switch id="rem-cloud-files" checked={removeLogging && removeCloudFiles} disabled={!removeLogging} onCheckedChange={setRemoveCloudFiles} />
                  </div>

                  <div className={cn("flex items-center justify-between group", bucketDisabled && "opacity-60")}>
                    <div className="space-y-0.5">
                      <Label htmlFor="rem-buck-new" className="text-sm font-medium cursor-pointer">Delete FOS bucket and all data</Label>
                      {bucketDisabled ? (
                        <p className="text-[10px] text-amber-500 font-medium">
                          {!removeLogging && !removeRum
                            ? "FOS bucket cannot be deleted because both standard logging and RUM are being kept active."
                            : !removeLogging
                            ? "FOS bucket cannot be deleted because Request Logging is being kept active."
                            : "FOS bucket cannot be deleted because Real User Monitoring (RUM) is being kept active."
                          }
                        </p>
                      ) : service.rum_enabled ? (
                        <p className="text-[10px] text-amber-500 font-medium">Warning: This bucket also contains Real User Monitoring (RUM) data. Deleting it will permanently lose all request AND RUM logs.</p>
                      ) : (
                        <p className="text-[10px] text-muted-foreground text-destructive font-medium">Warning: This cannot be undone. All logs will be lost.</p>
                      )}
                    </div>
                    <Switch id="rem-buck-new" checked={effectiveRemoveBucket} disabled={bucketDisabled} onCheckedChange={setRemoveBucket} />
                  </div>

                  <div className="flex items-center justify-between group">
                    <div className="space-y-0.5">
                      <Label htmlFor="rem-cache-new" className="text-sm font-medium cursor-pointer">Purge local database and cache</Label>
                      <p className="text-[10px] text-muted-foreground text-destructive font-medium">Removes the local DuckDB file, Iceberg catalog, and buffer cache.</p>
                    </div>
                    <Switch id="rem-cache-new" checked={removeCache} onCheckedChange={setRemoveCache} />
                  </div>
                </div>
              </div>
            </div>
          </div>

          {tokenRequired && (
            <div className="space-y-2">
              <div className="flex items-center gap-2 mb-1">
                <AlertCircle className="h-4 w-4 text-destructive" />
                <h4 className="text-xs font-bold uppercase tracking-widest text-destructive">Fastly API Token</h4>
              </div>
              <div className="bg-destructive/5 border border-destructive/20 rounded-lg p-4 space-y-2">
                <Label htmlFor="teardown-api-token" className="text-sm font-medium">
                  Paste a Fastly token with the <code>global</code> scope
                </Label>
                <p className="text-[11px] text-muted-foreground">
                  The server no longer falls back to a stored token for destructive teardown.
                  Provide a token validated to have full permissions on this service.
                </p>
                <Input
                  id="teardown-api-token"
                  type="password"
                  placeholder="Fastly API token"
                  value={apiToken}
                  onChange={(e) => setApiToken(e.target.value)}
                  className="font-mono text-sm"
                  autoComplete="off"
                />
              </div>
            </div>
          )}
        </div>
      )}
    </SSEActionDialogShell>
  )
}
