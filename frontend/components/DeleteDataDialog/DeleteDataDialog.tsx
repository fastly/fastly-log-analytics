'use client'

import React, { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter
} from '@/components/ui/dialog'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { AlertTriangle, AlertCircle, HardDrive, Cloud } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEProgressView } from '@/components/SSEModal'
import { client } from '@/lib/api'
import { formatBytes } from '@/lib/format'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'
import type { components } from '@/types/api.generated'

type ServiceConfig = components["schemas"]["ServiceConfig"]

interface DeleteDataDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}

export function DeleteDataDialog({ service, open, onOpenChange, onComplete }: DeleteDataDialogProps) {
  const [isExecuting, setIsExecuting] = useState(false)
  // Type-to-confirm (GitHub-repo-delete style): the destructive button
  // stays disabled until this exactly matches the service name, so a
  // stray click can't wipe the wrong service's log history.
  const [confirmText, setConfirmText] = useState('')
  const { lines, status, error: sseError, start, stop, reset } = useSSE()

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      const id = setTimeout(() => {
        setIsExecuting(false)
        setConfirmText('')
        reset()
      }, 300)
      return () => clearTimeout(id)
    }
  }, [open, reset])

  // Cloud (Iceberg table) stats for the row being deleted — NOT necessarily
  // the active service, so `service_id` overrides the default x-service-id
  // header the same way the reset-logs call itself does (see handleExecute).
  const serviceId = service?.service_id
  const { data: icebergInfo, isLoading: icebergLoading } = useQuery({
    queryKey: ['admin', 'iceberg-info', serviceId],
    queryFn: async () => {
      // `service_id` isn't in the generated query-param type for this route
      // (it arrives via the shared `Depends(get_source)` chain, which the
      // OpenAPI codegen doesn't surface as a named param) — same escape
      // hatch UsageLogClient uses for the same endpoint shape.
      const { data } = await client.GET('/api/admin/iceberg-info', {
        params: { query: { service_id: serviceId } as Record<string, string> },
      })
      return data
    },
    enabled: open && !isExecuting && !!serviceId,
    staleTime: 30_000,
  })

  if (!service) return null

  const canExecute = confirmText.trim() === service.name

  const handleExecute = () => {
    if (!canExecute) return
    setIsExecuting(true)
    // `?service_id=` (not the JSON body) is what backend.deps.get_source
    // resolves the target service from — this table lists every service,
    // not just the currently-active one, so the query param overrides
    // useSSE's default x-service-id (the globally active service) to make
    // sure we delete the row that was actually clicked. `confirm` is a
    // server-side belt-and-suspenders check that it matches.
    start(`/api/admin/reset-logs?service_id=${encodeURIComponent(service.service_id)}`, {
      confirm: service.service_id,
    })
  }

  const handleClose = (isOpen: boolean) => {
    if (status === 'streaming') return
    onOpenChange(isOpen)
    if (!isOpen && status === 'done') {
      if (onComplete) onComplete()
      // Full reload (matches TeardownDialog) rather than relying solely on
      // query invalidation — several cards (local cache size/file count,
      // Iceberg stats, dashboards) cache-serve a stale-but-not-yet-expired
      // value for a few seconds post-reset; a reload guarantees the admin
      // sees the post-delete state immediately instead of stale numbers.
      window.location.reload()
    }
  }

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className={cn("sm:max-w-lg", panelDialogContent)} showCloseButton={status !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2 text-destructive text-xl font-bold">
            <AlertTriangle className="h-6 w-6" />
            Delete Data: {service.name}
          </DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {isExecuting ? (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
              <div className="text-center space-y-2">
                <h3 className="text-lg font-semibold tracking-tight">Deleting Log Data</h3>
                <p className="text-sm text-muted-foreground">Please do not close this window until the process is complete.</p>
              </div>

              <SSEProgressView
                lines={lines}
                status={status}
                error={sseError}
                className="h-[320px]"
                progressLabel="Progress"
                doneMessage="Log data deleted. You may now close this window."
              />
            </div>
          ) : (
            <div className="p-8 space-y-6">
              <Alert variant="destructive" className="bg-destructive/5 text-destructive border-destructive/20">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription className="text-[13px] ml-1 font-medium">
                  This permanently deletes all of this service&apos;s log data — both local (the analytical
                  database and cache) and cloud-stored (the Iceberg log table in Fastly Object Storage).
                  New logs from the live edge will start populating immediately afterward. This cannot be
                  undone.
                </AlertDescription>
              </Alert>

              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-lg border border-destructive/20 bg-destructive/5 p-3 space-y-1">
                  <div className="flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                    <HardDrive className="h-3 w-3" /> Local
                  </div>
                  <div className="text-lg font-mono font-bold tracking-tight">
                    {formatBytes(service.duckdb_size_bytes ?? 0)}
                  </div>
                  <div className="text-[11px] text-muted-foreground">
                    {(service.cache_file_count ?? 0).toLocaleString()} file{service.cache_file_count === 1 ? '' : 's'}
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
                        {(icebergInfo?.data_files ?? 0).toLocaleString()} file{icebergInfo?.data_files === 1 ? '' : 's'}
                      </div>
                    </>
                  )}
                </div>
              </div>

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
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
          {!isExecuting ? (
            <>
              <Button variant="ghost" onClick={() => onOpenChange(false)} className="h-10 px-6 text-xs">Cancel</Button>
              <Button
                variant="destructive"
                className="h-10 px-8 font-bold"
                onClick={handleExecute}
                disabled={!canExecute}
                title={!canExecute ? `Type "${service.name}" to confirm` : undefined}
              >
                Delete Data
              </Button>
            </>
          ) : (
            <>
              {status !== 'streaming' && (
                 <Button variant="outline" onClick={() => handleClose(false)} className="h-10 px-6">
                   {status === 'done' ? 'Close' : 'Cancel'}
                 </Button>
              )}
              {status === 'streaming' && (
                <Button variant="outline" onClick={stop} className="h-10 px-6">Stop</Button>
              )}
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
