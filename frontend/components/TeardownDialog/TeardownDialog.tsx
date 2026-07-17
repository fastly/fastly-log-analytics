'use client'

import React, { useState, useEffect } from 'react'
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
import { Switch } from '@/components/ui/switch'
import {
  AlertTriangle,
  Trash2,
  AlertCircle
} from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEProgressView } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from '@/lib/panel-dialog'
import type { components } from '@/types/api.generated'

type ServiceConfig = components["schemas"]["ServiceConfig"]

interface TeardownDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onComplete?: () => void
}

function isAnalyst(service: ServiceConfig | null) {
  return service?.access_level === 'read_only'
}

export function TeardownDialog({ service, open, onOpenChange, onComplete }: TeardownDialogProps) {
  const [removeLogging] = useState(true)
  const [removeCdn] = useState(true)
  const [removeBucket, setRemoveBucket] = useState(true)
  const [removeCache, setRemoveCache] = useState(true)
  const [isExecuting, setIsExecuting] = useState(false)
  // Security: backend now requires a caller-supplied Fastly token with the
  // `global` scope for any teardown that touches Fastly (logging/CDN/bucket).
  // Server-stored credentials are no longer accepted as a fallback. The admin
  // must paste their own token each time so a CSRF or stolen-session attacker
  // cannot trigger a customer outage without also having a valid Fastly token.
  const [apiToken, setApiToken] = useState('')

  const { lines, status, error: sseError, start, stop, reset } = useSSE()

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      const id = setTimeout(() => {
        setIsExecuting(false)
        setApiToken('')
        reset()
      }, 300)
      return () => clearTimeout(id)
    }
  }, [open, reset])

  if (!service) return null

  const analyst = isAnalyst(service)
  // Security: teardown is now POST-only (CSRF defense). The token, service
  // id and removal flags travel in the request body, not the URL — keeps
  // the Fastly API token out of browser history, proxy access logs, and
  // the Referer header on any subsequent navigation.
  const teardownUrl = '/api/provision/teardown'
  const teardownBody: Record<string, unknown> = analyst
    ? {
        service_id: service.service_id,
        remove_logging: false,
        remove_cdn: false,
        remove_bucket: false,
        remove_cache: removeCache,
      }
    : {
        service_id: service.service_id,
        remove_logging: removeLogging,
        remove_cdn: removeCdn,
        remove_bucket: removeBucket,
        remove_cache: removeCache,
        token: apiToken,
      }

  const tokenRequired = !analyst && (removeLogging || removeCdn || removeBucket)
  const canExecute = !tokenRequired || apiToken.trim().length > 0

  const handleExecute = () => {
    if (!canExecute) return
    setIsExecuting(true)
    start(teardownUrl, teardownBody)
  }

  const handleClose = (isOpen: boolean) => {
    if (status === 'streaming') return
    onOpenChange(isOpen)
    if (!isOpen && status === 'done') {
      if (onComplete) onComplete()
      window.location.reload()
    }
  }

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className={cn("sm:max-w-2xl", panelDialogContent)} showCloseButton={status !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2 text-destructive text-xl font-bold">
            <AlertTriangle className="h-6 w-6" />
            Teardown: {service.name}
          </DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {isExecuting ? (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
               <div className="text-center space-y-2">
                  <h3 className="text-lg font-semibold tracking-tight">Executing Teardown Actions</h3>
                  <p className="text-sm text-muted-foreground">Please do not close this window until the process is complete.</p>
               </div>

               <SSEProgressView
                 lines={lines}
                 status={status}
                 error={sseError}
                 className="h-[400px]"
                 progressLabel="Progress"
                 doneMessage="Teardown completed successfully! You may now close this window."
               />
            </div>
          ) : analyst ? (
            <div className="p-8 space-y-8">
              <Alert variant="destructive" className="bg-destructive/5 text-destructive border-destructive/20">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription className="text-[13px] ml-1 font-medium">
                  This will remove your local analyst connection to this service. It does not affect the shared cloud data or the admin's configuration.
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
                      <div className="flex items-center justify-between group">
                        <div className="space-y-0.5">
                          <Label htmlFor="rem-logging" className="text-sm font-medium">Delete Fastly logging endpoint</Label>
                          <p className="text-[10px] text-muted-foreground">Removes the S3 logging configuration from the service.</p>
                        </div>
                        <Switch id="rem-logging" checked={true} disabled={true} className="opacity-50" />
                      </div>

                      <div className="flex items-center justify-between group">
                        <div className="space-y-0.5">
                          <Label htmlFor="rem-cdn" className="text-sm font-medium">Delete CDN VCL proxy service</Label>
                          <p className="text-[10px] text-muted-foreground">Permanently removes the secondary CDN service.</p>
                        </div>
                        <Switch id="rem-cdn" checked={true} disabled={true} className="opacity-50" />
                      </div>

                      <div className="flex items-center justify-between group">
                        <div className="space-y-0.5">
                          <Label htmlFor="rem-buck-new" className="text-sm font-medium cursor-pointer">Delete FOS bucket and all data</Label>
                          <p className="text-[10px] text-muted-foreground text-destructive font-medium">Warning: This cannot be undone. All logs will be lost.</p>
                        </div>
                        <Switch id="rem-buck-new" checked={removeBucket} onCheckedChange={setRemoveBucket} />
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
                title={!canExecute ? 'Enter your Fastly API token to proceed' : undefined}
              >
                Execute Teardown
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
