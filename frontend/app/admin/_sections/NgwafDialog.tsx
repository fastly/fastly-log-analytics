'use client'
import React, { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'
import { panelDialogContent, panelDialogHeaderSolid, panelDialogFooter } from '@/lib/panel-dialog'
import { cn } from '@/lib/utils'
import { Bot } from 'lucide-react'
import type { ServiceConfig } from '@/types/api'


interface Props {
  service: ServiceConfig | null
  onClose: () => void
}

/** Keyed on the service id so opening a different service starts from fresh form state. */
export function NgwafDialog(props: Props) {
  return <NgwafDialogBody key={props.service?.service_id ?? ''} {...props} />
}

function NgwafDialogBody({ service, onClose }: Props) {
  const queryClient = useQueryClient()
  const [ngwafWorkspaceId, setNgwafWorkspaceId] = useState(service?.ngwaf_workspace_id || '')
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<{ id: string; name: string }[]>([])
  const [ngwafFetchError, setNgwafFetchError] = useState('')
  const [ngwafSaved, setNgwafSaved] = useState(false)
  // Security: backend now requires a caller-supplied Fastly token for
  // the PATCH that rebinds the workspace. The admin enters the same token
  // they use to fetch the workspaces list, so the constant-time stored-key
  // match in the backend lets through the legitimate admin flow without
  // requiring them to remember it from somewhere else.
  const [ngwafApiToken, setNgwafApiToken] = useState('')

  const fetchWorkspacesMutation = useMutation({
    mutationFn: async ({ serviceId, token }: { serviceId: string; token: string }) => {
      const data = await unwrapClient(client.GET("/api/provision/ngwaf-workspaces", {
        params: { query: { service_id: serviceId } },
        headers: { Authorization: `Bearer ${token}` },
      }))
      return (data?.workspaces || []).flatMap((w) => (w.id && w.name ? [{ id: w.id, name: w.name }] : []))
    },
    onMutate: () => {
      setNgwafWorkspaces([])
      setNgwafFetchError('')
    },
    onSuccess: (workspaces) => {
      setNgwafWorkspaces(workspaces)
    },
    onError: (e) => {
      setNgwafFetchError(extractApiError(e) || 'Could not load workspaces')
    },
  })

  const saveWorkspaceMutation = useMutation({
    mutationFn: async ({ serviceId, token, workspaceId }: { serviceId: string; token: string; workspaceId: string | null }) => {
      return unwrapClient(client.PATCH("/api/provision/services/{service_id}/ngwaf-workspace", {
        params: { path: { service_id: serviceId } },
        headers: { Authorization: `Bearer ${token}` },
        body: { ngwaf_workspace_id: workspaceId },
      }))
    },
    onSuccess: () => {
      setNgwafSaved(true)
      queryClient.invalidateQueries({ queryKey: queryKeys.services() })
    },
    onError: (e) => {
      setNgwafFetchError(extractApiError(e) || 'Failed to save')
    },
  })

  const ngwafFetching = fetchWorkspacesMutation.isPending
  const ngwafSaving = saveWorkspaceMutation.isPending

  return (
    <Dialog open={!!service} onOpenChange={(open) => { if (!open) onClose() }}>
      <DialogContent className={cn("sm:max-w-md", panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2">
            <Bot className="h-5 w-5 text-primary" />
            NGWAF Bot Enrichment
          </DialogTitle>
          <DialogDescription>
            Set the NGWAF workspace for <strong>{service?.name}</strong>. When configured, the bot sync cron will enrich log data with specific bot names from Fastly NGWAF.
          </DialogDescription>
        </DialogHeader>

        <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1">
          {/* Security: token must be supplied before workspace fetch
              AND before workspace save. Single input drives both. */}
          {service && !ngwafSaved && (
            <div className="space-y-1">
              <Label htmlFor="ngwaf-api-token" className="text-xs font-semibold">
                Fastly API token
              </Label>
              <p className="text-[10px] text-muted-foreground">
                Required to list AND save NGWAF workspace bindings (security /).
              </p>
              <div className="flex gap-2">
                <Input
                  id="ngwaf-api-token"
                  type="password"
                  placeholder="Fastly API token"
                  value={ngwafApiToken}
                  onChange={(e) => setNgwafApiToken(e.target.value)}
                  className="h-8 font-mono text-xs flex-1"
                  autoComplete="off"
                />
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!ngwafApiToken.trim() || ngwafFetching}
                  onClick={() => {
                    if (!service) return
                    fetchWorkspacesMutation.mutate({ serviceId: service.service_id, token: ngwafApiToken })
                  }}
                  className="h-8 text-xs"
                >
                  {ngwafFetching ? 'Loading…' : 'Load'}
                </Button>
              </div>
            </div>
          )}

          {ngwafFetching ? (
            <p className="text-xs text-muted-foreground animate-pulse">Loading workspaces…</p>
          ) : ngwafWorkspaces.length > 0 ? (
            <div className="space-y-1">
              <Label className="text-xs font-semibold">Select workspace</Label>
              <Select value={ngwafWorkspaceId} onValueChange={(v) => setNgwafWorkspaceId(v ?? '')}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue placeholder="Choose a workspace…" />
                </SelectTrigger>
                <SelectContent>
                  {ngwafWorkspaces.map(w => (
                    <SelectItem key={w.id} value={w.id} className="text-xs">
                      {w.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          ) : ngwafFetchError ? (
            <p className="text-xs text-destructive">{ngwafFetchError}</p>
          ) : null}

          {ngwafSaved && (
            <p className="text-xs text-green-600 font-medium">Workspace saved. The NGWAF sync cron will start on the next scheduler tick.</p>
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
          {ngwafSaved ? (
            <Button size="sm" onClick={onClose}>Close</Button>
          ) : (
            <>
              <Button variant="outline" size="sm" onClick={onClose}>Cancel</Button>
              <Button
                size="sm"
                disabled={ngwafSaving || !ngwafApiToken.trim()}
                title={!ngwafApiToken.trim() ? 'Enter your Fastly API token to save' : undefined}
                onClick={() => {
                  if (!service) return
                  saveWorkspaceMutation.mutate({
                    serviceId: service.service_id,
                    token: ngwafApiToken,
                    workspaceId: ngwafWorkspaceId.trim() || null,
                  })
                }}
              >
                {ngwafSaving ? 'Saving…' : 'Save'}
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
