'use client'
import React, { useState, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import type { components } from '@/types/api.generated'
import { client } from '@/lib/api'
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
import { Bot } from 'lucide-react'

type ServiceConfig = components["schemas"]["ServiceConfig"]

interface Props {
  service: ServiceConfig | null
  onClose: () => void
}

export function NgwafDialog({ service, onClose }: Props) {
  const queryClient = useQueryClient()
  const [ngwafWorkspaceId, setNgwafWorkspaceId] = useState('')
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<{ id: string; name: string }[]>([])
  const [ngwafFetchError, setNgwafFetchError] = useState('')
  const [ngwafFetching, setNgwafFetching] = useState(false)
  const [ngwafSaving, setNgwafSaving] = useState(false)
  const [ngwafSaved, setNgwafSaved] = useState(false)
  // Security: backend now requires a caller-supplied Fastly token for
  // the PATCH that rebinds the workspace. The admin enters the same token
  // they use to fetch the workspaces list, so the constant-time stored-key
  // match in the backend lets through the legitimate admin flow without
  // requiring them to remember it from somewhere else.
  const [ngwafApiToken, setNgwafApiToken] = useState('')

  // Re-init whenever a new service is opened.
  useEffect(() => {
    if (!service) return
    setNgwafWorkspaceId(service.ngwaf_workspace_id || '')
    setNgwafWorkspaces([])
    setNgwafFetchError('')
    setNgwafSaved(false)
    setNgwafApiToken('')
  }, [service?.service_id])

  return (
    <Dialog open={!!service} onOpenChange={(open) => { if (!open) onClose() }}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Bot className="h-5 w-5 text-primary" />
            NGWAF Bot Enrichment
          </DialogTitle>
          <DialogDescription>
            Set the NGWAF workspace for <strong>{service?.name}</strong>. When configured, the bot sync cron will enrich log data with specific bot names from Fastly NGWAF.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
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
                  onClick={async () => {
                    if (!service) return
                    setNgwafWorkspaces([])
                    setNgwafFetchError('')
                    setNgwafFetching(true)
                    try {
                      const { data } = await client.GET("/api/provision/ngwaf-workspaces" as any, {
                        params: { query: { service_id: service.service_id } },
                        headers: { Authorization: `Bearer ${ngwafApiToken}` }
                      })
                      setNgwafWorkspaces((data as any)?.workspaces || [])
                    } catch (e: any) {
                      setNgwafFetchError(e?.message || 'Could not load workspaces')
                    } finally {
                      setNgwafFetching(false)
                    }
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

        <DialogFooter>
          {ngwafSaved ? (
            <Button size="sm" onClick={onClose}>Close</Button>
          ) : (
            <>
              <Button variant="outline" size="sm" onClick={onClose}>Cancel</Button>
              <Button
                size="sm"
                disabled={ngwafSaving || !ngwafApiToken.trim()}
                title={!ngwafApiToken.trim() ? 'Enter your Fastly API token to save' : undefined}
                onClick={async () => {
                  if (!service) return
                  setNgwafSaving(true)
                  try {
                    // Security: backend requires a Fastly token bound
                    // to this service. We pass whatever token the admin
                    // entered above; backend accepts either the stored key
                    // (constant-time match) or a token with the 'global'
                    // scope on this service.
                    await client.PATCH("/api/provision/services/{service_id}/ngwaf-workspace" as any, {
                      params: {
                        path: { service_id: service.service_id },
                      },
                      headers: { Authorization: `Bearer ${ngwafApiToken}` },
                      body: { ngwaf_workspace_id: ngwafWorkspaceId.trim() || null } as any,
                    })
                    setNgwafSaved(true)
                    queryClient.invalidateQueries({ queryKey: ['services'] })
                  } catch (e: any) {
                    setNgwafFetchError(e?.message || 'Failed to save')
                  } finally {
                    setNgwafSaving(false)
                  }
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
