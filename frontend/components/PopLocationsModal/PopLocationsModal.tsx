'use client'

import React, { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogFooter,
  DialogTitle,
} from '@/components/ui/dialog'
import { ScrollArea } from '@/components/ui/scroll-area'
import { MapPin, RefreshCw, CheckCircle2, AlertCircle } from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderMuted,
} from '@/lib/panel-dialog'

interface PopLocationsModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function PopLocationsModal({ open, onOpenChange }: PopLocationsModalProps) {
  const [apiKey, setApiKey] = useState('')
  const queryClient = useQueryClient()

  const { data, isLoading } = useQuery({
    queryKey: ['pop-locations'],
    queryFn: async () => {
      const { data } = await client.GET("/api/admin/pop-locations")
      return data as any
    },
    enabled: open,
  })

  const mutation = useMutation({
    mutationFn: async () => {
      const { data } = await client.POST("/api/admin/pop-locations/refresh", {
        body: { token: apiKey }
      })
      return data as any
    },
    onSuccess: (result) => {
      queryClient.setQueryData(['pop-locations'], { pops: result?.pops || [] })
      setApiKey('')
    },
  })

  const pops: any[] = data?.pops || []
  const withCoords = pops.filter(p =>
    (p.coordinates?.latitude ?? p.attributes?.latitude) != null
  )

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn("sm:max-w-2xl", panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderMuted}>
          <DialogTitle className="flex items-center gap-2">
            <MapPin className="h-4 w-4" />
            POP Location Data
          </DialogTitle>
          <p className="text-sm text-muted-foreground mt-1">
            POP coordinates power the Impossible Distance insight — they detect when a client's
            claimed geo location is physically impossible given their TCP RTT. Refresh when
            Fastly adds new POPs.
          </p>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
          {/* Cache status */}
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium">Cached POPs</span>
            <div className="flex items-center gap-2">
              <Badge
                variant={withCoords.length > 0 ? 'success' : 'secondary'}
                className="text-[10px] px-1.5"
              >
                {withCoords.length} with coordinates
              </Badge>
              {pops.length > 0 && (
                <Badge variant="outline" className="text-[10px] px-1.5">
                  {pops.length} total
                </Badge>
              )}
            </div>
          </div>

          {isLoading ? (
            <div className="h-48 flex items-center justify-center text-muted-foreground text-sm animate-pulse border rounded-lg">
              Loading...
            </div>
          ) : pops.length === 0 ? (
            <div className="h-48 flex flex-col items-center justify-center gap-2 border rounded-lg bg-muted/20">
              <AlertCircle className="h-5 w-5 text-muted-foreground" />
              <p className="text-sm text-muted-foreground">No POP data cached yet.</p>
              <p className="text-xs text-muted-foreground">Enter your Fastly API key below to fetch it.</p>
            </div>
          ) : (
            <ScrollArea className="h-52 border rounded-lg">
              <div className="divide-y">
                {pops.map((pop: any) => {
                  const code = pop.code || pop.id || '?'
                  const name = pop.name || pop.attributes?.name || ''
                  const lat = pop.coordinates?.latitude ?? pop.attributes?.latitude
                  const lon = pop.coordinates?.longitude ?? pop.attributes?.longitude
                  const hasGeo = lat != null && lon != null
                  return (
                    <div key={code} className="flex items-center gap-3 px-3 py-1.5 hover:bg-muted/30">
                      <span className="font-mono text-xs font-bold w-10 shrink-0 text-foreground">{code}</span>
                      <span className="text-xs text-muted-foreground flex-1 truncate">{name}</span>
                      {hasGeo ? (
                        <span className="text-[10px] font-mono text-muted-foreground/70 shrink-0 tabular-nums">
                          {Number(lat).toFixed(2)}, {Number(lon).toFixed(2)}
                        </span>
                      ) : (
                        <span className="text-[10px] text-destructive/70 shrink-0">no coords</span>
                      )}
                    </div>
                  )
                })}
              </div>
            </ScrollArea>
          )}

          {/* Update form */}
          <div className="space-y-2 pt-2 border-t">
            <Label htmlFor="pop-api-key" className="text-sm font-medium">
              Update from Fastly API
            </Label>
            <p className="text-xs text-muted-foreground">
              A read-only Fastly API token is sufficient. The key is used once to call{' '}
              <code className="font-mono">/datacenters</code> and is not stored.
            </p>
            <div className="flex gap-2">
              <input
                id="pop-api-key"
                type="password"
                placeholder="Fastly API key..."
                value={apiKey}
                onChange={e => setApiKey(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && apiKey.trim() && !mutation.isPending && mutation.mutate()}
                className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 font-mono"
              />
              <Button
                onClick={() => mutation.mutate()}
                disabled={!apiKey.trim() || mutation.isPending}
                className="shrink-0"
              >
                <RefreshCw className={`h-4 w-4 mr-2 ${mutation.isPending ? 'animate-spin' : ''}`} />
                {mutation.isPending ? 'Fetching...' : 'Update'}
              </Button>
            </div>

            {mutation.isSuccess && (
              <div className="flex items-center gap-2 text-sm text-green-600 dark:text-green-400">
                <CheckCircle2 className="h-4 w-4" />
                Updated — {withCoords.length} POPs with coordinates cached.
              </div>
            )}
            {mutation.isError && (
              <div className="flex items-center gap-2 text-sm text-destructive">
                <AlertCircle className="h-4 w-4" />
                {(mutation.error as any)?.message || 'Update failed. Check your API key.'}
              </div>
            )}
          </div>
        </div>

        <DialogFooter className={panelDialogFooter} />
      </DialogContent>
    </Dialog>
  )
}
