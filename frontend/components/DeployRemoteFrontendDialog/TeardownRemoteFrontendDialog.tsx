'use client'

import * as React from 'react'
import { Globe, AlertTriangle, Loader2 } from 'lucide-react'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { client, extractApiError } from '@/lib/api'
import { cn } from '@/lib/utils'
import { panelDialogContent, panelDialogHeaderSolid, panelDialogFooter } from '@/lib/panel-dialog'

interface TeardownRemoteFrontendDialogProps {
  serviceId: string
  domainName: string
  onSuccess: () => void
}

export function TeardownRemoteFrontendDialog({
  serviceId,
  domainName,
  onSuccess,
}: TeardownRemoteFrontendDialogProps) {
  const [open, setOpen] = React.useState(false)
  const [tearingDown, setTearingDown] = React.useState(false)
  const [error, setError] = React.useState('')
  const [tokenOverride, setTokenOverride] = React.useState('')

  const handleTeardown = async () => {
    setTearingDown(true)
    setError('')
    try {
      const { error: apiError } = await client.POST('/api/sharing/teardown-frontend', {
        body: {
          service_id: serviceId,
          token_override: tokenOverride || null,
        },
      })

      if (apiError) {
        throw new Error(extractApiError(apiError))
      }

      setOpen(false)
      onSuccess()
    } catch (err) {
      setError(extractApiError(err))
    } finally {
      setTearingDown(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="destructive" className="gap-1.5 border-destructive/20 hover:bg-destructive/5">
            <Globe className="h-4 w-4 text-destructive animate-pulse" />
            Teardown Frontend
          </Button>
        }
      />

      <DialogContent className={cn('sm:max-w-md', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2 text-xl font-bold text-destructive">
            <AlertTriangle className="h-5 w-5 text-destructive" />
            Teardown Remote Frontend
          </DialogTitle>
          <DialogDescription className="text-muted-foreground/90 mt-1.5 text-xs">
            Deactivate and delete the Fastly remote frontend proxy for this service.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0 px-6 py-5 space-y-4">
          <p className="text-sm leading-relaxed">
            You are about to completely tear down the remote frontend proxy deployed at{' '}
            <span className="font-mono font-semibold text-primary">{domainName}</span>.
          </p>

          <Alert variant="destructive" className="border-destructive/30 bg-destructive/5">
            <AlertTriangle className="h-4 w-4 text-destructive" />
            <AlertDescription className="text-xs font-medium text-destructive">
              This action is destructive and irreversible. It will deactivate the active version on Fastly
              and permanently delete the proxy service. Analysts will no longer be able to access the remote dashboard.
            </AlertDescription>
          </Alert>

          <div className="space-y-1.5">
            <label htmlFor="teardown-token" className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
              Fastly API Key Override (Optional)
            </label>
            <input
              id="teardown-token"
              type="password"
              placeholder="defaults to service configuration key"
              value={tokenOverride}
              onChange={(e) => setTokenOverride(e.target.value)}
              disabled={tearingDown}
              className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
            />
          </div>

          {error && (
            <Alert variant="destructive">
              <AlertDescription className="text-xs">{error}</AlertDescription>
            </Alert>
          )}
        </div>

        <DialogFooter className={cn('gap-2 border-t pt-4', panelDialogFooter)}>
          <Button
            variant="outline"
            onClick={() => setOpen(false)}
            disabled={tearingDown}
            className="w-full sm:w-auto"
          >
            Cancel
          </Button>
          <Button
            variant="destructive"
            onClick={handleTeardown}
            disabled={tearingDown}
            className="w-full sm:w-auto gap-1.5"
          >
            {tearingDown && <Loader2 className="h-3 w-3 animate-spin" />}
            {tearingDown ? 'Tearing down…' : 'Teardown Remote Service'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
