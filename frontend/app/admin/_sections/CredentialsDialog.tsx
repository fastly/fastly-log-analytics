'use client'
import React, { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import type { components } from '@/types/api.generated'
import { client } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'

type ServiceConfig = components["schemas"]["ServiceConfig"]

interface Props {
  service: ServiceConfig | null
  onClose: () => void
  /** Re-initialized whenever a new service is opened (token vs manual). */
  initialMode: 'token' | 'manual'
}

export function CredentialsDialog({ service, onClose, initialMode }: Props) {
  const [credMode, setCredMode] = useState<'token' | 'manual'>(initialMode)
  const [credApiToken, setCredApiToken] = useState('')
  const [credAccessKey, setCredAccessKey] = useState('')
  const [credSecretKey, setCredSecretKey] = useState('')

  // Reset local state whenever a new service is opened.
  React.useEffect(() => {
    setCredMode(initialMode)
    setCredApiToken('')
    setCredAccessKey('')
    setCredSecretKey('')
  }, [service?.service_id, initialMode])

  const credentialsMutation = useMutation({
    mutationFn: async ({ service_id, payload }: { service_id: string; payload: { api_token: string } | { access_key: string; secret_key: string } }) => {
      const { data } = await client.PATCH("/api/services/{service_id}/credentials", {
        params: { path: { service_id } },
        body: payload as any
      })
      return data
    },
    onSuccess: () => {
      onClose()
    },
  })

  function handleClose() {
    onClose()
    credentialsMutation.reset()
  }

  return (
    <Dialog open={!!service} onOpenChange={(open) => { if (!open) handleClose() }}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Rotate FOS Credentials</DialogTitle>
          <DialogDescription>
            Replace the Fastly Object Storage access key for <strong>{service?.name}</strong>.
            {service?.access_level === 'read_write'
              ? ' Use your Fastly API token to auto-generate a new key, or enter one manually.'
              : ' Enter the new key credentials manually.'}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          {/* Mode toggle — admins only */}
          {service?.access_level === 'read_write' && (
            <div className="flex rounded-md border overflow-hidden text-xs font-semibold">
              <button
                type="button"
                className={`flex-1 py-1.5 transition-colors ${credMode === 'token' ? 'bg-primary text-primary-foreground' : 'hover:bg-muted text-muted-foreground'}`}
                onClick={() => { setCredMode('token'); credentialsMutation.reset() }}
              >
                Auto (API Token)
              </button>
              <button
                type="button"
                className={`flex-1 py-1.5 transition-colors ${credMode === 'manual' ? 'bg-primary text-primary-foreground' : 'hover:bg-muted text-muted-foreground'}`}
                onClick={() => { setCredMode('manual'); credentialsMutation.reset() }}
              >
                Manual
              </button>
            </div>
          )}

          {/* Token mode */}
          {credMode === 'token' && service?.access_level === 'read_write' && (
            <div className="space-y-1.5">
              <Label htmlFor="cred-api-token" className="text-sm">Fastly API Token</Label>
              <p className="text-xs text-muted-foreground">
                A new <code>read-write-objects</code> FOS key will be created for this bucket. The old key will be deleted automatically.
              </p>
              <Input
                id="cred-api-token"
                type="password"
                placeholder="Fastly API token"
                value={credApiToken}
                onChange={(e) => setCredApiToken(e.target.value)}
                className="font-mono text-sm"
              />
            </div>
          )}

          {/* Manual mode */}
          {(credMode === 'manual' || service?.access_level !== 'read_write') && (
            <>
              <div className="space-y-1.5">
                <Label htmlFor="cred-access-key" className="text-sm">Access Key ID</Label>
                <Input
                  id="cred-access-key"
                  placeholder="FOS access key ID"
                  value={credAccessKey}
                  onChange={(e) => setCredAccessKey(e.target.value)}
                  className="font-mono text-sm"
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="cred-secret-key" className="text-sm">Secret Access Key</Label>
                <Input
                  id="cred-secret-key"
                  type="password"
                  placeholder="FOS secret access key"
                  value={credSecretKey}
                  onChange={(e) => setCredSecretKey(e.target.value)}
                  className="font-mono text-sm"
                />
              </div>
            </>
          )}

          {credentialsMutation.isError && (
            <p className="text-sm text-destructive">
              {(credentialsMutation.error as any)?.message ?? 'Failed to update credentials.'}
            </p>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleClose}>Cancel</Button>
          <Button
            disabled={
              credentialsMutation.isPending ||
              (credMode === 'token' ? !credApiToken : !credAccessKey || !credSecretKey)
            }
            onClick={() => {
              if (!service) return
              const payload = credMode === 'token'
                ? { api_token: credApiToken }
                : { access_key: credAccessKey, secret_key: credSecretKey }
              credentialsMutation.mutate({ service_id: service.service_id, payload })
            }}
          >
            {credentialsMutation.isPending
              ? (credMode === 'token' ? 'Creating key…' : 'Validating…')
              : (credMode === 'token' ? 'Rotate Key' : 'Save Credentials')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
