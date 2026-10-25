'use client'
import React, { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useRouter } from 'next/navigation'
import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { cronCacheBust } from '@/lib/admin-stream-apply'
import { useMounted } from '@/hooks/useBootstrap'
import { useServiceStore } from '@/stores/filterStore'
// Direct import (not via the barrel) so the page bundle drops the
// @dnd-kit tree that the reorder-enabled DataTable pulls in.
import { DataTableReadonly as DataTable } from '@/components/DataTable'
import { ProvisionWizard } from '@/components/ProvisionWizard/ProvisionWizard'
import { TeardownDialog } from '@/components/TeardownDialog'
import { DeleteDataDialog } from '@/components/TeardownDialog'
import { CronSettingsModal } from '@/components/CronSettingsModal/CronSettingsModal'
import { LogSettingsModal } from '@/components/LogSettingsModal/LogSettingsModal'
import { InviteAnalystDialog } from '@/components/InviteAnalystDialog'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
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
import { cn, panelDialogContent, panelDialogHeaderSolid, panelDialogFooter } from '@/lib/utils'
import { AlertCircle, Bot, Plus } from 'lucide-react'

import { buildServiceColumns } from './ServicesTableColumns'
import type { ServiceConfig } from '@/types/api'

interface CredentialsDialogProps {
  service: ServiceConfig | null
  onClose: () => void
  /** Re-initialized whenever a new service is opened (token vs manual). */
  initialMode: 'token' | 'manual'
}

/** Keyed so opening a different service (or mode) starts from fresh form state. */
export function CredentialsDialog(props: CredentialsDialogProps) {
  return <CredentialsDialogBody key={`${props.service?.service_id ?? ''}|${props.initialMode}`} {...props} />
}

function CredentialsDialogBody({ service, onClose, initialMode }: CredentialsDialogProps) {
  const [credMode, setCredMode] = useState<'token' | 'manual'>(initialMode)
  const [credApiToken, setCredApiToken] = useState('')
  const [credAccessKey, setCredAccessKey] = useState('')
  const [credSecretKey, setCredSecretKey] = useState('')

  const credentialsMutation = useMutation({
    mutationFn: async ({ service_id, payload }: { service_id: string; payload: { api_token: string } | { access_key: string; secret_key: string } }) => {
      return unwrapClient(client.PATCH("/api/services/{service_id}/credentials", {
        params: { path: { service_id } },
        // The body model defaults each field to ""; the generated type marks them all required.
        body: { api_token: '', access_key: '', secret_key: '', ...payload }
      }))
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
      <DialogContent className={cn("sm:max-w-md", panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle>Rotate FOS Credentials</DialogTitle>
          <DialogDescription>
            Replace the Fastly Object Storage access key for <strong>{service?.name}</strong>.
            {service?.access_level === 'read_write'
              ? ' Use your Fastly API token to auto-generate a new key, or enter one manually.'
              : ' Enter the new key credentials manually.'}
          </DialogDescription>
        </DialogHeader>

        <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1">
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
              {credentialsMutation.error?.message ?? 'Failed to update credentials.'}
            </p>
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
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

interface NgwafDialogProps {
  service: ServiceConfig | null
  onClose: () => void
}

/** Keyed on the service id so opening a different service starts from fresh form state. */
export function NgwafDialog(props: NgwafDialogProps) {
  return <NgwafDialogBody key={props.service?.service_id ?? ''} {...props} />
}

function NgwafDialogBody({ service, onClose }: NgwafDialogProps) {
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


export function ServicesTable() {
  const queryClient = useQueryClient()
  const mounted = useMounted()
  // Separate selectors — each subscribes to only its slice. Destructuring
  // useServiceStore() rebuilds the table on every setServices/setInitialized.
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const setActiveServiceId = useServiceStore(s => s.setActiveServiceId)
  const router = useRouter()
  const [cronService, setCronService] = useState<ServiceConfig | null>(null)
  const [settingsService, setSettingsService] = useState<ServiceConfig | null>(null)
  const [teardownService, setTeardownService] = useState<ServiceConfig | null>(null)
  const [deleteDataService, setDeleteDataService] = useState<ServiceConfig | null>(null)
  const [inviteService, setInviteService] = useState<ServiceConfig | null>(null)
  const [credentialsService, setCredentialsService] = useState<ServiceConfig | null>(null)
  // Cached "initial mode" for the credentials dialog — picked when the
  // dialog opens so the child can re-init local state for each new service.
  const [credInitialMode, setCredInitialMode] = useState<'token' | 'manual'>('token')
  const [wizardOpen, setWizardOpen] = useState(false)
  const [ngwafService, setNgwafService] = useState<ServiceConfig | null>(null)

  function openCredentials(service: ServiceConfig) {
    setCredInitialMode(service.access_level === 'read_write' ? 'token' : 'manual')
    setCredentialsService(service)
  }

  // Switch the active service + navigate to its dashboard. Invalidates the
  // bootstrap query so its services list / active_service_id refresh BEFORE
  // useBootstrap's reconcile effect runs — otherwise switching to a freshly-
  // added service whose id isn't yet in the cached (5-min staleTime)
  // bootstrap.services makes the reconcile treat the selection as unknown and
  // revert it to the previous service, bouncing the admin back + to /admin.
  // Mirrors the teardown invalidate below.
  const switchToService = React.useCallback(
    (id: string) => {
      setActiveServiceId(id)
      queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
      router.push(`/dashboard?service=${id}`)
    },
    [setActiveServiceId, queryClient, router],
  )

  const openRum = React.useCallback(
    (s: ServiceConfig) => {
      setActiveServiceId(s.service_id)
      router.push(`/admin/rum?service=${s.service_id}`)
    },
    [setActiveServiceId, router],
  )

  const { data: services, isLoading, isFetching, isError, error } = useQuery({
    queryKey: queryKeys.services(),
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET("/api/services", { signal }))
    },
  })

  const isRefreshing = isFetching && !isLoading

  const handleRefreshAll = React.useCallback(() => {
    void queryClient.invalidateQueries({ queryKey: queryKeys.services() })
    void queryClient.invalidateQueries({ queryKey: queryKeys.admin.overviewAll() })
    void queryClient.invalidateQueries({ queryKey: queryKeys.admin.healthSnapshot() })
    void queryClient.invalidateQueries({ queryKey: queryKeys.syncStatus(activeServiceId) })
  }, [queryClient, activeServiceId])

  const columns = React.useMemo(
    () => buildServiceColumns({
      activeServiceId,
      switchToService,
      servicesLength: services?.services?.length || 0,
      setCronService,
      setSettingsService,
      setDeleteDataService,
      setTeardownService,
      setInviteService,
      openNgwaf: setNgwafService,
      openRum,
      openCredentials,
    }),
    [activeServiceId, switchToService, services?.services?.length, openRum, openCredentials],
  )

  return (
    <>
      <div className="space-y-4" data-testid="admin-services-section">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <h2 className="text-xl font-semibold tracking-tight">Service Management</h2>
            <Button size="sm" onClick={() => setWizardOpen(true)}>
              <Plus className="h-4 w-4 mr-1" /> Add Service
            </Button>
          </div>
          <Button
            size="sm"
            variant="outline"
            data-testid="admin-overview-refresh-btn"
            onClick={handleRefreshAll}
            disabled={isFetching}
          >
            Refresh Status
          </Button>
        </div>

        {isLoading && (
          <div
            className="text-xs text-muted-foreground"
            role="status"
            aria-live="polite"
            data-testid="services-loading-status"
          >
            Loading service configs...
          </div>
        )}

        {isError && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Couldn&apos;t load services</AlertTitle>
            <AlertDescription className="text-xs">
              {error instanceof Error ? error.message : 'The backend returned an error. The list below may be incomplete — investigate before provisioning new services.'}
            </AlertDescription>
          </Alert>
        )}

        <div
          data-testid="services-table-container"
          className={`border rounded-lg bg-card shadow-sm overflow-hidden transition-opacity ${
            isRefreshing ? 'opacity-40 pointer-events-none' : ''
          }`}
        >
          <DataTable
            columns={columns}
            data={services?.services || []}
            isLoading={isLoading}
            searchKey="name"
            emptyMessage="No services yet"
            emptyHint="Add a service to get started."
          />
        </div>
      </div>

      <ProvisionWizard
        open={wizardOpen}
        onOpenChange={setWizardOpen}
      />

      {cronService && (
        <CronSettingsModal
          service={cronService}
          open={!!cronService}
          onOpenChange={(open) => !open && setCronService(null)}
        />
      )}

      {settingsService && (
        <LogSettingsModal
          service={settingsService}
          open={!!settingsService}
          onOpenChange={(open) => !open && setSettingsService(null)}
        />
      )}

      <InviteAnalystDialog
        service={inviteService}
        open={!!inviteService}
        onOpenChange={(open) => !open && setInviteService(null)}
      />

      <TeardownDialog
        service={teardownService}
        open={!!teardownService}
        onOpenChange={(open) => !open && setTeardownService(null)}
        onComplete={() => {
          queryClient.invalidateQueries({ queryKey: queryKeys.services() })
          queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
          setTeardownService(null)
        }}
      />

      <DeleteDataDialog
        service={deleteDataService}
        open={!!deleteDataService}
        onOpenChange={(open) => !open && setDeleteDataService(null)}
        onComplete={() => {
          cronCacheBust(queryClient, deleteDataService?.service_id)
          queryClient.invalidateQueries({ queryKey: queryKeys.services() })
          setDeleteDataService(null)
        }}
      />

      <CredentialsDialog
        service={credentialsService}
        initialMode={credInitialMode}
        onClose={() => setCredentialsService(null)}
      />

      <NgwafDialog
        service={ngwafService}
        onClose={() => setNgwafService(null)}
      />
    </>
  )
}
