'use client'
import React, { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useRouter } from 'next/navigation'
import { AlertCircle, Bot, ChevronDown, CloudDownload, Eraser, ExternalLink, Eye, KeyRound, Play, Plus, Settings, Settings2, Trash2 } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import type { ServiceConfig } from '@/types/api'
import { adminFetch, client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { cn, downloadBlob, formatBytes, formatCount, panelDialogContent, panelDialogFooter, panelDialogHeaderSolid } from '@/lib/utils'
import { useMounted } from '@/hooks/useBootstrap'
import { cronCacheBust } from '@/hooks/useSSE'
import { useServiceStore } from '@/stores/filterStore'
import { DataTableReadonly as DataTable } from '@/components/DataTable'
import { ProvisionWizard } from '@/components/ProvisionWizard/ProvisionWizard'
import { DeleteDataDialog, TeardownDialog } from '@/components/TeardownDialog'
import { CronSettingsModal } from '@/components/CronSettingsModal/CronSettingsModal'
import { LogSettingsModal } from '@/components/LogSettingsModal/LogSettingsModal'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

interface ServiceColumnDeps {
  activeServiceId: string | null; switchToService: (id: string) => void; servicesLength: number
  setCronService: (s: ServiceConfig) => void; setSettingsService: (s: ServiceConfig) => void
  setDeleteDataService: (s: ServiceConfig) => void; setTeardownService: (s: ServiceConfig) => void
  openNgwaf: (s: ServiceConfig) => void
  openRum?: (s: ServiceConfig) => void; openCredentials: (s: ServiceConfig) => void
}

function MonoExternalCell({ text, href, title }: { text: string; href: string; title: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <span className="font-mono text-xs text-muted-foreground">{text}</span>
      <a href={href} target="_blank" rel="noreferrer" className="text-muted-foreground hover:text-foreground opacity-50 hover:opacity-100 transition-opacity" title={title}>
        <ExternalLink className="h-3 w-3" />
      </a>
    </div>
  )
}

function buildServiceColumns(deps: ServiceColumnDeps): ColumnDef<ServiceConfig>[] {
  const { activeServiceId, switchToService, servicesLength, setCronService, setSettingsService, setDeleteDataService, setTeardownService, openNgwaf, openRum, openCredentials } = deps
  return [
    {
      accessorKey: 'name', header: 'Service Name', size: 200,
      cell: ({ row }) => (
        <div className="flex items-center gap-2 font-medium">
          {row.original.service_id === activeServiceId && <Badge variant="default" className="h-5 px-1.5 text-[10px] uppercase font-bold bg-blue-600 hover:bg-blue-700 shadow-none border-none">Active</Badge>}
          {row.getValue('name')}
        </div>
      ),
    },
    {
      accessorKey: 'service_id', header: 'ID', size: 160,
      cell: ({ row }) => { const id = row.getValue('service_id') as string; return <MonoExternalCell text={id} href={`https://manage.fastly.com/configure/services/${id}`} title="View Service in Fastly" /> },
    },
    {
      accessorKey: 'fos_bucket', header: 'FOS Bucket', size: 180,
      cell: ({ row }) => <MonoExternalCell text={row.getValue('fos_bucket') as string} href="https://manage.fastly.com/resources/object-storage/buckets" title="View Object Storage in Fastly" />,
    },
    {
      id: 'local_cache', accessorFn: (row) => row.duckdb_size_bytes, header: 'Local Cache', size: 180,
      cell: ({ row }) => {
        const size = row.original.duckdb_size_bytes, files = row.original.cache_file_count || 0, rows = row.original.log_row_count || 0
        if (!size) return <span className="text-xs text-muted-foreground italic">No cache</span>
        return (
          <div className="flex flex-col gap-0.5">
            <span className="font-mono text-xs tabular-nums text-muted-foreground">{formatBytes(size)}</span>
            <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">
              <span className="flex items-center gap-1">
                {formatCount(files)} file{files === 1 ? '' : 's'}
                {files > 0 && (
                  <Button variant="ghost" size="icon" aria-label="Download local cache as ZIP" className="h-4 w-4 p-0 ml-0.5 hover:bg-transparent text-muted-foreground hover:text-primary transition-colors" title="Download local cache as ZIP" onClick={(e) => {
                    e.stopPropagation(); const sid = row.original.service_id
                    void adminFetch(`/api/download-all?service_id=${encodeURIComponent(sid)}&include=local`).then(async (res) => { if (res.ok) downloadBlob(await res.blob(), `${sid}_local_cache.zip`) }).catch(() => {})
                  }}><CloudDownload className="h-3 w-3" /></Button>
                )}
              </span>
              <span>•</span><span>{formatCount(rows)} rows</span>
            </div>
          </div>
        )
      },
    },
    {
      id: 'status', accessorFn: (row) => (row.cron_sync?.enabled ? 1 : 0), header: 'Cron Sync', size: 140,
      cell: ({ row: { original: service } }) => (
        <div className="flex items-center gap-2">
          {service.cron_sync?.enabled ? <Badge variant="success" className="shadow-none px-1.5 py-0 uppercase text-[10px] font-bold">Enabled</Badge> : <Badge variant="secondary" className="px-1.5 py-0 shadow-none uppercase text-[10px] font-bold">Disabled</Badge>}
          <Button variant="ghost" size="icon" aria-label="Cron sync settings" className="h-6 w-6 hover:bg-muted cursor-pointer" onClick={() => setCronService(service)} title="Cron Sync Settings"><Settings className="h-3 w-3 text-muted-foreground" /></Button>
        </div>
      ),
    },
    {
      id: 'actions', header: 'Actions', size: servicesLength > 0 ? 220 : 120,
      cell: ({ row: { original: service } }) => {
        const isActive = service.service_id === activeServiceId
        return (
          <div className="flex items-center gap-2">
            <DropdownMenu>
              <DropdownMenuTrigger render={<Button variant="outline" size="sm" className="h-8 gap-1.5 px-3 font-bold uppercase text-[10px] tracking-wider">Manage <ChevronDown className="h-3.5 w-3.5" /></Button>} />
              <DropdownMenuContent align="end" className="w-52">
                {isActive && <DropdownMenuItem disabled><Play className="mr-2 h-4 w-4" /> Active Service</DropdownMenuItem>}
                <DropdownMenuItem onClick={() => setSettingsService(service)}><Settings2 className="mr-2 h-4 w-4" /> Log Settings</DropdownMenuItem>
                {openRum && <DropdownMenuItem onClick={() => openRum(service)}><Eye className="mr-2 h-4 w-4" /> RUM Configuration</DropdownMenuItem>}
                <DropdownMenuItem onClick={() => openNgwaf(service)}><Bot className="mr-2 h-4 w-4" /> NGWAF Config</DropdownMenuItem>
                <DropdownMenuItem onClick={() => openCredentials(service)}><KeyRound className="mr-2 h-4 w-4" /> Rotate Key</DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem className="text-destructive focus:text-destructive focus:bg-destructive/10" onClick={() => setDeleteDataService(service)}><Eraser className="mr-2 h-4 w-4" /> Delete Data</DropdownMenuItem>
                <DropdownMenuItem className="text-destructive focus:text-destructive focus:bg-destructive/10" onClick={() => setTeardownService(service)}><Trash2 className="mr-2 h-4 w-4" /> Teardown Service</DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            {!isActive && <Button variant="default" size="sm" className="h-8 px-3 text-[11px] font-bold uppercase tracking-tight bg-primary hover:bg-primary/90" onClick={() => switchToService(service.service_id)}><Play className="h-3 w-3 mr-1.5 fill-current" /> Switch to</Button>}
          </div>
        )
      },
    },
  ]
}

interface CredentialsDialogProps { service: ServiceConfig | null; onClose: () => void }
function CredentialsDialog(props: CredentialsDialogProps) {
  return <CredentialsDialogBody key={props.service?.service_id ?? ''} {...props} />
}
function CredentialsDialogBody({ service, onClose }: CredentialsDialogProps) {
  const [credMode, setCredMode] = useState<'token' | 'manual'>('token')
  const [credApiToken, setCredApiToken] = useState('')
  const [credAccessKey, setCredAccessKey] = useState('')
  const [credSecretKey, setCredSecretKey] = useState('')
  const credentialsMutation = useMutation({
    mutationFn: async ({ service_id, payload }: { service_id: string; payload: { api_token: string } | { access_key: string; secret_key: string } }) =>
      unwrapClient(client.PATCH('/api/services/{service_id}/credentials', { params: { path: { service_id } }, body: { api_token: '', access_key: '', secret_key: '', ...payload } })),
    onSuccess: () => onClose(),
  })
  const handleClose = () => { onClose(); credentialsMutation.reset() }
  return (
    <Dialog open={!!service} onOpenChange={(open) => { if (!open) handleClose() }}>
      <DialogContent className={cn('sm:max-w-md', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle>Rotate FOS Credentials</DialogTitle>
          <DialogDescription>
            Replace the Fastly Object Storage access key for <strong>{service?.name}</strong>.
            {' Use your Fastly API token to auto-generate a new key, or enter one manually.'}
          </DialogDescription>
        </DialogHeader>
        <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1">
          {(
            <div className="flex rounded-md border overflow-hidden text-xs font-semibold">
              {(['token', 'manual'] as const).map((m) => (
                <button key={m} type="button" className={`flex-1 py-1.5 transition-colors ${credMode === m ? 'bg-primary text-primary-foreground' : 'hover:bg-muted text-muted-foreground'}`} onClick={() => { setCredMode(m); credentialsMutation.reset() }}>
                  {m === 'token' ? 'Auto (API Token)' : 'Manual'}
                </button>
              ))}
            </div>
          )}
          {credMode === 'token' && (
            <div className="space-y-1.5">
              <Label htmlFor="cred-api-token" className="text-sm">Fastly API Token</Label>
              <p className="text-xs text-muted-foreground">A new <code>read-write-objects</code> FOS key will be created for this bucket. The old key will be deleted automatically.</p>
              <Input id="cred-api-token" type="password" placeholder="Fastly API token" value={credApiToken} onChange={(e) => setCredApiToken(e.target.value)} className="font-mono text-sm" />
            </div>
          )}
          {credMode === 'manual' && (
            <>
              <div className="space-y-1.5">
                <Label htmlFor="cred-access-key" className="text-sm">Access Key ID</Label>
                <Input id="cred-access-key" placeholder="FOS access key ID" value={credAccessKey} onChange={(e) => setCredAccessKey(e.target.value)} className="font-mono text-sm" />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="cred-secret-key" className="text-sm">Secret Access Key</Label>
                <Input id="cred-secret-key" type="password" placeholder="FOS secret access key" value={credSecretKey} onChange={(e) => setCredSecretKey(e.target.value)} className="font-mono text-sm" />
              </div>
            </>
          )}
          {credentialsMutation.isError && <p className="text-sm text-destructive">{credentialsMutation.error?.message ?? 'Failed to update credentials.'}</p>}
        </div>
        <DialogFooter className={panelDialogFooter}>
          <Button variant="outline" onClick={handleClose}>Cancel</Button>
          <Button
            disabled={credentialsMutation.isPending || (credMode === 'token' ? !credApiToken : !credAccessKey || !credSecretKey)}
            onClick={() => {
              if (!service) return
              const payload = credMode === 'token' ? { api_token: credApiToken } : { access_key: credAccessKey, secret_key: credSecretKey }
              credentialsMutation.mutate({ service_id: service.service_id, payload })
            }}
          >
            {credentialsMutation.isPending ? (credMode === 'token' ? 'Creating key…' : 'Validating…') : (credMode === 'token' ? 'Rotate Key' : 'Save Credentials')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

interface NgwafDialogProps { service: ServiceConfig | null; onClose: () => void }
function NgwafDialog(props: NgwafDialogProps) {
  return <NgwafDialogBody key={props.service?.service_id ?? ''} {...props} />
}
function NgwafDialogBody({ service, onClose }: NgwafDialogProps) {
  const queryClient = useQueryClient()
  const [ngwafWorkspaceId, setNgwafWorkspaceId] = useState(service?.ngwaf_workspace_id || '')
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<{ id: string; name: string }[]>([])
  const [ngwafFetchError, setNgwafFetchError] = useState('')
  const [ngwafSaved, setNgwafSaved] = useState(false)
  const [ngwafApiToken, setNgwafApiToken] = useState('')
  const fetchWorkspacesMutation = useMutation({
    mutationFn: async ({ serviceId, token }: { serviceId: string; token: string }) => {
      const data = await unwrapClient(client.GET('/api/provision/ngwaf-workspaces', { params: { query: { service_id: serviceId } }, headers: { Authorization: `Bearer ${token}` } }))
      return (data?.workspaces || []).flatMap((w) => (w.id && w.name ? [{ id: w.id, name: w.name }] : []))
    },
    onMutate: () => { setNgwafWorkspaces([]); setNgwafFetchError('') },
    onSuccess: (workspaces) => setNgwafWorkspaces(workspaces),
    onError: (e) => setNgwafFetchError(extractApiError(e) || 'Could not load workspaces'),
  })
  const saveWorkspaceMutation = useMutation({
    mutationFn: async ({ serviceId, token, workspaceId }: { serviceId: string; token: string; workspaceId: string | null }) =>
      unwrapClient(client.PATCH('/api/provision/services/{service_id}/ngwaf-workspace', { params: { path: { service_id: serviceId } }, headers: { Authorization: `Bearer ${token}` }, body: { ngwaf_workspace_id: workspaceId } })),
    onSuccess: () => { setNgwafSaved(true); queryClient.invalidateQueries({ queryKey: queryKeys.services() }) },
    onError: (e) => setNgwafFetchError(extractApiError(e) || 'Failed to save'),
  })
  const ngwafFetching = fetchWorkspacesMutation.isPending, ngwafSaving = saveWorkspaceMutation.isPending
  return (
    <Dialog open={!!service} onOpenChange={(open) => { if (!open) onClose() }}>
      <DialogContent className={cn('sm:max-w-md', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2"><Bot className="h-5 w-5 text-primary" />NGWAF Bot Enrichment</DialogTitle>
          <DialogDescription>Set the NGWAF workspace for <strong>{service?.name}</strong>. When configured, the bot sync cron will enrich log data with specific bot names from Fastly NGWAF.</DialogDescription>
        </DialogHeader>
        <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1">
          {service && !ngwafSaved && (
            <div className="space-y-1">
              <Label htmlFor="ngwaf-api-token" className="text-xs font-semibold">Fastly API token</Label>
              <p className="text-[10px] text-muted-foreground">Required to list AND save NGWAF workspace bindings (security /).</p>
              <div className="flex gap-2">
                <Input id="ngwaf-api-token" type="password" placeholder="Fastly API token" value={ngwafApiToken} onChange={(e) => setNgwafApiToken(e.target.value)} className="h-8 font-mono text-xs flex-1" autoComplete="off" />
                <Button size="sm" variant="outline" disabled={!ngwafApiToken.trim() || ngwafFetching} onClick={() => { if (service) fetchWorkspacesMutation.mutate({ serviceId: service.service_id, token: ngwafApiToken }) }} className="h-8 text-xs">
                  {ngwafFetching ? 'Loading…' : 'Load'}
                </Button>
              </div>
            </div>
          )}
          {ngwafFetching ? <p className="text-xs text-muted-foreground animate-pulse">Loading workspaces…</p> : ngwafWorkspaces.length > 0 ? (
            <div className="space-y-1">
              <Label className="text-xs font-semibold">Select workspace</Label>
              <Select value={ngwafWorkspaceId} onValueChange={(v) => setNgwafWorkspaceId(v ?? '')}>
                <SelectTrigger className="h-8 text-xs"><SelectValue placeholder="Choose a workspace…" /></SelectTrigger>
                <SelectContent>{ngwafWorkspaces.map((w) => <SelectItem key={w.id} value={w.id} className="text-xs">{w.name}</SelectItem>)}</SelectContent>
              </Select>
            </div>
          ) : ngwafFetchError ? <p className="text-xs text-destructive">{ngwafFetchError}</p> : null}
          {ngwafSaved && <p className="text-xs text-green-600 font-medium">Workspace saved. The NGWAF sync cron will start on the next scheduler tick.</p>}
        </div>
        <DialogFooter className={panelDialogFooter}>
          {ngwafSaved ? <Button size="sm" onClick={onClose}>Close</Button> : (
            <>
              <Button variant="outline" size="sm" onClick={onClose}>Cancel</Button>
              <Button size="sm" disabled={ngwafSaving || !ngwafApiToken.trim()} title={!ngwafApiToken.trim() ? 'Enter your Fastly API token to save' : undefined} onClick={() => {
                if (!service) return
                saveWorkspaceMutation.mutate({ serviceId: service.service_id, token: ngwafApiToken, workspaceId: ngwafWorkspaceId.trim() || null })
              }}>{ngwafSaving ? 'Saving…' : 'Save'}</Button>
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
  const storeServiceId = useServiceStore((s) => s.activeServiceId)
  const activeServiceId = mounted ? storeServiceId : null
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId)
  const router = useRouter()
  const [cronService, setCronService] = useState<ServiceConfig | null>(null)
  const [settingsService, setSettingsService] = useState<ServiceConfig | null>(null)
  const [teardownService, setTeardownService] = useState<ServiceConfig | null>(null)
  const [deleteDataService, setDeleteDataService] = useState<ServiceConfig | null>(null)
  const [credentialsService, setCredentialsService] = useState<ServiceConfig | null>(null)
  const [wizardOpen, setWizardOpen] = useState(false)
  const [ngwafService, setNgwafService] = useState<ServiceConfig | null>(null)

  const openCredentials = React.useCallback((service: ServiceConfig) => setCredentialsService(service), [])
  const switchToService = React.useCallback((id: string) => {
    setActiveServiceId(id); queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() }); router.push(`/dashboard?service=${id}`)
  }, [setActiveServiceId, queryClient, router])
  const openRum = React.useCallback((s: ServiceConfig) => {
    setActiveServiceId(s.service_id); router.push(`/admin/rum?service=${s.service_id}`)
  }, [setActiveServiceId, router])

  const { data: services, isLoading, isFetching, isError, error } = useQuery({
    queryKey: queryKeys.services(),
    queryFn: async ({ signal }) => unwrapClient(client.GET('/api/services', { signal })),
  })
  const isRefreshing = isFetching && !isLoading
  const handleRefreshAll = React.useCallback(() => {
    for (const queryKey of [queryKeys.services(), queryKeys.admin.overviewAll(), queryKeys.admin.healthSnapshot(), queryKeys.syncStatus(activeServiceId)]) {
      void queryClient.invalidateQueries({ queryKey })
    }
  }, [queryClient, activeServiceId])

  const columns = React.useMemo(() => buildServiceColumns({
    activeServiceId, switchToService, servicesLength: services?.services?.length || 0,
    setCronService, setSettingsService, setDeleteDataService, setTeardownService, openNgwaf: setNgwafService, openRum, openCredentials,
  }), [activeServiceId, switchToService, services?.services?.length, openRum, openCredentials])

  return (
    <>
      <div className="space-y-4" data-testid="admin-services-section">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <h2 className="text-xl font-semibold tracking-tight">Service Management</h2>
            <Button size="sm" onClick={() => setWizardOpen(true)}><Plus className="h-4 w-4 mr-1" /> Add Service</Button>
          </div>
          <Button size="sm" variant="outline" data-testid="admin-overview-refresh-btn" onClick={handleRefreshAll} disabled={isFetching}>Refresh Status</Button>
        </div>
        {isLoading && <div className="text-xs text-muted-foreground" role="status" aria-live="polite" data-testid="services-loading-status">Loading service configs...</div>}
        {isError && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Couldn&apos;t load services</AlertTitle>
            <AlertDescription className="text-xs">{error instanceof Error ? error.message : 'The backend returned an error. The list below may be incomplete — investigate before provisioning new services.'}</AlertDescription>
          </Alert>
        )}
        <div data-testid="services-table-container" className={`border rounded-lg bg-card shadow-sm overflow-hidden min-h-[244px] transition-opacity ${isRefreshing ? 'opacity-40 pointer-events-none' : ''}`}>
          <DataTable columns={columns} data={services?.services || []} isLoading={isLoading} searchKey="name" emptyMessage="No services yet" emptyHint="Add a service to get started." />
        </div>
      </div>
      <ProvisionWizard open={wizardOpen} onOpenChange={setWizardOpen} />
      {cronService && <CronSettingsModal service={cronService} open={!!cronService} onOpenChange={(open) => !open && setCronService(null)} />}
      {settingsService && <LogSettingsModal service={settingsService} open={!!settingsService} onOpenChange={(open) => !open && setSettingsService(null)} />}
      <TeardownDialog service={teardownService} open={!!teardownService} onOpenChange={(open) => !open && setTeardownService(null)} onComplete={() => {
        queryClient.invalidateQueries({ queryKey: queryKeys.services() }); queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() }); setTeardownService(null)
      }} />
      <DeleteDataDialog service={deleteDataService} open={!!deleteDataService} onOpenChange={(open) => !open && setDeleteDataService(null)} onComplete={() => {
        cronCacheBust(queryClient, deleteDataService?.service_id); queryClient.invalidateQueries({ queryKey: queryKeys.services() }); setDeleteDataService(null)
      }} />
      <CredentialsDialog service={credentialsService} onClose={() => setCredentialsService(null)} />
      <NgwafDialog service={ngwafService} onClose={() => setNgwafService(null)} />
    </>
  )
}
