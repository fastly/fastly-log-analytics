'use client'

import * as React from 'react'
import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { QRCodeCanvas } from 'qrcode.react'
import { z } from 'zod'
import {
  AlertTriangle, Check, Copy, Eye, EyeOff, FileJson, History,
  KeyRound, Loader2, Pencil, Plus, QrCode, Trash2, UserPlus,
} from 'lucide-react'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { CopyButton } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { formatDeterministicUTC } from '@/lib/format'
import { cn, panelDialogContent, panelDialogFooter, panelDialogHeaderSolid } from '@/lib/utils'
import { useAutoFlash, useCopyToClipboard, useNowMs, useOpenGeneration } from '@/hooks/useSSE'
import type { ServiceConfig } from '@/types/api'
import type { components } from '@/types/api.generated'
import { CreateInviteDialog } from './CreateInviteDialog'
import { SortableHead, useShareMutation, useTableSort, type Invite, type ShareStatus, type SortAccessors } from './SharingControlPanel'

// ── InviteAnalystDialog (Path A standalone viewer key generator) ──────────

type AnalystInvite = components['schemas']['AnalystInvite']

export interface InviteAnalystDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

type InviteStep = 'confirm' | 'creating' | 'result'

const REVEAL_TTL_MS = 5 * 60 * 1000
export const recentInvites = new Map<string, { invite: AnalystInvite; issuedAt: number }>()

export function getRecentInvite(serviceId: string): AnalystInvite | null {
  const entry = recentInvites.get(serviceId)
  if (!entry) return null
  if (Date.now() - entry.issuedAt > REVEAL_TTL_MS) {
    recentInvites.delete(serviceId)
    return null
  }
  return entry.invite
}

export function CopyField({ label, value, secret, multiline = false }: { label: string; value: string; secret?: boolean; multiline?: boolean }) {
  const [revealed, setRevealed] = useState(false)
  const display = secret && !revealed ? '•'.repeat(Math.min(value.length, 24)) : value

  return (
    <div className="space-y-1.5">
      <Label className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">{label}</Label>
      <div className="flex items-start gap-2">
        <div className={`flex-1 font-mono text-sm bg-muted/50 border rounded-md px-3 py-2 min-w-0 ${multiline ? 'break-all overflow-hidden max-h-32 overflow-y-auto' : 'flex items-center gap-2 truncate'}`}>
          <span className="flex-1">{display}</span>
        </div>
        <div className="flex items-center gap-1">
          {secret && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={revealed ? 'Hide value' : 'Reveal value'}
              className="h-9 w-9 shrink-0 text-muted-foreground hover:text-foreground"
              onClick={() => setRevealed((r) => !r)}
              title={revealed ? 'Hide' : 'Reveal'}
            >
              {revealed ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
            </Button>
          )}
          <CopyButton
            text={value}
            ariaLabel="Copy to clipboard"
            title="Copy"
            className="h-9 w-9 shrink-0 text-muted-foreground hover:text-foreground"
            iconClassName="h-4 w-4"
          />
        </div>
      </div>
    </div>
  )
}

export function CachedInviteBanner({ serviceId, onReveal }: { serviceId: string; onReveal: () => void }) {
  const nowMs = useNowMs()
  const entry = recentInvites.get(serviceId)
  if (!entry || !getRecentInvite(serviceId)) return null
  const ageMs = nowMs ? nowMs - entry.issuedAt : 0
  const remainingMin = Math.max(1, Math.ceil(Math.max(0, REVEAL_TTL_MS - ageMs) / 60000))
  return (
    <Alert className="bg-primary/5 border-primary/20">
      <History className="h-4 w-4" />
      <AlertDescription className="text-sm ml-1 flex items-center justify-between gap-3">
        <span>A key was issued for this service in the last {remainingMin} min — still available in this tab.</span>
        <Button variant="outline" size="sm" onClick={onReveal} className="h-8 shrink-0">
          <Eye className="h-3.5 w-3.5 mr-1.5" />
          Reveal
        </Button>
      </AlertDescription>
    </Alert>
  )
}

export function InviteAnalystDialog(props: InviteAnalystDialogProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <InviteAnalystDialogBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function InviteAnalystDialogBody({ service, open, onOpenChange }: InviteAnalystDialogProps) {
  const [result, setResult] = useState<AnalystInvite | null>(null)
  const { copied: jsonCopied, copy: copyJson } = useCopyToClipboard(2000)

  const createInviteMutation = useMutation({
    mutationFn: async (serviceId: string) => {
      const data = await unwrapClient(client.POST('/api/services/{service_id}/generate-viewer-key', {
        params: { path: { service_id: serviceId } },
      }))
      return data as AnalystInvite
    },
    onSuccess: (invite, serviceId) => {
      recentInvites.set(serviceId, { invite, issuedAt: Date.now() })
      setResult(invite)
    },
  })

  if (!service) return null

  const step: InviteStep = result ? 'result' : createInviteMutation.isPending ? 'creating' : 'confirm'
  const error = createInviteMutation.error
    ? (createInviteMutation.error instanceof Error && createInviteMutation.error.message
        ? createInviteMutation.error.message
        : 'Failed to create analyst key')
    : ''

  const handleCopyJson = () => {
    if (!result) return
    const config: Record<string, string> = {
      name: result.name,
      service_id: result.service_id,
      fos_bucket: result.fos_bucket,
      fos_region: result.fos_region,
      fos_endpoint: result.fos_endpoint,
      fos_prefix: result.fos_prefix,
      access_key_id: result.access_key_id,
      secret_key: result.secret_key,
    }
    if (result.cdn_url) config.cdn_url = result.cdn_url
    if (result.cdn_service_id) config.cdn_service_id = result.cdn_service_id
    if (result.cdn_secret) config.cdn_secret = result.cdn_secret
    if (result.iceberg_metadata_location) config.iceberg_metadata_location = result.iceberg_metadata_location
    void copyJson(JSON.stringify(config, null, 2))
  }

  return (
    <Dialog open={open} onOpenChange={(isOpen) => { if (step !== 'creating') onOpenChange(isOpen) }}>
      <DialogContent className={cn('sm:max-w-xl', panelDialogContent)} showCloseButton={step !== 'creating'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <div className="flex items-center justify-between mb-1">
            <DialogTitle className="flex items-center gap-2 text-xl font-bold">
              <UserPlus className="h-5 w-5" />
              Invite Analyst
            </DialogTitle>
            <div className="flex items-center gap-1.5 mr-6">
              <div className={`h-1.5 w-6 rounded-full transition-colors ${step === 'confirm' ? 'bg-primary' : 'bg-muted'}`} />
              <div className={`h-1.5 w-6 rounded-full transition-colors ${step === 'result' ? 'bg-primary' : 'bg-muted'}`} />
            </div>
          </div>
          <Badge variant="secondary" className="w-fit font-mono text-[10px] font-normal tracking-tight uppercase">
            {service.name}
          </Badge>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {step === 'confirm' && (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-right-4 duration-300">
              <CachedInviteBanner serviceId={service.service_id} onReveal={() => {
                const cached = getRecentInvite(service.service_id)
                if (cached) setResult(cached)
              }} />
              <div className="rounded-lg border bg-muted/30 p-4 space-y-3">
                <div className="flex items-center gap-2 text-sm font-semibold">
                  <KeyRound className="h-4 w-4 text-primary" />
                  What will happen
                </div>
                <ul className="text-sm text-muted-foreground space-y-1.5 ml-6 list-disc">
                  <li>A new <strong className="text-foreground">read-only</strong> Fastly Object Storage access key will be created, scoped to this service&apos;s bucket.</li>
                  <li>Your stored Fastly API token will be used — no re-entry needed.</li>
                  <li>You&apos;ll receive a JSON config to send to the analyst.</li>
                </ul>
              </div>
              {error && (
                <Alert variant="destructive" className="bg-destructive/5 border-destructive/20">
                  <AlertTriangle className="h-4 w-4" />
                  <AlertDescription className="text-sm ml-1">{error}</AlertDescription>
                </Alert>
              )}
            </div>
          )}

          {step === 'creating' && (
            <div className="p-8 flex flex-col items-center justify-center gap-4 text-center min-h-[300px]">
              <Loader2 className="h-10 w-10 animate-spin text-primary/50" />
              <div className="space-y-1">
                <p className="font-bold text-lg tracking-tight">Generating Analyst Access…</p>
                <p className="text-sm text-muted-foreground">Provisioning read-only keys via Fastly API.</p>
              </div>
            </div>
          )}

          {step === 'result' && result && (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
              <Alert className="bg-amber-500/10 border-amber-500/30 text-amber-700 dark:text-amber-400">
                <AlertTriangle className="h-4 w-4" />
                <AlertDescription className="text-sm ml-1 font-medium">
                  Save the secret key now — available in this browser tab for {REVEAL_TTL_MS / 60000} min, then unrecoverable.
                </AlertDescription>
              </Alert>
              <div className="space-y-4">
                <CopyField label="Display Name" value={result.name} />
                <CopyField label="Fastly Service ID" value={result.service_id} />
                <CopyField label="FOS Bucket" value={result.fos_bucket} />
                <CopyField label="FOS Region" value={result.fos_region} />
                <CopyField label="Access Key ID" value={result.access_key_id} />
                <CopyField label="Secret Key" value={result.secret_key} secret />
                {result.cdn_url && <CopyField label="CDN URL" value={result.cdn_url} />}
                {result.cdn_secret && <CopyField label="CDN Secret" value={result.cdn_secret} secret />}
              </div>
            </div>
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
          {step === 'confirm' && (
            <Button onClick={() => createInviteMutation.mutate(service.service_id)} className="h-10 px-8 font-bold">
              <UserPlus className="h-4 w-4 mr-2" />
              Generate Invite
            </Button>
          )}
          {step === 'result' && (
            <>
              <Button variant="outline" onClick={handleCopyJson} className="h-10 px-6 gap-2">
                {jsonCopied ? <Check className="h-4 w-4 text-emerald-500" /> : <FileJson className="h-4 w-4" />}
                {jsonCopied ? 'Copied!' : 'Copy JSON'}
              </Button>
              <Button onClick={() => onOpenChange(false)} className="h-10 px-8">Done</Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ── InvitationsPanel (Path B live share invitations) ──────────────────────

interface InvitationsPanelProps {
  status: ShareStatus | null
  onRefresh: () => Promise<void> | void
  onError: (msg: string) => void
  onViewAuditLogs?: (email: string) => void
}

const PasscodeDraftSchema = z.string().min(1)

function buildShareCard(
  invite: Invite,
  publicUrl: string | null,
  allServices: Array<{ service_id: string; name?: string }> = []
): string {
  const url = publicUrl || '(sharing not active)'
  const nameById = new Map(allServices.map((s) => [s.service_id, s.name || '']))
  const formatted = (invite.service_ids || []).map((id: string) => {
    const name = nameById.get(id)
    return name ? `${name} (${id})` : id
  })
  const services = formatted.length ? formatted.join(', ') : 'all assigned services'
  const credLines =
    invite.auth_method === 'oauth'
      ? `Sign-in:      ${invite.oauth_provider ? `${invite.oauth_provider} (SSO)` : 'SSO'}\n              Sign in at the link with the account for ${invite.email}.`
      : `Passcode:     (delivered separately — never paste here)`
  return `==================================================
FASTLY LOG ANALYSIS - SHARE DASHBOARD INVITATION
==================================================
You have been invited to view the analyst dashboard.

Access Link:  ${url}/share-login
Login Email:  ${invite.email}
${credLines}
Authorized Services: ${services}
Valid Until:  ${invite.expires_at ? new Date(invite.expires_at).toUTCString() : 'Unlimited'}

Note: You will be required to review and agree to the Terms of Access upon signing in before accessing the dashboard.
==================================================`
}

export function InvitationsPanel({ status, onRefresh, onError, onViewAuditLogs }: InvitationsPanelProps) {
  const [createOpen, setCreateOpen] = React.useState(false)
  const { busy, run } = useShareMutation(onError, onRefresh)
  const [lastCopiedInviteId, setLastCopiedInviteId] = React.useState<string | null>(null)
  const { copied, copy } = useCopyToClipboard(1500)
  const [qrInviteId, setQrInviteId] = React.useState<string | null>(null)
  const [editingServicesFor, setEditingServicesFor] = React.useState<string | null>(null)
  const [editingServicesDraft, setEditingServicesDraft] = React.useState<string[]>([])
  const [editingMaskIps, setEditingMaskIps] = React.useState(false)
  const [editingAllowConcurrent, setEditingAllowConcurrent] = React.useState(false)
  const [savingServices, setSavingServices] = React.useState(false)
  const [editingPasscodeFor, setEditingPasscodeFor] = React.useState<string | null>(null)
  const [passcodeDraft, setPasscodeDraft] = React.useState('')
  const [passcodeVisible, setPasscodeVisible] = React.useState(false)
  const [savingPasscode, setSavingPasscode] = React.useState(false)
  const [passcodeError, setPasscodeError] = React.useState<string | null>(null)
  const { value: passcodeSavedFor, flash: flashPasscodeSaved } = useAutoFlash<string>(2000)

  const services = status?.services || []
  const invites = status?.invites || []

  const onlineInviteIds = React.useMemo(
    () => new Set((status?.sessions || []).map((s) => s.invite_id)),
    [status?.sessions],
  )
  const svcNameById = React.useMemo(
    () => new Map(services.map((s) => [s.service_id, s.name])),
    [services],
  )

  const accessors = React.useMemo<SortAccessors<Invite>>(
    () => ({
      name: (i) => (i.name || '').toLowerCase(),
      email: (i) => (i.email || '').toLowerCase(),
      last_login_at: (i) => i.last_login_at ?? null,
      expires_at: (i) => i.expires_at ?? null,
    }),
    [],
  )
  const { sorted, sortKey, sortDir, toggle } = useTableSort(invites, accessors, {
    defaultKey: 'last_login_at',
    defaultDir: 'desc',
  })

  const handleRevokeInvite = (id: string) => {
    if (!confirm('Delete this invite and boot any sessions linked to it? This cannot be undone.')) return
    run(() => client.DELETE('/api/admin/share/invites/{invite_id}', { params: { path: { invite_id: id } } }))
  }

  const openEditServices = (invite: Invite) => {
    setEditingServicesFor(invite.id)
    setEditingServicesDraft(invite.service_ids || [])
    setEditingMaskIps(!!invite.pii_policy?.mask_ips)
    setEditingAllowConcurrent(!!invite.allow_concurrent_sessions)
  }

  const handleSaveServices = async (inviteId: string) => {
    setSavingServices(true)
    onError('')
    try {
      await unwrapClient(client.PATCH('/api/admin/share/invites/{invite_id}/services', {
        params: { path: { invite_id: inviteId } },
        body: { service_ids: editingServicesDraft },
      }))
      await unwrapClient(client.PATCH('/api/admin/share/invites/{invite_id}/pii', {
        params: { path: { invite_id: inviteId } },
        body: { mask_ips: editingMaskIps },
      }))
      await unwrapClient(client.PATCH('/api/admin/share/invites/{invite_id}/sharing', {
        params: { path: { invite_id: inviteId } },
        body: { allow_concurrent_sessions: editingAllowConcurrent },
      }))
      setEditingServicesFor(null)
      await onRefresh()
    } catch (e) {
      onError(extractApiError(e))
    } finally {
      setSavingServices(false)
    }
  }

  const openEditPasscode = (invite: Invite) => {
    setEditingPasscodeFor(invite.id)
    setPasscodeDraft('')
    setPasscodeVisible(false)
    setPasscodeError(null)
  }

  const handleSavePasscode = async (inviteId: string) => {
    const parsed = PasscodeDraftSchema.safeParse(passcodeDraft)
    if (!parsed.success) return
    setSavingPasscode(true)
    setPasscodeError(null)
    try {
      await unwrapClient(client.PATCH('/api/admin/share/invites/{invite_id}/passcode', {
        params: { path: { invite_id: inviteId } },
        body: { passcode: parsed.data },
      }))
      setEditingPasscodeFor(null)
      setPasscodeDraft('')
      flashPasscodeSaved(inviteId)
      await onRefresh()
    } catch (e) {
      setPasscodeError(extractApiError(e))
    } finally {
      setSavingPasscode(false)
    }
  }

  const handleCopyShareCard = (invite: Invite) => {
    setLastCopiedInviteId(invite.id)
    void copy(buildShareCard(invite, status?.public_url ?? null, services))
  }

  return (
    <section className="rounded-lg border bg-card p-4 space-y-3" data-testid="admin-share-invites-panel">
      <div className="flex items-center justify-between gap-2">
        <h4 className="text-sm font-semibold">Active invitations</h4>
        <Button size="sm" onClick={() => setCreateOpen(true)} data-testid="admin-share-new-invite-btn">
          <Plus className="h-4 w-4 mr-1" />
          New invitation
        </Button>
      </div>

      <Table>
        <TableHeader>
          <TableRow>
            <SortableHead label="Name" sortKey="name" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="Email" sortKey="email" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="Last login" sortKey="last_login_at" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <TableHead>Services</TableHead>
            <SortableHead label="Expires" sortKey="expires_at" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <TableHead className="text-right">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {sorted.map((invite: Invite) => {
            const online = onlineInviteIds.has(invite.id)
            return (
              <TableRow key={invite.id}>
                <TableCell className="font-medium">
                  <div className="flex items-center gap-1.5">
                    {online && (
                      <span
                        className="h-2 w-2 rounded-full bg-emerald-500 shrink-0 animate-pulse"
                        role="img"
                        title="Online now (has an active session)"
                        aria-label="Online now"
                      />
                    )}
                    <span>{invite.name}</span>
                    <Badge
                      variant="outline"
                      className="text-[10px] font-normal"
                      title={invite.auth_method === 'oauth' ? 'Signs in via SSO / OIDC' : 'Signs in with a passcode'}
                    >
                      {invite.auth_method === 'oauth'
                        ? `SSO${invite.oauth_provider ? ` · ${invite.oauth_provider}` : ''}`
                        : 'Passcode'}
                    </Badge>
                    {invite.allow_concurrent_sessions && (
                      <Badge variant="outline" className="text-[10px] font-normal" title="Multiple people can use this link at the same time">
                        Shared
                      </Badge>
                    )}
                  </div>
                  {onViewAuditLogs && (
                    <button
                      type="button"
                      onClick={() => onViewAuditLogs(invite.email)}
                      aria-label={`View audit logs for ${invite.email}`}
                      className="text-[10px] text-primary hover:underline block mt-0.5 text-left font-normal"
                    >
                      view audit logs
                    </button>
                  )}
                </TableCell>
                <TableCell className="text-xs">{invite.email}</TableCell>
                <TableCell className="text-xs whitespace-nowrap">
                  {invite.last_login_at ? formatDeterministicUTC(invite.last_login_at) : <span className="text-muted-foreground">Never</span>}
                </TableCell>
                <TableCell className="text-xs">
                  <div className="flex flex-col gap-1 items-start">
                    {(invite.service_ids || []).map((s: string) => {
                      const nm = svcNameById.get(s)
                      return (
                        <Badge key={s} variant="secondary" className="text-[10px]" title={nm ? `${nm} (${s})` : s}>
                          {nm ? <>{nm} <span className="opacity-60">({s})</span></> : s}
                        </Badge>
                      )
                    })}
                  </div>
                </TableCell>
                <TableCell className="text-xs">{formatDeterministicUTC(invite.expires_at)}</TableCell>
                <TableCell className="text-right space-x-1">
                  <Button size="sm" variant="ghost" onClick={() => handleCopyShareCard(invite)} title="Copy share card">
                    {copied && lastCopiedInviteId === invite.id ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
                  </Button>
                  <Popover open={qrInviteId === invite.id} onOpenChange={(o) => setQrInviteId(o ? invite.id : null)}>
                    <PopoverTrigger
                      render={(props: React.ComponentPropsWithRef<'button'>) => (
                        <Button {...props} size="sm" variant="ghost" title="QR code" disabled={!status?.public_url}>
                          <QrCode className="h-3 w-3" />
                        </Button>
                      )}
                    />
                    <PopoverContent className="w-56 items-center text-center">
                      <div className="text-xs font-semibold">{invite.name || invite.email}</div>
                      {status?.public_url ? (
                        <>
                          <QRCodeCanvas value={`${status.public_url}/share-login`} size={180} className="mx-auto" aria-label={`QR code for ${invite.email}`} />
                          <div className="text-[10px] text-muted-foreground break-all font-mono">{status.public_url}/share-login</div>
                        </>
                      ) : (
                        <div className="text-xs text-muted-foreground">Start sharing to generate a QR.</div>
                      )}
                    </PopoverContent>
                  </Popover>
                  <Popover open={editingServicesFor === invite.id} onOpenChange={(o) => (o ? openEditServices(invite) : setEditingServicesFor(null))}>
                    <PopoverTrigger
                      render={(props: React.ComponentPropsWithRef<'button'>) => (
                        <Button {...props} size="sm" variant="ghost" title="Edit invite (services, privacy, shared logins)">
                          <Pencil className="h-3 w-3" />
                        </Button>
                      )}
                    />
                    <PopoverContent className="w-72">
                      <div className="text-xs font-semibold mb-1">Authorized services</div>
                      <div className="space-y-1 max-h-48 overflow-y-auto">
                        {services.map((svc) => {
                          const checked = editingServicesDraft.includes(svc.service_id)
                          return (
                            <label key={svc.service_id} className="flex items-center gap-2 text-sm cursor-pointer">
                              <Checkbox
                                checked={checked}
                                onCheckedChange={(v) =>
                                  setEditingServicesDraft((prev) =>
                                    v ? [...prev, svc.service_id] : prev.filter((id) => id !== svc.service_id),
                                  )
                                }
                              />
                              <span className="truncate">{svc.name}</span>
                            </label>
                          )
                        })}
                        {!services.length && <p className="text-xs text-muted-foreground">No services available.</p>}
                      </div>
                      <label className="flex items-center gap-2 text-sm cursor-pointer pt-2 mt-2 border-t">
                        <Checkbox checked={editingMaskIps} onCheckedChange={(v) => setEditingMaskIps(!!v)} />
                        <span>Anonymize client IPs (mask PII)</span>
                      </label>
                      <label className="flex items-center gap-2 text-sm cursor-pointer pt-2 mt-2 border-t">
                        <Checkbox checked={editingAllowConcurrent} onCheckedChange={(v) => setEditingAllowConcurrent(!!v)} />
                        <span>Allow shared logins (multiple people at once)</span>
                      </label>
                      <div className="flex justify-end gap-1 pt-2 border-t">
                        <Button size="sm" variant="ghost" onClick={() => setEditingServicesFor(null)} disabled={savingServices}>
                          Cancel
                        </Button>
                        <Button size="sm" onClick={() => handleSaveServices(invite.id)} disabled={savingServices}>
                          {savingServices && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
                          Save
                        </Button>
                      </div>
                    </PopoverContent>
                  </Popover>
                  {invite.auth_method !== 'oauth' && (
                    <Popover open={editingPasscodeFor === invite.id} onOpenChange={(o) => (o ? openEditPasscode(invite) : setEditingPasscodeFor(null))}>
                      <PopoverTrigger
                        render={(props: React.ComponentPropsWithRef<'button'>) => (
                          <Button {...props} size="sm" variant="ghost" title="Update passcode (re-send share card afterwards)">
                            {passcodeSavedFor === invite.id ? <Check className="h-3 w-3 text-emerald-500" /> : <KeyRound className="h-3 w-3" />}
                          </Button>
                        )}
                      />
                      <PopoverContent className="w-72">
                        <div className="text-xs font-semibold mb-1">New passcode</div>
                        <div className="text-[10px] text-muted-foreground mb-2">
                          Sessions using the old passcode keep working until they expire — but new logins will need the new one.
                        </div>
                        <div className="flex items-center gap-1">
                          <input
                            type={passcodeVisible ? 'text' : 'password'}
                            className="flex h-8 w-full rounded border border-input bg-background px-2 py-1 text-xs"
                            value={passcodeDraft}
                            onChange={(e) => {
                              setPasscodeDraft(e.target.value)
                              setPasscodeError(null)
                            }}
                            placeholder="e.g. ocean-breeze-cabin-42"
                            autoFocus
                          />
                          <Button size="sm" variant="ghost" type="button" onClick={() => setPasscodeVisible((v) => !v)} title={passcodeVisible ? 'Hide' : 'Show'}>
                            {passcodeVisible ? <EyeOff className="h-3 w-3" /> : <Eye className="h-3 w-3" />}
                          </Button>
                        </div>
                        {passcodeError && <div className="text-[11px] text-destructive pt-1">{passcodeError}</div>}
                        <div className="flex justify-end gap-1 pt-2 border-t mt-2">
                          <Button size="sm" variant="ghost" onClick={() => setEditingPasscodeFor(null)} disabled={savingPasscode}>
                            Cancel
                          </Button>
                          <Button size="sm" onClick={() => handleSavePasscode(invite.id)} disabled={savingPasscode || !passcodeDraft}>
                            {savingPasscode && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
                            Save
                          </Button>
                        </div>
                      </PopoverContent>
                    </Popover>
                  )}
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => handleRevokeInvite(invite.id)}
                    disabled={busy}
                    title="Delete invite (boots any active sessions)"
                  >
                    <Trash2 className="h-3 w-3 text-destructive" />
                  </Button>
                </TableCell>
              </TableRow>
            )
          })}
          {!invites.length && (
            <TableRow>
              <TableCell colSpan={6} className="text-center text-xs text-muted-foreground">
                No invitations yet.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      <CreateInviteDialog open={createOpen} onOpenChange={setCreateOpen} services={services} onCreated={onRefresh} />
    </section>
  )
}
