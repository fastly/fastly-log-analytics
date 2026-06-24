'use client'

import * as React from 'react'
import { QRCodeCanvas } from 'qrcode.react'
import { Check, Copy, Eye, EyeOff, KeyRound, Loader2, Pencil, Plus, QrCode, Trash2 } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { client, extractApiError } from '@/lib/api'

import { CreateInviteDialog } from './CreateInviteDialog'
import { useShareMutation } from './useShareMutation'
import { formatStamp, type ShareStatus } from './utils'

interface InvitationsPanelProps {
  status: ShareStatus | null
  onRefresh: () => Promise<void> | void
  onError: (msg: string) => void
  onViewAuditLogs?: (email: string) => void
}

function buildShareCard(
  invite: any,
  publicUrl: string | null,
  allServices: Array<{ service_id: string; name?: string }> = []
): string {
  const url = publicUrl || '(sharing not active)'
  // Render each authorized service as "Name (ID)" — falls back to bare
  // ID when the lookup misses (newly-added service the UI hasn't seen
  // yet, e.g.) so the invite is still useful.
  const nameById = new Map(allServices.map(s => [s.service_id, s.name || '']))
  const formatted = (invite.service_ids || []).map((id: string) => {
    const name = nameById.get(id)
    return name ? `${name} (${id})` : id
  })
  const services = formatted.length ? formatted.join(', ') : 'all assigned services'
  return `==================================================
FASTLY LOG ANALYSIS - SHARE DASHBOARD INVITATION
==================================================
You have been invited to view the analyst dashboard.

Access Link:  ${url}/share-login
Login Email:  ${invite.email}
Passcode:     (delivered separately — never paste here)
Authorized Services: ${services}
Valid Until:  ${invite.expires_at ? new Date(invite.expires_at).toUTCString() : 'Unlimited'}
==================================================`
}

export function InvitationsPanel({ status, onRefresh, onError, onViewAuditLogs }: InvitationsPanelProps) {
  const [createOpen, setCreateOpen] = React.useState(false)
  const { busy, run } = useShareMutation(onError, onRefresh)
  const [copiedInviteId, setCopiedInviteId] = React.useState<string | null>(null)
  const [qrInviteId, setQrInviteId] = React.useState<string | null>(null)
  const [editingServicesFor, setEditingServicesFor] = React.useState<string | null>(null)
  const [editingServicesDraft, setEditingServicesDraft] = React.useState<string[]>([])
  const [savingServices, setSavingServices] = React.useState(false)
  const [editingPasscodeFor, setEditingPasscodeFor] = React.useState<string | null>(null)
  const [passcodeDraft, setPasscodeDraft] = React.useState('')
  const [passcodeVisible, setPasscodeVisible] = React.useState(false)
  const [savingPasscode, setSavingPasscode] = React.useState(false)
  const [passcodeError, setPasscodeError] = React.useState<string | null>(null)
  const [passcodeSavedFor, setPasscodeSavedFor] = React.useState<string | null>(null)

  const services = status?.services || []
  const invites = status?.invites || []

  const handleRevokeInvite = (id: string) => {
    if (!confirm('Delete this invite and boot any sessions linked to it? This cannot be undone.')) return
    // DELETE is a strict superset of revoke (also boots active sessions)
    // and removes the row entirely so it disappears from the list. The
    // soft-revoke endpoint still exists for callers that want to keep
    // the row for audit; the trash-icon UX expects "gone".
    run(() =>
      client.DELETE('/api/admin/share/invites/{invite_id}' as any, {
        params: { path: { invite_id: id } },
      } as any),
    )
  }

  const openEditServices = (invite: any) => {
    setEditingServicesFor(invite.id)
    setEditingServicesDraft(invite.service_ids || [])
  }

  const handleSaveServices = async (inviteId: string) => {
    setSavingServices(true)
    onError('')
    try {
      await client.PATCH('/api/admin/share/invites/{invite_id}/services' as any, {
        params: { path: { invite_id: inviteId } },
        body: { service_ids: editingServicesDraft },
      } as any)
      setEditingServicesFor(null)
      await onRefresh()
    } catch (e: any) {
      onError(extractApiError(e))
    } finally {
      setSavingServices(false)
    }
  }

  const openEditPasscode = (invite: any) => {
    setEditingPasscodeFor(invite.id)
    setPasscodeDraft('')
    setPasscodeVisible(false)
    setPasscodeError(null)
  }

  const handleSavePasscode = async (inviteId: string) => {
    setSavingPasscode(true)
    setPasscodeError(null)
    try {
      await client.PATCH('/api/admin/share/invites/{invite_id}/passcode' as any, {
        params: { path: { invite_id: inviteId } },
        body: { passcode: passcodeDraft },
      } as any)
      setEditingPasscodeFor(null)
      setPasscodeDraft('')
      setPasscodeSavedFor(inviteId)
      setTimeout(() => setPasscodeSavedFor((id) => (id === inviteId ? null : id)), 2000)
      await onRefresh()
    } catch (e: any) {
      // Show validation errors inline (weak passcode etc.); don't push to the
      // top-level toast since the popover is the natural place to fix it.
      setPasscodeError(extractApiError(e))
    } finally {
      setSavingPasscode(false)
    }
  }

  const handleCopyShareCard = (invite: any) => {
    navigator.clipboard.writeText(
      buildShareCard(invite, status?.public_url ?? null, services)
    )
    setCopiedInviteId(invite.id)
    setTimeout(() => setCopiedInviteId(null), 1500)
  }

  return (
    <section className="rounded-lg border bg-card p-4 space-y-3">
      <div className="flex items-center justify-between gap-2">
        <h4 className="text-sm font-semibold">Active invitations</h4>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <Plus className="h-4 w-4 mr-1" />
          New invitation
        </Button>
      </div>

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Name</TableHead>
            <TableHead>Email</TableHead>
            <TableHead>Services</TableHead>
            <TableHead>Expires</TableHead>
            <TableHead className="text-right">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {invites.map((invite: any) => (
            <TableRow key={invite.id}>
              <TableCell className="font-medium">
                <div>{invite.name}</div>
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
              <TableCell className="text-xs">
                {(invite.service_ids || []).map((s: string) => (
                  <Badge key={s} variant="secondary" className="mr-1 text-[10px]">
                    {s}
                  </Badge>
                ))}
              </TableCell>
              <TableCell className="text-xs">{formatStamp(invite.expires_at)}</TableCell>
              <TableCell className="text-right space-x-1">
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => handleCopyShareCard(invite)}
                  title="Copy share card"
                >
                  {copiedInviteId === invite.id ? (
                    <Check className="h-3 w-3 text-emerald-500" />
                  ) : (
                    <Copy className="h-3 w-3" />
                  )}
                </Button>
                <Popover
                  open={qrInviteId === invite.id}
                  onOpenChange={(o) => setQrInviteId(o ? invite.id : null)}
                >
                  <PopoverTrigger
                    render={(props: React.ComponentPropsWithRef<'button'>) => (
                      <Button
                        {...props}
                        size="sm"
                        variant="ghost"
                        title="QR code"
                        disabled={!status?.public_url}
                      >
                        <QrCode className="h-3 w-3" />
                      </Button>
                    )}
                  />
                  <PopoverContent className="w-56 items-center text-center">
                    <div className="text-xs font-semibold">
                      {invite.name || invite.email}
                    </div>
                    {status?.public_url ? (
                      <>
                        <QRCodeCanvas
                          value={`${status.public_url}/share-login`}
                          size={180}
                          className="mx-auto"
                          aria-label={`QR code for ${invite.email}`}
                        />
                        <div className="text-[10px] text-muted-foreground break-all font-mono">
                          {status.public_url}/share-login
                        </div>
                      </>
                    ) : (
                      <div className="text-xs text-muted-foreground">
                        Start sharing to generate a QR.
                      </div>
                    )}
                  </PopoverContent>
                </Popover>
                <Popover
                  open={editingServicesFor === invite.id}
                  onOpenChange={(o) =>
                    o ? openEditServices(invite) : setEditingServicesFor(null)
                  }
                >
                  <PopoverTrigger
                    render={(props: React.ComponentPropsWithRef<'button'>) => (
                      <Button
                        {...props}
                        size="sm"
                        variant="ghost"
                        title="Edit authorized services"
                      >
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
                          <label
                            key={svc.service_id}
                            className="flex items-center gap-2 text-sm cursor-pointer"
                          >
                            <Checkbox
                              checked={checked}
                              onCheckedChange={(v) =>
                                setEditingServicesDraft((prev) =>
                                  v
                                    ? [...prev, svc.service_id]
                                    : prev.filter((id) => id !== svc.service_id),
                                )
                              }
                            />
                            <span className="truncate">{svc.name}</span>
                          </label>
                        )
                      })}
                      {!services.length && (
                        <p className="text-xs text-muted-foreground">No services available.</p>
                      )}
                    </div>
                    <div className="flex justify-end gap-1 pt-2 border-t">
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => setEditingServicesFor(null)}
                        disabled={savingServices}
                      >
                        Cancel
                      </Button>
                      <Button
                        size="sm"
                        onClick={() => handleSaveServices(invite.id)}
                        disabled={savingServices}
                      >
                        {savingServices && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
                        Save
                      </Button>
                    </div>
                  </PopoverContent>
                </Popover>
                <Popover
                  open={editingPasscodeFor === invite.id}
                  onOpenChange={(o) =>
                    o ? openEditPasscode(invite) : setEditingPasscodeFor(null)
                  }
                >
                  <PopoverTrigger
                    render={(props: React.ComponentPropsWithRef<'button'>) => (
                      <Button
                        {...props}
                        size="sm"
                        variant="ghost"
                        title="Update passcode (re-send share card afterwards)"
                      >
                        {passcodeSavedFor === invite.id ? (
                          <Check className="h-3 w-3 text-emerald-500" />
                        ) : (
                          <KeyRound className="h-3 w-3" />
                        )}
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
                      <Button
                        size="sm"
                        variant="ghost"
                        type="button"
                        onClick={() => setPasscodeVisible((v) => !v)}
                        title={passcodeVisible ? 'Hide' : 'Show'}
                      >
                        {passcodeVisible ? (
                          <EyeOff className="h-3 w-3" />
                        ) : (
                          <Eye className="h-3 w-3" />
                        )}
                      </Button>
                    </div>
                    {passcodeError && (
                      <div className="text-[11px] text-destructive pt-1">{passcodeError}</div>
                    )}
                    <div className="flex justify-end gap-1 pt-2 border-t mt-2">
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => setEditingPasscodeFor(null)}
                        disabled={savingPasscode}
                      >
                        Cancel
                      </Button>
                      <Button
                        size="sm"
                        onClick={() => handleSavePasscode(invite.id)}
                        disabled={savingPasscode || !passcodeDraft}
                      >
                        {savingPasscode && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
                        Save
                      </Button>
                    </div>
                  </PopoverContent>
                </Popover>
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
          ))}
          {!invites.length && (
            <TableRow>
              <TableCell colSpan={5} className="text-center text-xs text-muted-foreground">
                No invitations yet.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      <CreateInviteDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        services={services}
        onCreated={onRefresh}
      />
    </section>
  )
}
