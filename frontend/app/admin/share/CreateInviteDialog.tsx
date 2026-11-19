"use client"

import * as React from "react"
import { Controller, useForm, useWatch } from "react-hook-form"
import { zodResolver } from "@hookform/resolvers/zod"
import { z } from "zod"
import { Eye, EyeOff, KeyRound, Loader2, Sparkles } from "lucide-react"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { client, extractApiError, unwrapClient } from "@/lib/api"
import { IS_CLOUD_MODE } from "@/lib/cloud-engine/mode"
import { cn, panelDialogContent, panelDialogHeaderSolid } from "@/lib/utils"
import type { ShareStatus } from "./types"

export const DURATION_OPTIONS = [
  { label: '1 hour', value: 1 },
  { label: '1 day', value: 24 },
  { label: '7 days', value: 168 },
  { label: 'Unlimited', value: 0 },
]

export const QUERY_WINDOW_OPTIONS = [
  { label: 'Unlimited', value: 0 },
  { label: 'Last 2 hours', value: 2 },
  { label: 'Last 24 hours', value: 24 },
  { label: 'Last 7 days', value: 168 },
]

export interface CreateInviteDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  services: ShareStatus['services']
  onCreated: () => Promise<void> | void
}

export function CreateInviteDialog(props: CreateInviteDialogProps) {
  if (!props.open) return null
  return <CreateInviteDialogInner {...props} />
}

export const createInviteSchema = z.object({
  name: z.string().trim().min(1),
  email: z.string(),
  passcode: z.string(),
  duration: z.number(),
  ipWhitelist: z.string(),
  maskIps: z.boolean(),
  allowConcurrent: z.boolean(),
  queryWindow: z.number(),
  serviceIds: z.array(z.string()),
  authMode: z.enum(['passcode', 'oauth']),
  oauthProvider: z.string(),
  cloudRole: z.enum(['analyst', 'analyst-no-pii', 'admin']),
}).superRefine((values, context) => {
  if (IS_CLOUD_MODE) return
  if (!z.string().email().safeParse(values.email).success) {
    context.addIssue({ code: 'custom', path: ['email'], message: 'Enter a valid email address' })
  }
  if (values.authMode === 'passcode' && !values.passcode.trim()) {
    context.addIssue({ code: 'custom', path: ['passcode'], message: 'Enter a passcode' })
  }
  if (values.authMode === 'oauth' && !values.oauthProvider) {
    context.addIssue({ code: 'custom', path: ['oauthProvider'], message: 'Select an identity provider' })
  }
  if (values.serviceIds.length === 0) {
    context.addIssue({ code: 'custom', path: ['serviceIds'], message: 'Select a service' })
  }
})

function CreateInviteDialogInner({ open, onOpenChange, services, onCreated }: CreateInviteDialogProps) {
  const form = useForm<z.infer<typeof createInviteSchema>>({
    resolver: zodResolver(createInviteSchema),
    mode: 'onChange',
    defaultValues: {
      name: '', email: '', passcode: '', duration: 24, ipWhitelist: '',
      maskIps: false, allowConcurrent: false, queryWindow: 0, serviceIds: [],
      authMode: 'passcode', oauthProvider: '', cloudRole: 'analyst',
    },
  })
  const [revealPasscode, setRevealPasscode] = React.useState(false)
  const [creating, setCreating] = React.useState(false)
  const [error, setError] = React.useState('')
  const [createdCloudInviteUrl, setCreatedCloudInviteUrl] = React.useState<string | null>(null)
  const [copiedCloudLink, setCopiedCloudLink] = React.useState(false)
  const watched = useWatch({ control: form.control }) as z.infer<typeof createInviteSchema>
  const { name, authMode, oauthProvider, serviceIds } = watched
  const [providers, setProviders] = React.useState<Array<{ id: string; display_name: string; enabled: boolean }>>([])

  React.useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const data = await unwrapClient(client.GET('/api/admin/share/oauth-providers', {}))
        if (!cancelled) setProviders(data?.providers ?? [])
      } catch {
        /* leave empty — OAuth mode just stays disabled */
      }
    })()
    return () => { cancelled = true }
  }, [])

  const handleWordphrase = async () => {
    try {
      const data = await unwrapClient(client.GET('/api/admin/share/wordphrase', {}))
      form.setValue('passcode', data?.passcode ?? '', { shouldValidate: true, shouldDirty: true })
    } catch (e) {
      setError(extractApiError(e))
    }
  }

  const handleSubmit = form.handleSubmit(async (values) => {
    setError('')
    setCreating(true)
    try {
      if (IS_CLOUD_MODE) {
        const res = await fetch('/api/admin/invites', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({ display_name: values.name, role: values.cloudRole }),
        })
        const data = (await res.json().catch(() => ({}))) as { token?: string; detail?: string; error?: string }
        if (!res.ok) {
          throw new Error(data.detail || data.error || `Invite creation failed (${res.status})`)
        }
        await onCreated()
        if (data.token && typeof window !== 'undefined') {
          setCreatedCloudInviteUrl(`${window.location.origin}/share-login?token=${encodeURIComponent(data.token)}`)
          return
        }
        onOpenChange(false)
        return
      }
      await unwrapClient(
        client.POST('/api/admin/share/invites', {
          body: {
            name: values.name, email: values.email,
            ...(values.authMode === 'passcode' ? { passcode: values.passcode } : {}),
            auth_method: values.authMode,
            ...(values.authMode === 'oauth' ? { oauth_provider: values.oauthProvider } : {}),
            duration_hours: values.duration || null,
            ip_whitelist: values.ipWhitelist || null,
            service_ids: values.serviceIds,
            pii_policy: { mask_ips: values.maskIps },
            query_window_hours: values.queryWindow || null,
            allow_concurrent_sessions: values.allowConcurrent,
          },
        }),
      )
      await onCreated()
      onOpenChange(false)
    } catch (e) {
      setError(extractApiError(e))
    } finally {
      setCreating(false)
    }
  })

  const canSubmit = !creating && form.formState.isValid
  const providerLabel = providers.find((p) => p.id === oauthProvider)?.display_name ?? ''

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn('sm:max-w-2xl', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2 text-xl font-bold">
            <KeyRound className="h-5 w-5" />
            New invitation
          </DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0 px-6 py-4 space-y-3">
          {error && <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert>}

          {IS_CLOUD_MODE ? (
            createdCloudInviteUrl ? (
              <div className="space-y-3" data-testid="cloud-invite-link-result">
                <p className="text-xs text-muted-foreground">
                  Share this one-time passkey registration link with <strong>{name}</strong>. The token is only shown once.
                </p>
                <div className="flex items-center gap-2">
                  <Input readOnly value={createdCloudInviteUrl} className="font-mono text-xs" />
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      void navigator.clipboard?.writeText(createdCloudInviteUrl)
                      setCopiedCloudLink(true)
                    }}
                  >
                    {copiedCloudLink ? 'Copied' : 'Copy Link'}
                  </Button>
                </div>
              </div>
            ) : (
              <div className="space-y-3">
                <div className="space-y-1">
                  <Label htmlFor="invite-name" className="text-xs">Display Name</Label>
                  <Controller control={form.control} name="name" render={({ field }) => <Input id="invite-name" placeholder="Jane Doe" {...field} />} />
                </div>
                <div className="space-y-1">
                  <Label htmlFor="cloud-invite-role" className="text-xs">Role</Label>
                  <Controller control={form.control} name="cloudRole" render={({ field }) => (
                    <Select value={field.value} onValueChange={field.onChange}>
                      <SelectTrigger id="cloud-invite-role"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="analyst">Analyst (read-only dashboard access)</SelectItem>
                        <SelectItem value="analyst-no-pii">Analyst — No PII (client IPs masked)</SelectItem>
                        <SelectItem value="admin">Administrator (full control plane access)</SelectItem>
                      </SelectContent>
                    </Select>
                  )} />
                </div>
              </div>
            )
          ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="space-y-1">
              <Label htmlFor="invite-name" className="text-xs">Name</Label>
              <Controller control={form.control} name="name" render={({ field }) => <Input id="invite-name" placeholder="Jane Doe" {...field} />} />
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-email" className="text-xs">Email</Label>
              <Controller control={form.control} name="email" render={({ field }) => <Input id="invite-email" type="email" placeholder="jane@example.com" {...field} />} />
              {authMode === 'oauth' && (
                <p className="text-[11px] text-muted-foreground">
                  The analyst must sign in with the {providerLabel || 'provider'} account for this exact address.
                </p>
              )}
            </div>

            <fieldset className="md:col-span-2 space-y-1">
              <legend className="text-xs font-medium">Sign-in method</legend>
              <div className="flex gap-6 pt-1" role="radiogroup" aria-label="Sign-in method">
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <Controller control={form.control} name="authMode" render={({ field }) => (
                    <input type="radio" value="passcode" checked={field.value === 'passcode'} onChange={() => field.onChange('passcode')} name={field.name} />
                  )} />
                  Passcode (local)
                </label>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <Controller control={form.control} name="authMode" render={({ field }) => (
                    <input type="radio" value="oauth" checked={field.value === 'oauth'} onChange={() => field.onChange('oauth')} name={field.name} />
                  )} />
                  SSO / OIDC
                </label>
              </div>
            </fieldset>

            {authMode === 'passcode' && (
              <div className="space-y-1 md:col-span-2">
                <Label htmlFor="invite-passcode" className="text-xs">Passcode</Label>
                <div className="flex items-center gap-2">
                  <Controller control={form.control} name="passcode" render={({ field }) => (
                    <Input
                      id="invite-passcode"
                      type={revealPasscode ? 'text' : 'password'}
                      placeholder="ocean-breeze-cabin-42"
                      {...field}
                    />
                  )} />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    aria-label={revealPasscode ? 'Hide passcode' : 'Reveal passcode'}
                    onClick={() => setRevealPasscode((r) => !r)}
                    title={revealPasscode ? 'Hide passcode' : 'Reveal passcode'}
                  >
                    {revealPasscode ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                  </Button>
                  <Button type="button" variant="outline" size="sm" onClick={handleWordphrase} className="gap-1">
                    <Sparkles className="h-3 w-3" />
                    Wordphrase
                  </Button>
                </div>
              </div>
            )}

            {authMode === 'oauth' && (
              <div className="space-y-1 md:col-span-2">
                <Label htmlFor="invite-oauth-provider" className="text-xs">Identity provider</Label>
                {providers.length > 0 ? (
                  <Controller control={form.control} name="oauthProvider" render={({ field }) => (
                    <Select value={field.value} onValueChange={field.onChange}>
                      <SelectTrigger id="invite-oauth-provider">
                        <SelectValue placeholder="Select a provider" />
                      </SelectTrigger>
                      <SelectContent>
                        {providers.map((p) => (
                          <SelectItem key={p.id} value={p.id}>
                            {p.display_name}{!p.enabled ? ' (disabled)' : ''}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  )} />
                ) : (
                  <>
                    <Select disabled>
                      <SelectTrigger id="invite-oauth-provider">
                        <SelectValue placeholder="No identity providers configured" />
                      </SelectTrigger>
                      <SelectContent />
                    </Select>
                    <p className="text-xs text-muted-foreground">
                      No identity providers configured — add one to the registry first.
                    </p>
                  </>
                )}
              </div>
            )}
            <div className="space-y-1">
              <Label htmlFor="invite-expiration" className="text-xs">Expiration</Label>
              <Controller control={form.control} name="duration" render={({ field }) => (
                <Select value={String(field.value)} onValueChange={(value) => field.onChange(Number(value))}>
                  <SelectTrigger id="invite-expiration"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {DURATION_OPTIONS.map((opt) => (
                      <SelectItem key={opt.value} value={String(opt.value)}>{opt.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )} />
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-query-window" className="text-xs">Query window</Label>
              <Controller control={form.control} name="queryWindow" render={({ field }) => (
                <Select value={String(field.value)} onValueChange={(value) => field.onChange(Number(value))}>
                  <SelectTrigger id="invite-query-window"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {QUERY_WINDOW_OPTIONS.map((opt) => (
                      <SelectItem key={opt.value} value={String(opt.value)}>{opt.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )} />
            </div>
            <div className="space-y-1 md:col-span-2">
              <Label htmlFor="invite-ipwl" className="text-xs">IP whitelist (optional)</Label>
              <Controller control={form.control} name="ipWhitelist" render={({ field }) => <Input id="invite-ipwl" placeholder="192.168.1.50, 10.0.0.0/24" {...field} />} />
            </div>
            <div className="md:col-span-2 space-y-2">
              <Label className="text-xs">Authorized services <span className="text-destructive">*</span></Label>
              <div className="grid grid-cols-2 gap-2">
                {services.map((svc) => {
                  const checked = serviceIds.includes(svc.service_id)
                  return (
                    <label key={svc.service_id} className="flex items-center gap-2 text-sm cursor-pointer">
                      <Controller control={form.control} name="serviceIds" render={({ field }) => (
                        <Checkbox
                          checked={checked}
                          onCheckedChange={(value) =>
                            field.onChange(value ? [...field.value, svc.service_id] : field.value.filter((id) => id !== svc.service_id))
                          }
                        />
                      )} />
                      <span className="truncate">{svc.name}</span>
                    </label>
                  )
                })}
                {!services.length && (
                  <p className="text-xs text-muted-foreground col-span-2">
                    No services configured. Provision a service first.
                  </p>
                )}
              </div>
              {!!services.length && serviceIds.length === 0 && (
                <p className="text-xs text-muted-foreground">Select at least one service this analyst can view.</p>
              )}
            </div>
            <div className="md:col-span-2 flex items-center gap-2">
              <Controller control={form.control} name="maskIps" render={({ field }) => (
                <Checkbox id="invite-mask-ips" checked={field.value} onCheckedChange={(value) => field.onChange(!!value)} />
              )} />
              <Label htmlFor="invite-mask-ips" className="text-xs cursor-pointer">Anonymize client IPs (mask PII)</Label>
            </div>
            <div className="md:col-span-2 space-y-1">
              <div className="flex items-center gap-2">
                <Controller control={form.control} name="allowConcurrent" render={({ field }) => (
                  <Checkbox id="invite-allow-concurrent" checked={field.value} onCheckedChange={(value) => field.onChange(!!value)} />
                )} />
                <Label htmlFor="invite-allow-concurrent" className="text-xs cursor-pointer">
                  Allow multiple people to use this link at the same time
                </Label>
              </div>
              <p className="text-xs text-muted-foreground">
                Off (default): each new login signs out the previous one. On: several
                people can share this link — audit events won&apos;t distinguish between them.
              </p>
            </div>
            <div className="md:col-span-2 rounded-md bg-muted/40 p-2 text-xs text-muted-foreground">
              Note: Analysts are required to review and agree to the Terms of Access upon their first login before dashboard access is granted.
            </div>
          </div>
          )}
        </div>

        <DialogFooter className="px-6 py-3 border-t bg-muted/10 shrink-0">
          {createdCloudInviteUrl ? (
            <Button onClick={() => onOpenChange(false)}>Done</Button>
          ) : (
            <>
              <Button variant="outline" onClick={() => onOpenChange(false)} disabled={creating}>Cancel</Button>
              <Button onClick={handleSubmit} disabled={!canSubmit}>
                {creating && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
                Create invite
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
