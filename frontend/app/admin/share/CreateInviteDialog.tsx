"use client"

import * as React from "react"
import { useForm } from "react-hook-form"
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
  name: z.string().min(1),
  email: z.string().email(),
  passcode: z.string(),
  duration: z.number(),
  ipWhitelist: z.string(),
  maskIps: z.boolean(),
  allowConcurrent: z.boolean(),
  queryWindow: z.number(),
  serviceIds: z.array(z.string()).min(1),
  authMode: z.enum(['passcode', 'oauth']),
  oauthProvider: z.string(),
})

function CreateInviteDialogInner({ open, onOpenChange, services, onCreated }: CreateInviteDialogProps) {
  const form = useForm<z.infer<typeof createInviteSchema>>({
    resolver: zodResolver(createInviteSchema),
    defaultValues: {
      name: '', email: '', passcode: '', duration: 24, ipWhitelist: '',
      maskIps: false, allowConcurrent: false, queryWindow: 0, serviceIds: [],
      authMode: 'passcode', oauthProvider: '',
    },
  })
  const [name, setNameState] = React.useState('')
  const [email, setEmailState] = React.useState('')
  const [passcode, setPasscodeState] = React.useState('')
  const [revealPasscode, setRevealPasscode] = React.useState(false)
  const [duration, setDurationState] = React.useState(24)
  const [ipWhitelist, setIpWhitelistState] = React.useState('')
  const [maskIps, setMaskIpsState] = React.useState(false)
  const [allowConcurrent, setAllowConcurrentState] = React.useState(false)
  const [queryWindow, setQueryWindowState] = React.useState(0)
  const [serviceIds, setServiceIdsState] = React.useState<string[]>([])
  const [creating, setCreating] = React.useState(false)
  const [error, setError] = React.useState('')
  const [authMode, setAuthModeState] = React.useState<'passcode' | 'oauth'>('passcode')
  const [oauthProvider, setOauthProviderState] = React.useState('')

  const setName = (v: string) => { setNameState(v); form.setValue('name', v) }
  const setEmail = (v: string) => { setEmailState(v); form.setValue('email', v) }
  const setPasscode = (v: string) => { setPasscodeState(v); form.setValue('passcode', v) }
  const setDuration = (v: number) => { setDurationState(v); form.setValue('duration', v) }
  const setIpWhitelist = (v: string) => { setIpWhitelistState(v); form.setValue('ipWhitelist', v) }
  const setMaskIps = (v: boolean) => { setMaskIpsState(v); form.setValue('maskIps', v) }
  const setAllowConcurrent = (v: boolean) => { setAllowConcurrentState(v); form.setValue('allowConcurrent', v) }
  const setQueryWindow = (v: number) => { setQueryWindowState(v); form.setValue('queryWindow', v) }
  const setServiceIds: React.Dispatch<React.SetStateAction<string[]>> = (updater) => {
    setServiceIdsState((prev) => {
      const next = typeof updater === 'function' ? updater(prev) : updater
      form.setValue('serviceIds', next)
      return next
    })
  }
  const setAuthMode = (v: 'passcode' | 'oauth') => { setAuthModeState(v); form.setValue('authMode', v) }
  const setOauthProvider = (v: string) => { setOauthProviderState(v); form.setValue('oauthProvider', v) }
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
      setPasscode(data?.passcode ?? '')
    } catch (e) {
      setError(extractApiError(e))
    }
  }

  const handleSubmit = async () => {
    setError('')
    setCreating(true)
    try {
      await unwrapClient(
        client.POST('/api/admin/share/invites', {
          body: {
            name, email,
            ...(authMode === 'passcode' ? { passcode } : {}),
            auth_method: authMode,
            ...(authMode === 'oauth' ? { oauth_provider: oauthProvider } : {}),
            duration_hours: duration || null,
            ip_whitelist: ipWhitelist || null,
            service_ids: serviceIds,
            pii_policy: { mask_ips: maskIps },
            query_window_hours: queryWindow || null,
            allow_concurrent_sessions: allowConcurrent,
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
  }

  const canSubmit =
    !creating && !!name && !!email && serviceIds.length > 0 &&
    (authMode === 'passcode' ? !!passcode : !!oauthProvider)
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

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="space-y-1">
              <Label htmlFor="invite-name" className="text-xs">Name</Label>
              <Input id="invite-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="Jane Doe" />
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-email" className="text-xs">Email</Label>
              <Input id="invite-email" type="email" value={email} onChange={(e) => setEmail(e.target.value)} placeholder="jane@example.com" />
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
                  <input type="radio" name="invite-auth-mode" value="passcode" checked={authMode === 'passcode'} onChange={() => setAuthMode('passcode')} />
                  Passcode (local)
                </label>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input type="radio" name="invite-auth-mode" value="oauth" checked={authMode === 'oauth'} onChange={() => setAuthMode('oauth')} />
                  SSO / OIDC
                </label>
              </div>
            </fieldset>

            {authMode === 'passcode' && (
              <div className="space-y-1 md:col-span-2">
                <Label htmlFor="invite-passcode" className="text-xs">Passcode</Label>
                <div className="flex items-center gap-2">
                  <Input
                    id="invite-passcode"
                    type={revealPasscode ? 'text' : 'password'}
                    value={passcode}
                    onChange={(e) => setPasscode(e.target.value)}
                    placeholder="ocean-breeze-cabin-42"
                  />
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
                  <Select value={oauthProvider} onValueChange={(v) => setOauthProvider(v ?? '')}>
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
              <Select value={String(duration)} onValueChange={(v) => setDuration(Number(v))}>
                <SelectTrigger id="invite-expiration"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {DURATION_OPTIONS.map((opt) => (
                    <SelectItem key={opt.value} value={String(opt.value)}>{opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-query-window" className="text-xs">Query window</Label>
              <Select value={String(queryWindow)} onValueChange={(v) => setQueryWindow(Number(v))}>
                <SelectTrigger id="invite-query-window"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {QUERY_WINDOW_OPTIONS.map((opt) => (
                    <SelectItem key={opt.value} value={String(opt.value)}>{opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1 md:col-span-2">
              <Label htmlFor="invite-ipwl" className="text-xs">IP whitelist (optional)</Label>
              <Input id="invite-ipwl" value={ipWhitelist} onChange={(e) => setIpWhitelist(e.target.value)} placeholder="192.168.1.50, 10.0.0.0/24" />
            </div>
            <div className="md:col-span-2 space-y-2">
              <Label className="text-xs">Authorized services <span className="text-destructive">*</span></Label>
              <div className="grid grid-cols-2 gap-2">
                {services.map((svc) => {
                  const checked = serviceIds.includes(svc.service_id)
                  return (
                    <label key={svc.service_id} className="flex items-center gap-2 text-sm cursor-pointer">
                      <Checkbox
                        checked={checked}
                        onCheckedChange={(v) =>
                          setServiceIds((prev) => v ? [...prev, svc.service_id] : prev.filter((id) => id !== svc.service_id))
                        }
                      />
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
              <Checkbox id="invite-mask-ips" checked={maskIps} onCheckedChange={(v) => setMaskIps(!!v)} />
              <Label htmlFor="invite-mask-ips" className="text-xs cursor-pointer">Anonymize client IPs (mask PII)</Label>
            </div>
            <div className="md:col-span-2 space-y-1">
              <div className="flex items-center gap-2">
                <Checkbox id="invite-allow-concurrent" checked={allowConcurrent} onCheckedChange={(v) => setAllowConcurrent(!!v)} />
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
        </div>

        <DialogFooter className="px-6 py-3 border-t bg-muted/10 shrink-0">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={creating}>Cancel</Button>
          <Button onClick={handleSubmit} disabled={!canSubmit}>
            {creating && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            Create invite
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
