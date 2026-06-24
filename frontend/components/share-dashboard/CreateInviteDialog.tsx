'use client'

import * as React from 'react'
import { Eye, EyeOff, KeyRound, Loader2, Sparkles } from 'lucide-react'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { client, extractApiError } from '@/lib/api'
import { cn } from '@/lib/utils'
import { panelDialogContent, panelDialogHeaderSolid } from '@/lib/panel-dialog'

import type { ShareStatus } from './utils'

const DURATION_OPTIONS = [
  { label: '1 hour', value: 1 },
  { label: '1 day', value: 24 },
  { label: '7 days', value: 168 },
  { label: 'Unlimited', value: 0 },
]

const QUERY_WINDOW_OPTIONS = [
  { label: 'Unlimited', value: 0 },
  { label: 'Last 2 hours', value: 2 },
  { label: 'Last 24 hours', value: 24 },
  { label: 'Last 7 days', value: 168 },
]

interface CreateInviteDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  services: ShareStatus['services']
  onCreated: () => Promise<void> | void
}

export function CreateInviteDialog(props: CreateInviteDialogProps) {
  // Parent gates mounting on `open` so each open spins up a fresh form;
  // returning null when closed avoids setState-in-effect resets.
  if (!props.open) return null
  return <CreateInviteDialogInner {...props} />
}

function CreateInviteDialogInner({
  open,
  onOpenChange,
  services,
  onCreated,
}: CreateInviteDialogProps) {
  const [name, setName] = React.useState('')
  const [email, setEmail] = React.useState('')
  const [passcode, setPasscode] = React.useState('')
  const [revealPasscode, setRevealPasscode] = React.useState(false)
  const [duration, setDuration] = React.useState(24)
  const [ipWhitelist, setIpWhitelist] = React.useState('')
  const [maskIps, setMaskIps] = React.useState(false)
  const [queryWindow, setQueryWindow] = React.useState(0)
  const [serviceIds, setServiceIds] = React.useState<string[]>([])
  const [creating, setCreating] = React.useState(false)
  const [error, setError] = React.useState('')

  const handleWordphrase = async () => {
    try {
      const { data, response } = await client.GET('/api/admin/share/wordphrase' as any, {})
      if (!response.ok) throw new Error(`status ${response.status}`)
      setPasscode((data as any).passcode)
    } catch (e: any) {
      setError(extractApiError(e))
    }
  }

  const handleSubmit = async () => {
    setError('')
    setCreating(true)
    try {
      await client.POST('/api/admin/share/invites' as any, {
        body: {
          name,
          email,
          passcode,
          duration_hours: duration || null,
          ip_whitelist: ipWhitelist || null,
          service_ids: serviceIds,
          pii_policy: { mask_ips: maskIps },
          query_window_hours: queryWindow || null,
        },
      } as any)
      await onCreated()
      onOpenChange(false)
    } catch (e: any) {
      setError(extractApiError(e))
    } finally {
      setCreating(false)
    }
  }

  const canSubmit = !creating && !!name && !!email && !!passcode

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
          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="space-y-1">
              <Label htmlFor="invite-name" className="text-xs">Name</Label>
              <Input
                id="invite-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Jane Doe"
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-email" className="text-xs">Email</Label>
              <Input
                id="invite-email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="jane@example.com"
              />
            </div>
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
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={handleWordphrase}
                  className="gap-1"
                >
                  <Sparkles className="h-3 w-3" />
                  Wordphrase
                </Button>
              </div>
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-expiration" className="text-xs">Expiration</Label>
              <Select
                value={String(duration)}
                onValueChange={(v) => setDuration(Number(v))}
              >
                <SelectTrigger id="invite-expiration">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DURATION_OPTIONS.map((opt) => (
                    <SelectItem key={opt.value} value={String(opt.value)}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-query-window" className="text-xs">Query window</Label>
              <Select
                value={String(queryWindow)}
                onValueChange={(v) => setQueryWindow(Number(v))}
              >
                <SelectTrigger id="invite-query-window">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {QUERY_WINDOW_OPTIONS.map((opt) => (
                    <SelectItem key={opt.value} value={String(opt.value)}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1 md:col-span-2">
              <Label htmlFor="invite-ipwl" className="text-xs">IP whitelist (optional)</Label>
              <Input
                id="invite-ipwl"
                value={ipWhitelist}
                onChange={(e) => setIpWhitelist(e.target.value)}
                placeholder="192.168.1.50, 10.0.0.0/24"
              />
            </div>
            <div className="md:col-span-2 space-y-2">
              <Label className="text-xs">Authorized services</Label>
              <div className="grid grid-cols-2 gap-2">
                {services.map((svc) => {
                  const checked = serviceIds.includes(svc.service_id)
                  return (
                    <label
                      key={svc.service_id}
                      className="flex items-center gap-2 text-sm cursor-pointer"
                    >
                      <Checkbox
                        checked={checked}
                        onCheckedChange={(v) => {
                          setServiceIds((prev) =>
                            v
                              ? [...prev, svc.service_id]
                              : prev.filter((id) => id !== svc.service_id),
                          )
                        }}
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
            </div>
            <div className="md:col-span-2 flex items-center gap-2">
              <Checkbox
                id="invite-mask-ips"
                checked={maskIps}
                onCheckedChange={(v) => setMaskIps(!!v)}
              />
              <Label htmlFor="invite-mask-ips" className="text-xs cursor-pointer">
                Anonymize client IPs (mask PII)
              </Label>
            </div>
          </div>
        </div>

        <DialogFooter className="px-6 py-3 border-t bg-muted/10 shrink-0">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={creating}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={!canSubmit}>
            {creating && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            Create invite
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
