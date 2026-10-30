'use client'

import * as React from 'react'
import { useForm, useWatch, type UseFormSetValue } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useMutation } from '@tanstack/react-query'
import {
  Globe, Server, CheckCircle2, AlertCircle, Loader2,
  Sparkles, ExternalLink, AlertTriangle,
} from 'lucide-react'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/confirm-dialog'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { client, extractApiError } from '@/lib/api'
import { cn, panelDialogContent, panelDialogHeaderSolid, panelDialogFooter } from '@/lib/utils'
import { useOpenGeneration } from '@/hooks/useSSE'
import type { Schema } from '@/types/api'

export const domainStepSchema = z.object({
  isCustom: z.boolean(),
  prefix: z.string().min(3),
})

export const originStepSchema = z.object({
  originHost: z.string().min(1),
  originPort: z.number().int().min(1).max(65535),
  useSsl: z.boolean(),
  overrideHost: z.string(),
})

export const deployRemoteFrontendSchema = domainStepSchema.merge(originStepSchema).extend({
  serviceNameDraft: z.string(),
  isServiceNameEdited: z.boolean(),
  tokenOverride: z.string(),
})
export type DeployRemoteFrontendValues = z.infer<typeof deployRemoteFrontendSchema>

const teardownRemoteFrontendSchema = z.object({
  tokenOverride: z.string(),
})
type TeardownRemoteFrontendValues = z.infer<typeof teardownRemoteFrontendSchema>

export function DeployStepDomain({
  isCustom,
  prefix,
  setValue,
  checkingDomain,
  domainCheckResult,
  setDomainCheckResult,
  onCheckDomain,
}: {
  isCustom: boolean
  prefix: string
  setValue: UseFormSetValue<DeployRemoteFrontendValues>
  checkingDomain: boolean
  domainCheckResult: Schema<'ProvisionCheckDomainResponse'> | null
  setDomainCheckResult: (v: Schema<'ProvisionCheckDomainResponse'> | null) => void
  onCheckDomain: () => void
}) {
  return (
    <div className="space-y-4 animate-in fade-in slide-in-from-bottom-2 duration-200">
      <div className="space-y-1.5">
        <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
          Domain Provider / Type
        </Label>
        <div className="grid grid-cols-2 gap-3" role="radiogroup" aria-label="Domain Type">
          <button
            type="button"
            role="radio"
            aria-checked={!isCustom}
            onClick={() => {
              setValue('isCustom', false)
              setValue('prefix', '')
              setDomainCheckResult(null)
            }}
            className={cn(
              'flex flex-col items-start p-3.5 rounded-lg border text-left transition-all hover:bg-muted/50 focus:outline-none focus:ring-2 focus:ring-primary',
              !isCustom ? 'border-primary bg-primary/5 ring-1 ring-primary' : 'border-border bg-background'
            )}
          >
            <span className="text-sm font-semibold flex items-center gap-1.5">
              <Globe className="h-4 w-4 text-primary" /> Fastly SSL Domain
            </span>
            <span className="text-[11px] text-muted-foreground mt-1 leading-relaxed">
              Free shared subdomain under Fastly SSL Gateway.
            </span>
          </button>

          <button
            type="button"
            role="radio"
            aria-checked={isCustom}
            onClick={() => {
              setValue('isCustom', true)
              setValue('prefix', '')
              setDomainCheckResult(null)
            }}
            className={cn(
              'flex flex-col items-start p-3.5 rounded-lg border text-left transition-all hover:bg-muted/50 focus:outline-none focus:ring-2 focus:ring-primary',
              isCustom ? 'border-primary bg-primary/5 ring-1 ring-primary' : 'border-border bg-background'
            )}
          >
            <span className="text-sm font-semibold flex items-center gap-1.5">
              <Sparkles className="h-4 w-4 text-primary" /> Custom Domain
            </span>
            <span className="text-[11px] text-muted-foreground mt-1 leading-relaxed">
              Point your custom domain/CNAME to Fastly edge.
            </span>
          </button>
        </div>
      </div>

      {!isCustom ? (
        <div className="space-y-1.5">
          <Label htmlFor="domain-prefix" className="text-xs font-semibold">Subdomain Prefix</Label>
          <div className="flex items-center">
            <Input
              id="domain-prefix"
              value={prefix}
              onChange={(e) => {
                setValue('prefix', e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, ''))
                setDomainCheckResult(null)
              }}
              placeholder="my-dashboard-prefix"
              className="rounded-r-none border-r-0 h-10"
            />
            <span className="inline-flex h-10 items-center rounded-r-md border border-input bg-muted px-3 text-xs text-muted-foreground font-mono select-none">
              .global.ssl.fastly.net
            </span>
          </div>
          <p className="text-[11px] text-muted-foreground">Only alphanumeric characters and hyphens allowed.</p>
        </div>
      ) : (
        <div className="space-y-1.5">
          <Label htmlFor="custom-domain" className="text-xs font-semibold">Custom Domain Name</Label>
          <Input
            id="custom-domain"
            value={prefix}
            onChange={(e) => {
              setValue('prefix', e.target.value.toLowerCase().trim())
              setDomainCheckResult(null)
            }}
            placeholder="dashboard.mycompany.com"
            className="h-10"
          />
          <p className="text-[11px] text-muted-foreground">
            Ensure you have configured proper DNS mapping to route traffic to Fastly.
          </p>
        </div>
      )}

      <div className="flex items-center gap-3 pt-1">
        <Button
          type="button"
          variant="secondary"
          size="sm"
          disabled={checkingDomain || !prefix || prefix.length < 3}
          onClick={onCheckDomain}
          className="gap-1.5"
        >
          {checkingDomain ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Globe className="h-3.5 w-3.5" />}
          Check Domain Availability
        </Button>

        {domainCheckResult && (
          <div className="flex items-center gap-1.5 text-xs">
            {domainCheckResult.available ? (
              <span className="text-green-600 font-semibold flex items-center gap-1">
                <CheckCircle2 className="h-3.5 w-3.5 text-green-500" /> Domain is available!
              </span>
            ) : (
              <span className="text-destructive font-semibold flex items-center gap-1">
                <AlertCircle className="h-3.5 w-3.5 text-destructive" />
                {domainCheckResult.reason || 'This domain is already in use.'}
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

export function DeployStepOrigin({
  originHost,
  originPort,
  useSsl,
  overrideHost,
  setValue,
}: {
  originHost: string
  originPort: number
  useSsl: boolean
  overrideHost: string
  setValue: UseFormSetValue<DeployRemoteFrontendValues>
}) {
  return (
    <div className="space-y-4 animate-in fade-in slide-in-from-bottom-2 duration-200">
      <div className="space-y-1.5">
        <Label htmlFor="origin-host" className="text-xs font-semibold">Origin Host / VM IP</Label>
        <div className="relative">
          <Input
            id="origin-host"
            value={originHost}
            onChange={(e) => setValue('originHost', e.target.value.trim())}
            placeholder="e.g. 203.0.113.10"
            className="h-10 pl-9"
          />
          <Server className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
        </div>
        <p className="text-[11px] text-muted-foreground">
          The host address or external VM IP of the analytics backend dashboard server.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-1.5">
          <Label htmlFor="origin-port" className="text-xs font-semibold">Backend Port</Label>
          <Input
            id="origin-port"
            type="number"
            value={originPort}
            onChange={(e) => setValue('originPort', Number(e.target.value))}
            placeholder="80"
            className="h-10"
            min={1}
            max={65535}
          />
        </div>

        <div className="flex flex-col justify-end">
          <div className="flex items-center justify-between border rounded-lg px-3.5 py-2 bg-muted/5 h-10">
            <div className="space-y-0.5">
              <Label htmlFor="use-ssl" className="text-xs font-medium cursor-pointer">Use SSL / HTTPS</Label>
            </div>
            <Switch id="use-ssl" checked={useSsl} onCheckedChange={(v) => setValue('useSsl', v)} />
          </div>
        </div>
      </div>

      <div className="space-y-1.5">
        <Label htmlFor="override-host" className="text-xs font-semibold">Host Header Override (Optional)</Label>
        <Input
          id="override-host"
          value={overrideHost}
          onChange={(e) => setValue('overrideHost', e.target.value.trim())}
          placeholder="None - default"
          className="h-10"
        />
        <p className="text-[11px] text-muted-foreground">
          Overrides the Host header sent to the backend. Defaults to the backend origin host if empty.
        </p>
      </div>
    </div>
  )
}

export function DeployStepSuccess({ deployResult }: { deployResult: Schema<'SharingDeployResponse'> }) {
  return (
    <div className="text-center py-6 space-y-5 animate-in zoom-in-95 duration-300">
      <div className="inline-flex items-center justify-center h-16 w-16 rounded-full bg-green-500/10 text-green-500 ring-4 ring-green-500/5">
        <CheckCircle2 className="h-10 w-10" />
      </div>

      <div className="space-y-1.5">
        <h3 className="text-lg font-bold tracking-tight">Deployment Complete!</h3>
        <p className="text-xs text-muted-foreground px-4 leading-relaxed">
          Your remote frontend service has been successfully created, configured, and activated on Fastly CDN.
        </p>
      </div>

      {/* Target Link card */}
      <div className="border border-green-500/20 bg-green-500/5 rounded-lg p-4 max-w-sm mx-auto text-left space-y-2">
        <Label className="text-[10px] font-bold text-green-700 uppercase tracking-widest block">
          Active Live Sharing Domain
        </Label>
        <div className="flex items-center justify-between gap-3 bg-background border rounded px-3 py-2">
          <a
            href={`https://${deployResult.domain_name}`}
            target="_blank"
            rel="noopener noreferrer"
            className="font-mono text-xs text-primary font-medium hover:underline truncate flex items-center gap-1.5"
          >
            https://{deployResult.domain_name}
            <ExternalLink className="h-3.5 w-3.5 flex-shrink-0" />
          </a>
          <CopyButton
            text={`https://${deployResult.domain_name}`}
            className="h-8 w-8 text-muted-foreground hover:text-foreground shrink-0"
            iconClassName="h-4 w-4"
          />
        </div>
      </div>

      {/* Technical breakdown */}
      <div className="border rounded-lg p-3.5 text-xs text-left max-w-sm mx-auto space-y-1.5 bg-muted/20">
        <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-2">
          Fastly Deployment Specs
        </p>
        <div className="flex justify-between font-mono text-[11px]">
          <span className="text-muted-foreground">Service ID:</span>
          <span className="font-semibold select-all">{deployResult.service_id}</span>
        </div>
        <div className="flex justify-between font-mono text-[11px]">
          <span className="text-muted-foreground">Version:</span>
          <span className="font-semibold">#{deployResult.version}</span>
        </div>
        <div className="flex justify-between font-mono text-[11px]">
          <span className="text-muted-foreground">Origin Host:</span>
          <span className="font-semibold truncate max-w-[200px]">{deployResult.origin_host}</span>
        </div>
      </div>
    </div>
  )
}

export interface TeardownRemoteFrontendDialogProps {
  serviceId: string
  domainName: string
  onSuccess: () => void
}

export function TeardownRemoteFrontendDialog(props: TeardownRemoteFrontendDialogProps) {
  const [open, setOpen] = React.useState(false)
  const generation = useOpenGeneration(open)

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
      <TeardownRemoteFrontendDialogBody key={generation} {...props} onClose={() => setOpen(false)} />
    </Dialog>
  )
}

function TeardownRemoteFrontendDialogBody({
  serviceId,
  domainName,
  onSuccess,
  onClose,
}: TeardownRemoteFrontendDialogProps & { onClose: () => void }) {
  const [error, setError] = React.useState('')
  const { control, setValue } = useForm<TeardownRemoteFrontendValues>({
    resolver: zodResolver(teardownRemoteFrontendSchema),
    defaultValues: { tokenOverride: '' },
  })
  const { tokenOverride } = useWatch({ control }) as TeardownRemoteFrontendValues

  const teardownMutation = useMutation({
    mutationFn: async () => {
      const { error: apiError } = await client.POST('/api/sharing/teardown-frontend', {
        body: {
          service_id: serviceId,
          token_override: tokenOverride || null,
        },
      })
      if (apiError) throw new Error(extractApiError(apiError))
    },
    onMutate: () => setError(''),
    onSuccess: () => {
      onClose()
      onSuccess()
    },
    onError: (err) => setError(extractApiError(err)),
  })
  const tearingDown = teardownMutation.isPending
  const handleTeardown = () => teardownMutation.mutate()

  return (
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
              onChange={(e) => setValue('tokenOverride', e.target.value)}
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
          <Button variant="outline" onClick={onClose} disabled={tearingDown} className="w-full sm:w-auto">
            Cancel
          </Button>
          <Button variant="destructive" onClick={handleTeardown} disabled={tearingDown} className="w-full sm:w-auto gap-1.5">
            {tearingDown && <Loader2 className="h-3 w-3 animate-spin" />}
            {tearingDown ? 'Tearing down…' : 'Teardown Remote Service'}
          </Button>
        </DialogFooter>
      </DialogContent>
  )
}
