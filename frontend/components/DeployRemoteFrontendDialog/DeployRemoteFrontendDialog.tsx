'use client'

import * as React from 'react'
import {
  Globe,
  Server,
  CheckCircle2,
  AlertCircle,
  ArrowRight,
  Loader2,
  Sparkles,
  ChevronRight,
  ChevronLeft,
  Copy,
  Check,
  ExternalLink
} from 'lucide-react'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard'
import { client, extractApiError } from '@/lib/api'
import { cn } from '@/lib/utils'
import { panelDialogContent, panelDialogHeaderSolid, panelDialogFooter } from '@/lib/panel-dialog'
import { useSearchParams } from 'next/navigation'

export function DeployRemoteFrontendDialog() {
  const searchParams = useSearchParams()
  const serviceId = searchParams?.get('service')

  const [open, setOpen] = React.useState(false)
  const [step, setStep] = React.useState<1 | 2 | 3 | 'success'>(1)

  // Step 1: Domain Selection State
  const [isCustom, setIsCustom] = React.useState(false)
  const [prefix, setPrefix] = React.useState('')
  const [checkingDomain, setCheckingDomain] = React.useState(false)
  const [domainCheckResult, setDomainCheckResult] = React.useState<{
    available: boolean
    reason?: string | null
    note?: string | null
  } | null>(null)

  // Step 2: Origin Configuration State
  const [originHost, setOriginHost] = React.useState('34.123.30.195')
  const [originPort, setOriginPort] = React.useState(80)
  const [useSsl, setUseSsl] = React.useState(false)
  const [overrideHost, setOverrideHost] = React.useState('')

  // Step 3: Trigger Deployment State
  const [serviceName, setServiceName] = React.useState('')
  const [isServiceNameEdited, setIsServiceNameEdited] = React.useState(false)
  const [tokenOverride, setTokenOverride] = React.useState('')
  const [deploying, setDeploying] = React.useState(false)
  const [deployError, setDeployError] = React.useState('')
  const [deployResult, setDeployResult] = React.useState<{
    service_id: string
    version: number
    domain_name: string
    origin_host: string
  } | null>(null)

  // Clipboard hook for copying live link
  const { copied, copy } = useCopyToClipboard()

  // Reset State on Open/Close
  React.useEffect(() => {
    if (open) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setStep(1)
      setPrefix('')
      setIsCustom(false)
      setDomainCheckResult(null)
      setOriginHost('34.123.30.195')
      setOriginPort(80)
      setUseSsl(false)
      setOverrideHost('')
      setTokenOverride('')
      setServiceName('')
      setIsServiceNameEdited(false)
      setDeployError('')
      setDeployResult(null)
    }
  }, [open])

  // Computed Domain based on step 1 selections
  const computedDomain = React.useMemo(() => {
    if (!prefix) return ''
    return isCustom ? prefix : `${prefix}.global.ssl.fastly.net`
  }, [prefix, isCustom])

  // Auto-generate service name when domain changes, unless user edited it
  React.useEffect(() => {
    if (!isServiceNameEdited && computedDomain) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setServiceName(`Fastly Log Analytics Remote Frontend - ${computedDomain}`)
    }
  }, [computedDomain, isServiceNameEdited])

  // Handle Domain Verification
  const handleCheckDomain = async () => {
    if (!prefix || prefix.length < 3) return
    setCheckingDomain(true)
    setDomainCheckResult(null)
    try {
      const { data, error } = await client.GET('/api/provision/check-domain', {
        params: {
          query: {
            prefix: prefix,
            is_custom: isCustom,
          },
        },
      })

      if (error) {
        throw new Error(extractApiError(error))
      }

      setDomainCheckResult({
        available: !!data?.available,
        reason: data?.reason || null,
        note: data?.note || null,
      })
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err)
      setDomainCheckResult({
        available: false,
        reason: errorMsg || 'Failed to verify domain status. Please try again.',
      })
    } finally {
      setCheckingDomain(false)
    }
  }

  // Handle Deploy Service Submit
  const handleDeploy = async () => {
    setDeploying(true)
    setDeployError('')
    try {
      const { data, error } = await client.POST('/api/sharing/deploy-frontend', {
        body: {
          service_name: serviceName,
          domain_name: computedDomain,
          origin_host: originHost,
          origin_port: originPort,
          use_ssl: useSsl,
          token_override: tokenOverride || null,
          override_host: overrideHost || null,
          service_id: serviceId || null,
        },
      })

      if (error) {
        throw new Error(extractApiError(error))
      }

      setDeployResult(data)
      setStep('success')
    } catch (err) {
      setDeployError(extractApiError(err))
    } finally {
      setDeploying(false)
    }
  }

  // Validation Rules
  const canGoToStep2 = domainCheckResult?.available === true && !checkingDomain && prefix.length >= 3
  const canGoToStep3 = !!originHost && originPort > 0 && originPort < 65536
  const canDeploy = !deploying && !!serviceName && canGoToStep3 && canGoToStep2

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="outline" className="gap-1.5 border-primary/20 hover:bg-primary/5">
            <Globe className="h-4 w-4 text-primary" />
            Deploy Remote Frontend
          </Button>
        }
      />

      <DialogContent className={cn('sm:max-w-lg', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2 text-xl font-bold">
            <Globe className="h-5 w-5 text-primary" />
            Deploy Remote Frontend
          </DialogTitle>
        </DialogHeader>

        {/* Step Progress Indicators */}
        {step !== 'success' && (
          <div className="flex items-center justify-between px-6 py-2.5 bg-muted/20 border-b text-[11px] text-muted-foreground select-none">
            <span className={cn('font-medium', step === 1 && 'text-primary font-semibold')}>
              1. Domain Selection
            </span>
            <ChevronRight className="h-3 w-3 text-muted-foreground/50" />
            <span className={cn('font-medium', step === 2 && 'text-primary font-semibold')}>
              2. Origin Configuration
            </span>
            <ChevronRight className="h-3 w-3 text-muted-foreground/50" />
            <span className={cn('font-medium', step === 3 && 'text-primary font-semibold')}>
              3. Deploy Service
            </span>
          </div>
        )}

        {/* Body Container */}
        <div className="flex-1 overflow-y-auto min-h-0 px-6 py-5 space-y-4">
          {/* STEP 1: Domain Selection */}
          {step === 1 && (
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
                      setIsCustom(false)
                      setPrefix('')
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
                      setIsCustom(true)
                      setPrefix('')
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
                  <Label htmlFor="domain-prefix" className="text-xs font-semibold">
                    Subdomain Prefix
                  </Label>
                  <div className="flex items-center">
                    <Input
                      id="domain-prefix"
                      value={prefix}
                      onChange={(e) => {
                        setPrefix(e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, ''))
                        setDomainCheckResult(null)
                      }}
                      placeholder="my-dashboard-prefix"
                      className="rounded-r-none border-r-0 h-10"
                    />
                    <span className="inline-flex h-10 items-center rounded-r-md border border-input bg-muted px-3 text-xs text-muted-foreground font-mono select-none">
                      .global.ssl.fastly.net
                    </span>
                  </div>
                  <p className="text-[11px] text-muted-foreground">
                    Only alphanumeric characters and hyphens allowed.
                  </p>
                </div>
              ) : (
                <div className="space-y-1.5">
                  <Label htmlFor="custom-domain" className="text-xs font-semibold">
                    Custom Domain Name
                  </Label>
                  <Input
                    id="custom-domain"
                    value={prefix}
                    onChange={(e) => {
                      setPrefix(e.target.value.toLowerCase().trim())
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
                  onClick={handleCheckDomain}
                  className="gap-1.5"
                >
                  {checkingDomain ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Globe className="h-3.5 w-3.5" />
                  )}
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
          )}

          {/* STEP 2: Origin Configuration */}
          {step === 2 && (
            <div className="space-y-4 animate-in fade-in slide-in-from-bottom-2 duration-200">
              <div className="space-y-1.5">
                <Label htmlFor="origin-host" className="text-xs font-semibold">
                  Origin Host / VM IP
                </Label>
                <div className="relative">
                  <Input
                    id="origin-host"
                    value={originHost}
                    onChange={(e) => setOriginHost(e.target.value.trim())}
                    placeholder="e.g. 34.123.30.195"
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
                  <Label htmlFor="origin-port" className="text-xs font-semibold">
                    Backend Port
                  </Label>
                  <Input
                    id="origin-port"
                    type="number"
                    value={originPort}
                    onChange={(e) => setOriginPort(Number(e.target.value))}
                    placeholder="80"
                    className="h-10"
                    min={1}
                    max={65535}
                  />
                </div>

                <div className="flex flex-col justify-end">
                  <div className="flex items-center justify-between border rounded-lg px-3.5 py-2 bg-muted/5 h-10">
                    <div className="space-y-0.5">
                      <Label htmlFor="use-ssl" className="text-xs font-medium cursor-pointer">
                        Use SSL / HTTPS
                      </Label>
                    </div>
                    <Switch id="use-ssl" checked={useSsl} onCheckedChange={setUseSsl} />
                  </div>
                </div>
              </div>

              <div className="space-y-1.5">
                <Label htmlFor="override-host" className="text-xs font-semibold">
                  Host Header Override (Optional)
                </Label>
                <Input
                  id="override-host"
                  value={overrideHost}
                  onChange={(e) => setOverrideHost(e.target.value.trim())}
                  placeholder="None - default"
                  className="h-10"
                />
                <p className="text-[11px] text-muted-foreground">
                  Overrides the Host header sent to the backend. Defaults to the backend origin host if empty.
                </p>
              </div>
            </div>
          )}

          {/* STEP 3: Trigger Deployment */}
          {step === 3 && (
            <div className="space-y-4 animate-in fade-in slide-in-from-bottom-2 duration-200">
              {deployError && (
                <Alert variant="destructive">
                  <AlertCircle className="h-4 w-4" />
                  <AlertDescription>{deployError}</AlertDescription>
                </Alert>
              )}

              <div className="space-y-1.5">
                <Label htmlFor="service-name" className="text-xs font-semibold">
                  Fastly Service Name
                </Label>
                <Input
                  id="service-name"
                  value={serviceName}
                  onChange={(e) => {
                    setServiceName(e.target.value)
                    setIsServiceNameEdited(true)
                  }}
                  placeholder="Fastly Log Analytics Remote Frontend"
                  className="h-10"
                  disabled={deploying}
                />
                <p className="text-[11px] text-muted-foreground">
                  Identifies this remote frontend proxy inside your Fastly account.
                </p>
              </div>

              {/* Advanced: Token Override */}
              <div className="pt-1">
                <details className="group border border-dashed rounded-lg p-3 bg-muted/10 text-xs">
                  <summary className="cursor-pointer font-medium text-muted-foreground select-none hover:text-foreground flex items-center gap-1.5">
                    Advanced: Override Fastly API Token
                  </summary>
                  <div className="mt-3 space-y-1.5 animate-in fade-in duration-200">
                    <Label htmlFor="token-override" className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                      Token Override (Optional)
                    </Label>
                    <Input
                      id="token-override"
                      type="password"
                      value={tokenOverride}
                      onChange={(e) => setTokenOverride(e.target.value)}
                      placeholder="fastly_..."
                      className="h-9 text-xs font-mono"
                      disabled={deploying}
                    />
                    <p className="text-[10px] text-muted-foreground leading-normal">
                      Leave empty to use the system default Fastly API Token configured in your server credentials or environment.
                    </p>
                  </div>
                </details>
              </div>

              {/* Review card */}
              <div className="border rounded-lg bg-muted/10 p-4 space-y-2.5 text-xs">
                <h4 className="font-semibold text-muted-foreground uppercase tracking-wider text-[10px]">
                  Deployment Summary
                </h4>
                <div className="grid grid-cols-2 gap-y-2 gap-x-4 pt-1 border-t border-border/50">
                  <div>
                    <p className="text-[10px] text-muted-foreground uppercase">Target Domain</p>
                    <p className="font-mono text-[11px] truncate mt-0.5" title={computedDomain}>
                      {computedDomain}
                    </p>
                  </div>
                  <div>
                    <p className="text-[10px] text-muted-foreground uppercase">Origin Backend</p>
                    <p className="font-mono text-[11px] mt-0.5">
                      {originHost}:{originPort} {useSsl ? '(SSL)' : ''}
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* STEP SUCCESS */}
          {step === 'success' && deployResult && (
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
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-8 w-8 text-muted-foreground hover:text-foreground shrink-0"
                    onClick={() => copy(`https://${deployResult.domain_name}`)}
                  >
                    {copied ? <Check className="h-4 w-4 text-green-500" /> : <Copy className="h-4 w-4" />}
                  </Button>
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
          )}
        </div>

        {/* Dialog Footer */}
        <DialogFooter className={panelDialogFooter}>
          {step === 1 && (
            <div className="flex justify-between w-full items-center">
              <Button variant="outline" onClick={() => setOpen(false)}>
                Close
              </Button>
              <Button disabled={!canGoToStep2} onClick={() => setStep(2)} className="gap-1.5">
                Next <ArrowRight className="h-4 w-4" />
              </Button>
            </div>
          )}

          {step === 2 && (
            <div className="flex justify-between w-full items-center">
              <Button variant="outline" onClick={() => setStep(1)} className="gap-1">
                <ChevronLeft className="h-4 w-4" /> Back
              </Button>
              <Button disabled={!canGoToStep3} onClick={() => setStep(3)} className="gap-1.5">
                Next <ArrowRight className="h-4 w-4" />
              </Button>
            </div>
          )}

          {step === 3 && (
            <div className="flex justify-between w-full items-center">
              <Button variant="outline" onClick={() => setStep(2)} disabled={deploying} className="gap-1">
                <ChevronLeft className="h-4 w-4" /> Back
              </Button>
              <Button disabled={!canDeploy} onClick={handleDeploy} className="gap-1.5 min-w-[120px]">
                {deploying ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" /> Deploying...
                  </>
                ) : (
                  <>
                    <Globe className="h-4 w-4" /> Deploy Service
                  </>
                )}
              </Button>
            </div>
          )}

          {step === 'success' && (
            <div className="flex justify-end w-full">
              <Button variant="outline" onClick={() => setOpen(false)}>
                Done
              </Button>
            </div>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
