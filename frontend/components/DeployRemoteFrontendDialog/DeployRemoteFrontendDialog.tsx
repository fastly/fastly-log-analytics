'use client'

import * as React from 'react'
import { useForm, useWatch } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation } from '@tanstack/react-query'
import {
  Globe, AlertCircle, ArrowRight, Loader2,
  ChevronRight, ChevronLeft,
} from 'lucide-react'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { client, extractApiError } from '@/lib/api'
import { cn, panelDialogContent, panelDialogHeaderSolid, panelDialogFooter } from '@/lib/utils'
import { useOpenGeneration } from '@/hooks/useSSE'
import type { Schema } from '@/types/api'
import { useSearchParams } from 'next/navigation'
import {
  domainStepSchema,
  originStepSchema,
  deployRemoteFrontendSchema,
  type DeployRemoteFrontendValues,
  DeployStepDomain,
  DeployStepOrigin,
  DeployStepSuccess,
  TeardownRemoteFrontendDialog,
} from './DeployStepsAndTeardown'

export { TeardownRemoteFrontendDialog }

export function DeployRemoteFrontendDialog() {
  const [open, setOpen] = React.useState(false)
  const generation = useOpenGeneration(open)

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
      <DeployRemoteFrontendDialogBody key={generation} onClose={() => setOpen(false)} />
    </Dialog>
  )
}

function DeployRemoteFrontendDialogBody({ onClose }: { onClose: () => void }) {
  const searchParams = useSearchParams()
  const serviceId = searchParams?.get('service')

  const [step, setStep] = React.useState<1 | 2 | 3 | 'success'>(1)
  const [domainCheckResult, setDomainCheckResult] = React.useState<Schema<'ProvisionCheckDomainResponse'> | null>(null)
  const [deployError, setDeployError] = React.useState('')
  const [deployResult, setDeployResult] = React.useState<Schema<'SharingDeployResponse'> | null>(null)

  const { control, setValue } = useForm<DeployRemoteFrontendValues>({
    resolver: zodResolver(deployRemoteFrontendSchema),
    defaultValues: {
      isCustom: false,
      prefix: '',
      originHost: typeof window !== 'undefined' ? window.location.hostname : '',
      originPort: typeof window !== 'undefined' && window.location.protocol === 'https:' ? 443 : 80,
      useSsl: typeof window !== 'undefined' && window.location.protocol === 'https:',
      overrideHost: '',
      serviceNameDraft: '',
      isServiceNameEdited: false,
      tokenOverride: '',
    },
  })
  const {
    isCustom, prefix, originHost, originPort, useSsl, overrideHost,
    serviceNameDraft, isServiceNameEdited, tokenOverride,
  } = useWatch({ control }) as DeployRemoteFrontendValues

  const computedDomain = React.useMemo(() => {
    if (!prefix) return ''
    return isCustom ? prefix : `${prefix}.global.ssl.fastly.net`
  }, [prefix, isCustom])

  const serviceName = isServiceNameEdited
    ? serviceNameDraft
    : computedDomain
      ? `Fastly Log Analytics Remote Frontend - ${computedDomain}`
      : ''

  const checkDomainMutation = useMutation({
    mutationFn: async () => {
      const { data, error } = await client.GET('/api/provision/check-domain', {
        params: { query: { prefix, is_custom: isCustom } },
      })
      if (error) throw new Error(extractApiError(error))
      return {
        available: !!data?.available,
        reason: data?.reason || null,
        note: data?.note || null,
      }
    },
    onMutate: () => setDomainCheckResult(null),
    onSuccess: (result) => setDomainCheckResult(result),
    onError: (err) => {
      const errorMsg = err instanceof Error ? err.message : String(err)
      setDomainCheckResult({
        available: false,
        reason: errorMsg || 'Failed to verify domain status. Please try again.',
      })
    },
  })
  const checkingDomain = checkDomainMutation.isPending
  const handleCheckDomain = () => {
    if (!domainStepSchema.safeParse({ isCustom, prefix }).success) return
    checkDomainMutation.mutate()
  }

  const deployMutation = useMutation({
    mutationFn: async () => {
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
      if (error) throw new Error(extractApiError(error))
      return data
    },
    onMutate: () => setDeployError(''),
    onSuccess: (data) => {
      setDeployResult(data ?? null)
      setStep('success')
    },
    onError: (err) => setDeployError(extractApiError(err)),
  })
  const deploying = deployMutation.isPending
  const handleDeploy = () => deployMutation.mutate()

  const canGoToStep2 = domainCheckResult?.available === true && !checkingDomain && domainStepSchema.safeParse({ isCustom, prefix }).success
  const canGoToStep3 = originStepSchema.safeParse({ originHost, originPort, useSsl, overrideHost }).success
  const canDeploy = !deploying && !!serviceName && canGoToStep3 && canGoToStep2

  return (
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
          {step === 1 && (
            <DeployStepDomain
              isCustom={isCustom}
              prefix={prefix}
              setValue={setValue}
              checkingDomain={checkingDomain}
              domainCheckResult={domainCheckResult}
              setDomainCheckResult={setDomainCheckResult}
              onCheckDomain={handleCheckDomain}
            />
          )}

          {step === 2 && (
            <DeployStepOrigin
              originHost={originHost}
              originPort={originPort}
              useSsl={useSsl}
              overrideHost={overrideHost}
              setValue={setValue}
            />
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
                <Label htmlFor="service-name" className="text-xs font-semibold">Fastly Service Name</Label>
                <Input
                  id="service-name"
                  value={serviceName}
                  onChange={(e) => {
                    setValue('serviceNameDraft', e.target.value)
                    setValue('isServiceNameEdited', true)
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
                      onChange={(e) => setValue('tokenOverride', e.target.value)}
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

          {step === 'success' && deployResult && <DeployStepSuccess deployResult={deployResult} />}
        </div>

        {/* Dialog Footer */}
        <DialogFooter className={panelDialogFooter}>
          {step === 1 && (
            <div className="flex justify-between w-full items-center">
              <Button variant="outline" onClick={onClose}>Close</Button>
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
                  <><Loader2 className="h-4 w-4 animate-spin" /> Deploying...</>
                ) : (
                  <><Globe className="h-4 w-4" /> Deploy Service</>
                )}
              </Button>
            </div>
          )}

          {step === 'success' && (
            <div className="flex justify-end w-full">
              <Button variant="outline" onClick={onClose}>Done</Button>
            </div>
          )}
        </DialogFooter>
      </DialogContent>
  )
}
