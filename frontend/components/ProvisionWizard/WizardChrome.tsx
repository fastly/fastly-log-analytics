"use client"

import React from 'react'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { CheckCircle2, ChevronLeft, Loader2, Plus, RotateCcw, XCircle } from 'lucide-react'
import { cn, ELAPSED_RELATIVE_AGO, formatElapsed, panelDialogFooter } from '@/lib/utils'
import { getStepsForMode, type Step, type WizardDraft, type WizardState } from './types'

function relativeTime(iso: string): string {
  const then = new Date(iso).getTime()
  return Number.isNaN(then)
    ? "earlier"
    : formatElapsed(Math.floor((Date.now() - then) / 1000), ELAPSED_RELATIVE_AGO)
}

export function ResumeBanner({
  draft,
  onResume,
  onStartFresh,
}: {
  draft: WizardDraft
  onResume: () => void
  onStartFresh: () => void
}) {
  const label =
    getStepsForMode(draft.mode, draft.config).find((s) => s.id === draft.currentStep)?.label ??
    draft.currentStep
  const when = relativeTime(draft.updatedAt)
  return (
    <div className="px-6 pt-4">
      <Alert>
        <RotateCcw className="h-4 w-4" />
        <AlertTitle>Resume previous wizard?</AlertTitle>
        <AlertDescription>
          You left off on the <span className="font-medium">{label}</span> step ({when}). Fastly API
          token and storage credentials were cleared for safety and will need to be re-entered.
        </AlertDescription>
        <div className="mt-3 flex flex-wrap gap-2 group-has-[>svg]/alert:col-start-2">
          <Button size="sm" onClick={onResume} data-testid="resume-banner-resume">
            Resume from {label}
          </Button>
          <Button size="sm" variant="ghost" onClick={onStartFresh} data-testid="resume-banner-start-fresh">
            Start fresh
          </Button>
        </div>
      </Alert>
    </div>
  )
}

export function WizardHeader({ s }: { s: WizardState }) {
  const { step, STEPS } = s
  const stepIndex = STEPS.findIndex((x) => x.id === step)
  return (
    <DialogHeader className="px-6 pt-6 pb-7 border-b">
      <DialogTitle className="flex items-center gap-2 text-xl font-bold">
        <Plus className="h-5 w-5 text-primary" />
        Provision New Service
      </DialogTitle>
      <div className="flex items-center justify-center w-full mt-6 px-1 sm:px-4 max-w-2xl mx-auto">
        {STEPS.map((s2, i) => {
          const isActive = step === s2.id
          const isCompleted = stepIndex > i
          return (
            <React.Fragment key={s2.id}>
              <div className="flex flex-col items-center shrink-0 relative">
                <div
                  className={cn(
                    "w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold transition-all duration-300 border-2",
                    isActive
                      ? "bg-primary border-primary text-primary-foreground shadow-md shadow-primary/20 scale-110"
                      : isCompleted
                        ? "bg-green-500 border-green-500 text-white"
                        : "bg-background border-muted text-muted-foreground",
                  )}
                  title={s2.label}
                >
                  {isCompleted ? <CheckCircle2 className="w-4 h-4" /> : i + 1}
                </div>
                <span
                  className={cn(
                    "text-[10px] font-semibold mt-1.5 absolute top-7 whitespace-nowrap transition-all duration-300",
                    isActive
                      ? "text-foreground font-bold opacity-100 scale-100"
                      : "text-muted-foreground opacity-60 hidden md:block",
                  )}
                >
                  {s2.label}
                </span>
              </div>
              {i < STEPS.length - 1 && (
                <div className="flex-1 min-w-[8px] max-w-[48px] h-[2px] mx-1 sm:mx-2 bg-muted relative">
                  <div
                    className={cn(
                      "absolute inset-0 bg-primary transition-all duration-500 ease-in-out",
                      stepIndex > i ? "w-full" : "w-0",
                    )}
                  />
                </div>
              )}
            </React.Fragment>
          )
        })}
      </div>
      <div className="text-center mt-7 text-xs text-muted-foreground font-medium">
        Step <span className="text-foreground font-semibold">{stepIndex + 1}</span> of {STEPS.length}
      </div>
    </DialogHeader>
  )
}

export function WizardFooter({ s }: { s: WizardState }) {
  const {
    step,
    setStep,
    mode,
    isDeploying,
    status,
    isDone,
    handleModalClose,
    token,
    isLoadingServices,
    handleTokenSubmit,
    selectedService,
    validateMutation,
    domainStatus,
    config,
    fetchTerraformPreview,
    handleDeploy,
    handleAdminIngest,
    fosStatus,
    setupPhase,
    stop,
  } = s

  const handleBack = () => {
    const baseSteps = getStepsForMode(mode, config).map((x) => x.id)
    const order = baseSteps.includes("execute") ? [...baseSteps, "terraform" as Step] : baseSteps
    const idx = order.indexOf(step)
    if (idx > 0) setStep(order[idx - 1])
  }

  return (
    <DialogFooter className={panelDialogFooter}>
      {!isDeploying && step !== "mode" && step !== "terraform" && !(step === "connect" && setupPhase === "done") && (
        <Button variant="ghost" className="mr-auto h-9 text-xs" onClick={handleBack}>
          <ChevronLeft className="h-4 w-4 mr-1" /> Back
        </Button>
      )}
      {status !== "streaming" && (
        <Button variant="outline" className="h-9 text-xs" onClick={() => handleModalClose(false)}>
          {status === "done" || isDone ? "Close & Reload" : "Cancel"}
        </Button>
      )}
      {!isDeploying && (
        <>
          {step === "mode" && (
            <Button disabled className="h-9 text-xs">
              Select a Role
            </Button>
          )}
          {step === "token" && (
            <Button disabled={!token || isLoadingServices} onClick={handleTokenSubmit} className="h-9 text-xs">
              {isLoadingServices && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Fetch Services
            </Button>
          )}
          {step === "service" && (
            <Button
              disabled={!selectedService || validateMutation.isPending}
              onClick={() => selectedService && validateMutation.mutate(selectedService.id)}
              className="h-9 text-xs"
            >
              {validateMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Continue
            </Button>
          )}
          {step === "features" && (
            <Button
              onClick={() => setStep("storage")}
              disabled={!config.logging_enabled && !config.rum_enabled}
              className="h-9 text-xs"
            >
              Continue
            </Button>
          )}
          {step === "storage" && (
            <Button
              onClick={() => setStep(config.logging_enabled ? "ngwaf" : "execute")}
              disabled={domainStatus === "taken" || domainStatus === "checking"}
              className="h-9 text-xs"
            >
              Continue
            </Button>
          )}
          {step === "ngwaf" && (
            <Button onClick={() => setStep("fields")} className="h-9 text-xs">
              {config.ngwaf_workspace_id ? "Continue" : "Skip"}
            </Button>
          )}
          {step === "fields" && (
            <Button onClick={() => setStep("execute")} className="h-9 text-xs">
              Review Settings
            </Button>
          )}
          {step === "execute" && (
            <div className="flex gap-2">
              <Button
                variant="secondary"
                className="h-9 font-bold"
                onClick={() => {
                  fetchTerraformPreview()
                  setStep("terraform")
                }}
              >
                View & Export Terraform
              </Button>
              {mode !== "ingest" ? (
                <Button
                  size="lg"
                  disabled={domainStatus === "taken" || !config.fos_bucket_name}
                  className="h-9 px-6 font-bold"
                  onClick={handleDeploy}
                >
                  Deploy to Fastly
                </Button>
              ) : (
                <Button
                  size="lg"
                  disabled={!config.fos_bucket_name}
                  className="h-9 px-6 font-bold"
                  onClick={handleAdminIngest}
                >
                  Complete Setup
                </Button>
              )}
            </div>
          )}
          {step === "terraform" && (
            <Button size="lg" className="h-9 px-6 font-bold" onClick={() => setStep("execute")}>
              Back to Review
            </Button>
          )}
          {step === "connect" && setupPhase === "form" && (
            <Button
              size="lg"
              disabled={!config.endpoint_name || !config.cdn_service_name || fosStatus !== "success"}
              className="h-9 px-6 font-bold"
              onClick={() => setStep("features")}
            >
              Continue
            </Button>
          )}
        </>
      )}
      {status === "streaming" && (
        <Button variant="outline" onClick={stop} className="h-9 text-xs">
          Stop
        </Button>
      )}
    </DialogFooter>
  )
}

export function StatusCheckIcon({ enabled }: { enabled: boolean }) {
  return enabled ? (
    <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
  ) : (
    <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
  )
}

export function FeatureSummaryBadge({
  icon: Icon,
  title,
  subtitle,
}: {
  icon: React.ComponentType<{ className?: string }>
  title: string
  subtitle: string
}) {
  return (
    <div className="flex-1 flex items-center gap-3 border rounded-xl p-3 bg-primary/[0.03] border-primary/10 shadow-sm">
      <div className="h-8 w-8 rounded-lg bg-primary/10 flex items-center justify-center text-primary shrink-0">
        <Icon className="h-4 w-4" />
      </div>
      <div className="flex flex-col min-w-0">
        <span className="text-xs font-semibold text-foreground">{title}</span>
        <span className="text-[10px] text-muted-foreground leading-snug">{subtitle}</span>
      </div>
    </div>
  )
}
