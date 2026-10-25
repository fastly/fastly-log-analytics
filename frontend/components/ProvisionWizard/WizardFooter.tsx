"use client";

import React from "react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { CheckCircle2, ChevronLeft, Loader2, Plus, RotateCcw } from "lucide-react";
import { ELAPSED_RELATIVE_AGO, formatElapsed } from "@/lib/format";
import { cn, panelDialogFooter } from "@/lib/utils";
import type { WizardState } from "./useWizardState";
import { getStepsForMode, type Step, type WizardDraft } from "./types";

// ─── ResumeBanner ───────────────────────────────────────────────────────────

function relativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "earlier";
  return formatElapsed(Math.floor((Date.now() - then) / 1000), ELAPSED_RELATIVE_AGO);
}

function stepLabelFor(draft: WizardDraft): string {
  const steps = getStepsForMode(draft.mode, draft.config);
  const found = steps.find((s) => s.id === draft.currentStep);
  return found?.label ?? draft.currentStep;
}

interface ResumeBannerProps {
  draft: WizardDraft;
  onResume: () => void;
  onStartFresh: () => void;
}

export function ResumeBanner({ draft, onResume, onStartFresh }: ResumeBannerProps) {
  const label = stepLabelFor(draft);
  const when = relativeTime(draft.updatedAt);
  return (
    <div className="px-6 pt-4">
      <Alert>
        <RotateCcw className="h-4 w-4" />
        <AlertTitle>Resume previous wizard?</AlertTitle>
        <AlertDescription>
          You left off on the <span className="font-medium">{label}</span> step
          {" "}({when}). Fastly API token and storage credentials were cleared
          for safety and will need to be re-entered.
        </AlertDescription>
        <div className="mt-3 flex flex-wrap gap-2 group-has-[>svg]/alert:col-start-2">
          <Button size="sm" onClick={onResume} data-testid="resume-banner-resume">
            Resume from {label}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={onStartFresh}
            data-testid="resume-banner-start-fresh"
          >
            Start fresh
          </Button>
        </div>
      </Alert>
    </div>
  );
}

// ─── WizardHeader ───────────────────────────────────────────────────────────

export function WizardHeader({ s }: { s: WizardState }) {
  const { step, STEPS } = s;
  const stepIndex = STEPS.findIndex((x) => x.id === step);

  return (
    <DialogHeader className="px-6 pt-6 pb-7 border-b">
      <DialogTitle className="flex items-center gap-2 text-xl font-bold">
        <Plus className="h-5 w-5 text-primary" />
        Provision New Service
      </DialogTitle>

      {/* Premium Stepper Timeline */}
      <div className="flex items-center justify-center w-full mt-6 px-1 sm:px-4 max-w-2xl mx-auto">
        {STEPS.map((s2, i) => {
          const isActive = step === s2.id;
          const isCompleted = stepIndex > i;

          return (
            <React.Fragment key={s2.id}>
              {/* Step Node */}
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
                  {isCompleted ? (
                    <CheckCircle2 className="w-4 h-4" />
                  ) : (
                    i + 1
                  )}
                </div>

                {/* Responsive Label - hidden on small screens unless active to save horizontal space */}
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

              {/* Connecting Line Segment */}
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
          );
        })}
      </div>

      {/* Dynamic step name details line */}
      <div className="text-center mt-7 text-xs text-muted-foreground font-medium">
        Step <span className="text-foreground font-semibold">{stepIndex + 1}</span> of {STEPS.length}
      </div>
    </DialogHeader>
  );
}

// ─── WizardFooter ───────────────────────────────────────────────────────────

export function WizardFooter({ s }: { s: WizardState }) {
  const {
    step,
    setStep,
    mode,
    isDeploying,
    status,
    isDone,
    handleModalClose,
    isAnalyzing,
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
    handleAnalyzeLake,
    importMode,
    importRange,
    handleJoin,
    joinPhase,
    stop,
  } = s;

  return (
    <DialogFooter className={panelDialogFooter}>
      {!isDeploying && step !== "mode" && step !== "terraform" && (
        <Button
          variant="ghost"
          className="mr-auto h-9 text-xs"
          disabled={isAnalyzing}
          onClick={() => {
            const getStepsOrder = (): Step[] => {
              if (mode === "join") {
                return ["mode", "join", "analyze", "settings", "confirm"];
              }
              if (
                mode === "ingest" &&
                (step === "join" ||
                  step === "analyze" ||
                  step === "settings" ||
                  step === "confirm")
              ) {
                return ["mode", "token", "service", "join", "analyze", "settings", "confirm"];
              }
              const steps: Step[] = ["mode", "token", "service", "features", "storage"];
              if (config.logging_enabled) {
                steps.push("ngwaf", "fields");
              }
              steps.push("execute", "terraform");
              return steps;
            };
            const order = getStepsOrder();
            const idx = order.indexOf(step);
            if (idx > 0) setStep(order[idx - 1] as Step);
          }}
        >
          <ChevronLeft className="h-4 w-4 mr-1" /> Back
        </Button>
      )}

      {status !== "streaming" && (
        <Button
          variant="outline"
          className="h-9 text-xs"
          onClick={() => handleModalClose(false)}
        >
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
            <Button
              disabled={!token || isLoadingServices}
              onClick={handleTokenSubmit}
              className="h-9 text-xs"
            >
              {isLoadingServices && (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              )}
              Fetch Services
            </Button>
          )}
          {step === "service" && (
            <Button
              disabled={!selectedService || validateMutation.isPending}
              onClick={() => {
                if (selectedService) {
                  validateMutation.mutate(selectedService.id);
                }
              }}
              className="h-9 text-xs"
            >
              {validateMutation.isPending && (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              )}
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
            <Button
              onClick={() => setStep("fields")}
              className="h-9 text-xs"
            >
              {config.ngwaf_workspace_id ? "Continue" : "Skip"}
            </Button>
          )}
          {step === "fields" && (
            <Button
              onClick={() => setStep("execute")}
              className="h-9 text-xs"
            >
              Review Settings
            </Button>
          )}

          {step === "execute" && (
            <div className="flex gap-2">
              <Button
                variant="secondary"
                className="h-9 font-bold"
                onClick={() => {
                  fetchTerraformPreview();
                  setStep("terraform");
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
            <Button
              size="lg"
              className="h-9 px-6 font-bold"
              onClick={() => setStep("execute")}
            >
              Back to Review
            </Button>
          )}

          {step === "join" && joinPhase === "form" && (
            <Button
              size="lg"
              disabled={
                !config.endpoint_name ||
                !config.cdn_service_name ||
                fosStatus !== "success" ||
                isAnalyzing
              }
              className="h-9 px-6 font-bold"
              onClick={handleAnalyzeLake}
            >
              {isAnalyzing && (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              )}
              Analyze Data Lake
            </Button>
          )}

          {step === "analyze" && (
            <Button
              className="h-9 text-xs"
              onClick={() => setStep("settings")}
            >
              Continue
            </Button>
          )}

          {step === "settings" && (
            <Button
              className="h-9 text-xs"
              onClick={() => setStep("confirm")}
            >
              Review Summary
            </Button>
          )}

          {step === "confirm" && (
            <Button
              size="lg"
              className="h-9 px-6 font-bold"
              onClick={handleJoin}
              disabled={
                !s.analystPathASupported ||
                (importMode === "range" &&
                  (!importRange.start || !importRange.end))
              }
            >
              Confirm & Connect
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
  );
}
