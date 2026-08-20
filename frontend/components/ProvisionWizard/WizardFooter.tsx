"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import { DialogFooter } from "@/components/ui/dialog";
import { ChevronLeft, Loader2 } from "lucide-react";
import { panelDialogFooter } from "@/lib/panel-dialog";
import type { WizardState } from "./useWizardState";
import type { Step } from "./types";

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
              onClick={() => {}}
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
            // The Terraform & VCL preview is a read-only side-trip: export the
            // files (button lives in the step body) and return. Deploy to
            // Fastly / Complete Setup stay on the Review (execute) step so
            // there's a single, unambiguous place to commit.
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
                importMode === "range" &&
                (!importRange.start || !importRange.end)
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
