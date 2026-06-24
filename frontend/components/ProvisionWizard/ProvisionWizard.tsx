"use client";

import React from "react";
import {
  Dialog,
  DialogContent,
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";
import { panelDialogContent } from "@/lib/panel-dialog";
import type { ProvisionWizardProps } from "./types";
import { useWizardState } from "./useWizardState";
import { WizardHeader } from "./WizardHeader";
import { WizardFooter } from "./WizardFooter";
import { ModeStep } from "./steps/ModeStep";
import { TokenStep } from "./steps/TokenStep";
import { ServiceStep } from "./steps/ServiceStep";
import { StorageStep } from "./steps/StorageStep";
import { JoinStep } from "./steps/JoinStep";
import { AnalyzeStep } from "./steps/AnalyzeStep";
import { SettingsStep } from "./steps/SettingsStep";
import { ConfirmStep } from "./steps/ConfirmStep";
import { NgwafStep } from "./steps/NgwafStep";
import { FieldsStep } from "./steps/FieldsStep";
import { ExecuteStep } from "./steps/ExecuteStep";
import { TerraformStep } from "./steps/TerraformStep";
import { ResumeBanner } from "./ResumeBanner";

export function ProvisionWizard({ open, onOpenChange }: ProvisionWizardProps) {
  const s = useWizardState(open, onOpenChange);

  return (
    <Dialog open={open} onOpenChange={s.handleModalClose}>
      <DialogContent
        // Pin a DEFINITE height (not just panelDialogContent's max-h-[90vh]) so
        // the flex-1/min-h-0/overflow chain inside each step actually creates a
        // scroll boundary: the Terraform/VCL preview's snippet pane scrolls
        // instead of growing the modal, and the modal stays a constant size
        // across steps instead of jumping as each step's content height changes.
        className={cn("sm:max-w-5xl h-[85vh]", panelDialogContent)}
        showCloseButton={s.status !== "streaming" && s.joinPhase !== "done"}
      >
        {s.pendingDraft && s.mode === null && (
          <ResumeBanner
            draft={s.pendingDraft}
            onResume={s.resumeDraft}
            onStartFresh={s.discardDraft}
          />
        )}
        <WizardHeader s={s} />

        <div className="flex-1 overflow-hidden flex flex-col">
          {s.step === "mode" && <ModeStep s={s} />}
          {s.step === "token" && <TokenStep s={s} />}
          {s.step === "service" && <ServiceStep s={s} />}
          {s.step === "storage" && <StorageStep s={s} />}
          {s.step === "join" && <JoinStep s={s} />}
          {s.step === "analyze" && <AnalyzeStep s={s} />}
          {s.step === "settings" && <SettingsStep s={s} />}
          {s.step === "confirm" && <ConfirmStep s={s} />}
          {s.step === "ngwaf" && <NgwafStep s={s} />}
          {s.step === "fields" && <FieldsStep s={s} />}
          {s.step === "execute" && <ExecuteStep s={s} />}
          {s.step === "terraform" && <TerraformStep s={s} />}
        </div>

        <WizardFooter s={s} />
      </DialogContent>
    </Dialog>
  );
}
