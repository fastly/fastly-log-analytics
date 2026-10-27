"use client"

import React from 'react'
import { Dialog, DialogContent } from '@/components/ui/dialog'
import { cn, panelDialogContent } from '@/lib/utils'
import { type ProvisionWizardProps, type WizardState } from './types'
import { useWizardState } from './useWizardState'
import { ResumeBanner, WizardFooter, WizardHeader } from './WizardChrome'
import { AnalyzeStep, StorageStep } from './steps/StorageStep'
import { JoinStep } from './steps/JoinStep'
import {
  ConfirmStep,
  ExecuteStep,
  FeaturesStep,
  FieldsStep,
  ModeStep,
  NgwafStep,
  ServiceStep,
  SettingsStep,
  TerraformStep,
  TokenStep,
} from './steps/AdminSteps'

export * from './types'
export * from './wizardActions'
export * from './useWizardState'
export * from './WizardChrome'
export * from './steps/StorageStep'
export * from './steps/JoinStep'
export * from './steps/AdminSteps'

const STEP_COMPONENTS: Record<WizardState["step"], React.ComponentType<{ s: WizardState }>> = {
  mode: ModeStep,
  token: TokenStep,
  service: ServiceStep,
  storage: StorageStep,
  features: FeaturesStep,
  join: JoinStep,
  analyze: AnalyzeStep,
  settings: SettingsStep,
  confirm: ConfirmStep,
  ngwaf: NgwafStep,
  fields: FieldsStep,
  execute: ExecuteStep,
  terraform: TerraformStep,
}

export function ProvisionWizard({ open, onOpenChange }: ProvisionWizardProps) {
  const s = useWizardState(open, onOpenChange)
  const StepComponent = STEP_COMPONENTS[s.step]
  return (
    <Dialog open={open} onOpenChange={s.handleModalClose}>
      <DialogContent
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
          {StepComponent && <StepComponent s={s} />}
        </div>
        <WizardFooter s={s} />
      </DialogContent>
    </Dialog>
  )
}
