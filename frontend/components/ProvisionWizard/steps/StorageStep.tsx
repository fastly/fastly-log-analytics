"use client"

import React from 'react'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import { useForm, type PathValue } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import type { ProvisionConfig, WizardState } from '../types'
import { storageStepSchema, type StorageStepFormValues } from '../wizardSchemas'
import { CdnFrontSection } from './DomainStep'
import {
  AutomationSection,
  LoggingSetupSection,
  RumVersionPicker,
  type RumVersionPickerProps,
} from './StorageSubPanels'

export { RumVersionPicker, storageStepSchema, type RumVersionPickerProps, type StorageStepFormValues }

export function StorageStep({ s }: { s: WizardState }) {
  const { config, setConfig, domainStatus, domainMessage, checkDomain } = s
  const { setValue } = useForm<StorageStepFormValues>({
    resolver: zodResolver(storageStepSchema),
    values: config,
  })
  const update = <K extends keyof StorageStepFormValues>(key: K, val: StorageStepFormValues[K]) => {
    setValue(key, val as PathValue<StorageStepFormValues, K>, { shouldDirty: true, shouldValidate: true })
    const generatedPatch: Partial<ProvisionConfig> = {}
    if (key === "fos_bucket_name") generatedPatch.fos_bucket_generated = false
    if (key === "cdn_prefix") generatedPatch.cdn_domain_generated = false
    setConfig({ ...config, [key]: val, ...generatedPatch })
  }
  const updateMany = (patch: Partial<StorageStepFormValues>) => {
    for (const [k, v] of Object.entries(patch) as [
      keyof StorageStepFormValues,
      StorageStepFormValues[keyof StorageStepFormValues],
    ][]) {
      setValue(k, v as PathValue<StorageStepFormValues, typeof k>, { shouldDirty: true, shouldValidate: true })
    }
    const generatedPatch: Partial<ProvisionConfig> = {}
    if ("fos_bucket_name" in patch) generatedPatch.fos_bucket_generated = false
    if ("cdn_prefix" in patch) generatedPatch.cdn_domain_generated = false
    setConfig({ ...config, ...patch, ...generatedPatch })
  }

  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
        <LoggingSetupSection config={config} update={update} updateMany={updateMany} />
        {!isCloudMode() && (
          <>
            <CdnFrontSection
              cdnPrefix={config.cdn_prefix}
              cdnShield={config.cdn_shield}
              domainStatus={domainStatus}
              domainMessage={domainMessage}
              onChangePrefix={(val) => update("cdn_prefix", val)}
              onChangeShield={(val) => update("cdn_shield", val)}
              onCheckDomain={checkDomain}
            />
            <AutomationSection
              config={config}
              selectedServiceId={s.selectedService?.id}
              update={update}
            />
          </>
        )}
      </div>
    </div>
  )
}
