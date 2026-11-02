"use client"

import React from 'react'
import { useForm, type PathValue } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import type { WizardState } from '../types'
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
    setConfig({ ...config, [key]: val })
  }

  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
        <LoggingSetupSection config={config} update={update} />
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
      </div>
    </div>
  )
}
