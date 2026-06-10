'use client'

import React from 'react'
import { CustomFieldsManager } from '@/components/CustomFields/CustomFieldsManager'

interface CustomFieldsStepProps {
  serviceId: string
}

export function CustomFieldsStep({ serviceId }: CustomFieldsStepProps) {
  return (
    <div className="m-0 border-none p-0 outline-none">
      <CustomFieldsManager serviceId={serviceId} />
    </div>
  )
}
