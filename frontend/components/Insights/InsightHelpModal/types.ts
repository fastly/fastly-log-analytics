import type React from 'react'

export interface InsightHelpModalProps {
  insightId: string
  isOpen: boolean
  onOpenChange: (open: boolean) => void
}

export interface InsightContent {
  title: string
  icon: React.ReactNode
  description: React.ReactNode
  diagram?: React.ReactNode
  fields: string[]
}
