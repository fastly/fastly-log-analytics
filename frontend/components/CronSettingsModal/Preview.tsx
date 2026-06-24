'use client'

import React from 'react'
import { SSEProgressView } from '@/components/SSEModal'
import type { SSELine, SSEStatus } from '@/hooks/useSSE'

interface PreviewProps {
  lines: SSELine[]
  status: SSEStatus
  error: string | null
}

export function Preview({ lines, status, error }: PreviewProps) {
  return (
    <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="text-center space-y-2">
        <h3 className="text-lg font-semibold tracking-tight">Updating Cron Settings</h3>
        <p className="text-sm text-muted-foreground">Applying new background sync configuration...</p>
      </div>
      <SSEProgressView
        lines={lines}
        status={status}
        error={error}
        className="h-[300px]"
        progressLabel="Step"
        doneMessage="Settings applied! You may now close this window."
      />
    </div>
  )
}
