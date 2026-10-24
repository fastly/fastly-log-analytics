'use client'

import * as React from 'react'
import { Check, Copy } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard'
import { cn } from '@/lib/utils'

export interface CopyButtonProps {
  text: string
  label?: string
  copiedLabel?: string
  ariaLabel?: string
  copiedAriaLabel?: string
  title?: string
  variant?: React.ComponentProps<typeof Button>['variant']
  size?: React.ComponentProps<typeof Button>['size']
  className?: string
  iconClassName?: string
  timeoutMs?: number
  stopPropagation?: boolean
  'data-testid'?: string
}

export function CopyButton({
  text,
  label,
  copiedLabel = 'Copied',
  ariaLabel,
  copiedAriaLabel,
  title = 'Copy for AI Analysis',
  variant = 'ghost',
  size = 'icon',
  className = 'h-5 w-5 text-muted-foreground hover:text-foreground hover:bg-muted ml-1 shrink-0',
  iconClassName,
  timeoutMs = 2000,
  stopPropagation = false,
  'data-testid': dataTestId,
}: CopyButtonProps) {
  const { copied, copy } = useCopyToClipboard(timeoutMs)
  const effectiveAriaLabel = copied && copiedAriaLabel ? copiedAriaLabel : ariaLabel

  return (
    <Button
      variant={variant}
      size={size}
      aria-label={effectiveAriaLabel}
      data-testid={dataTestId}
      className={className}
      onClick={(e) => {
        if (stopPropagation) e.stopPropagation()
        copy(text)
      }}
      title={title}
    >
      {label ? (
        copied ? (
          <>
            <Check className={cn('h-3 w-3 mr-1', iconClassName)} /> {copiedLabel}
          </>
        ) : (
          <>
            <Copy className={cn('h-3 w-3 mr-1', iconClassName)} /> {label}
          </>
        )
      ) : copied ? (
        <Check className={cn('h-3 w-3', iconClassName, ariaLabel ? 'text-emerald-500' : 'text-green-500')} />
      ) : (
        <Copy className={cn('h-3 w-3', iconClassName)} />
      )}
    </Button>
  )
}
