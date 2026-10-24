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
  title?: string
  variant?: React.ComponentProps<typeof Button>['variant']
  size?: React.ComponentProps<typeof Button>['size']
  className?: string
  timeoutMs?: number
  stopPropagation?: boolean
}

export function CopyButton({
  text,
  label,
  copiedLabel = 'Copied',
  ariaLabel,
  title = 'Copy for AI Analysis',
  variant = 'ghost',
  size = 'icon',
  className = 'h-5 w-5 text-muted-foreground hover:text-foreground hover:bg-muted ml-1 shrink-0',
  timeoutMs = 2000,
  stopPropagation = false,
}: CopyButtonProps) {
  const { copied, copy } = useCopyToClipboard(timeoutMs)

  return (
    <Button
      variant={variant}
      size={size}
      aria-label={ariaLabel}
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
            <Check className="h-3 w-3 mr-1" /> {copiedLabel}
          </>
        ) : (
          <>
            <Copy className="h-3 w-3 mr-1" /> {label}
          </>
        )
      ) : copied ? (
        <Check className={cn('h-3 w-3', ariaLabel ? 'text-emerald-500' : 'text-green-500')} />
      ) : (
        <Copy className="h-3 w-3" />
      )}
    </Button>
  )
}
