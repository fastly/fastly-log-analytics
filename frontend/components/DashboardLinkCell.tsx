'use client'

import Link from 'next/link'
import { ExternalLink } from 'lucide-react'
import { cn } from '@/lib/utils'

interface DashboardLinkCellProps {
  value: string | null | undefined
  href: string
  className?: string
  containerClassName?: string
}

export function DashboardLinkCell({
  value,
  href,
  className,
  containerClassName,
}: DashboardLinkCellProps) {
  return (
    <div className={cn('flex items-center gap-2 group', containerClassName)}>
      <span className={cn('truncate block', className)}>{value}</span>
      {value != null && (
        <Link
          href={href}
          className="opacity-0 group-hover:opacity-100 transition-opacity shrink-0"
          title="View in Dashboard"
          target="_blank"
          rel="noopener noreferrer"
        >
          <ExternalLink className="h-3 w-3 text-muted-foreground hover:text-primary" />
        </Link>
      )}
    </div>
  )
}
