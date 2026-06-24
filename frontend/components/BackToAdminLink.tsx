'use client'

import * as React from 'react'
import Link from 'next/link'
import { ArrowLeft } from 'lucide-react'

import { buttonVariants } from '@/components/ui/button'

type ButtonVariant = 'default' | 'outline' | 'secondary' | 'ghost' | 'destructive' | 'link'

/**
 * Standard "← Back to Admin" link rendered in every admin sub-page's
 * PageHeader. Five sites (session-scoring, queries, trends, share,
 * usage-log) duplicate this same Link + ArrowLeft + label shape with
 * minor variant differences.
 *
 * Variant defaults to ``outline`` (the most common choice). Other
 * callers pass ``variant="secondary"`` or ``variant="ghost"`` as
 * needed. ``prefetch`` defaults to ``true`` — the admin page is
 * already in the route cache the moment the user lands on a sub-page,
 * so a prefetch on the back link is essentially free.
 */
export function BackToAdminLink({
  variant = 'outline',
  prefetch = true,
}: {
  variant?: ButtonVariant
  prefetch?: boolean
}) {
  return (
    <Link href="/admin" prefetch={prefetch} className={buttonVariants({ variant, size: 'sm' })}>
      <ArrowLeft className="h-4 w-4 mr-1" />
      Back to Admin
    </Link>
  )
}
