'use client'

import * as React from 'react'
import Link from 'next/link'
import { ArrowLeft } from 'lucide-react'

import { buttonVariants } from '@/components/ui/button'
import { useServiceStore } from '@/stores/serviceStore'

type ButtonVariant = 'default' | 'outline' | 'secondary' | 'ghost' | 'destructive' | 'link'

export function BackToAdminLink({
  variant = 'outline',
  prefetch = false,
}: {
  variant?: ButtonVariant
  prefetch?: boolean
}) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const href = activeServiceId ? `/admin?service=${activeServiceId}` : '/admin'
  return (
    <Link href={href} prefetch={prefetch} className={buttonVariants({ variant, size: 'sm' })}>
      <ArrowLeft className="h-4 w-4 mr-1" />
      Back to Admin
    </Link>
  )
}
