'use client'

import dynamic from 'next/dynamic'
import { Skeleton } from '@/components/ui/skeleton'

export const DeferredSystemHealthCard = dynamic(
  () => import('@/components/SystemHealthCard').then((m) => m.SystemHealthCard),
  { ssr: false, loading: () => <Skeleton className="h-[280px] w-full rounded-xl" /> },
)

export const DeferredQuarantineSection = dynamic(
  () => import('@/app/control-room/_sections/QuarantineSection').then((m) => m.QuarantineSection),
  { ssr: false, loading: () => null },
)

export const DeferredGlobalSettings = dynamic(
  () => import('./AdminGlobalSettings').then((m) => m.GlobalSettings),
  { ssr: false, loading: () => <Skeleton className="h-[240px] w-full rounded-xl" /> },
)

export const DeferredPricingSettings = dynamic(
  () => import('./AdminGlobalSettings').then((m) => m.PricingSettings),
  { ssr: false, loading: () => <Skeleton className="h-[220px] w-full rounded-xl" /> },
)
