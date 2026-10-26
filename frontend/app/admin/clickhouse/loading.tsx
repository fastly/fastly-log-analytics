import { notFound } from 'next/navigation'
import { ClickHousePanelFallback, RouteSkeleton } from '@/components/ui/skeleton'

export { ClickHousePanelFallback }

export default function Loading() {
  const rawMode = (process.env.DEPLOYMENT_MODE || '').trim().toLowerCase()
  const isHighScale =
    rawMode === 'standard'
      ? false
      : rawMode === 'high_scale' ||
        ['1', 'true', 'yes', 'on'].includes((process.env.HIGH_SCALE_ENABLED || '').trim().toLowerCase())
  if (!isHighScale) notFound()
  return <RouteSkeleton variant="admin-clickhouse" />
}
