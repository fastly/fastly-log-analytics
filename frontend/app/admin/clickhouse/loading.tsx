import { ClickHousePanelFallback, RouteSkeleton } from '@/components/ui/skeleton'

export const dynamic = 'force-dynamic'

export { ClickHousePanelFallback }

export default function Loading() {
  return <RouteSkeleton variant="admin-clickhouse" />
}
