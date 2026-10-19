import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

export default function Loading() {
  return (
    <DashboardSkeleton
      title="Assets & Shield"
      description="Static asset delivery analysis, payload compression rates, and caching performance analyzer."
    />
  )
}
