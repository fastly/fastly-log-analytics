import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

export default function Loading() {
  return (
    <DashboardSkeleton
      title="Streaming"
      description="CMCD-powered video streaming quality analytics — buffer health, bitrate, throughput, and rebuffering."
    />
  )
}
