import { DashboardSkeleton } from '@/components/ReportLayout'

export default function Loading() {
  return (
    <DashboardSkeleton
      title="Streaming"
      description="CMCD-powered video streaming quality analytics — buffer health, bitrate, throughput, and rebuffering."
    />
  )
}
