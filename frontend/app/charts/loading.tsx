import { ChartsGridSkeleton } from '@/components/skeletons/PageSkeleton'

// title/description mirror the ReportShell props in page.tsx so the
// instant route skeleton shows the real header before the bundle loads.
export default function Loading() {
  return (
    <ChartsGridSkeleton
      title="Distribution Charts"
      description="Visualizing the Top 10 distributions for key log fields."
    />
  )
}
