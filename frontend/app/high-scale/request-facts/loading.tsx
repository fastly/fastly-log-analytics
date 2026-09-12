import { Skeleton } from '@/components/ui/skeleton'

export default function Loading() {
  return (
    <main className="container mx-auto max-w-7xl px-4 py-6">
      <Skeleton className="mb-6 h-12 w-80" />
      <Skeleton className="h-80 w-full" />
    </main>
  )
}
