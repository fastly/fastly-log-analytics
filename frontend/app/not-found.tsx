// Next.js App Router 404 boundary. Fires when:
//   * A page calls notFound() from next/navigation
//   * The router encounters a route that doesn't resolve to any page.tsx
//
// Mounted inside the root layout so the sidebar / header / filter bar
// still render. Server component (no client interactivity needed) so
// the 404 ships as static HTML with zero JS.

import Link from 'next/link'
import { FileQuestion } from 'lucide-react'

export default function NotFound() {
  return (
    <div className="flex flex-1 items-center justify-center p-6">
      <div className="flex w-full max-w-md flex-col items-start gap-3 rounded-lg border bg-card p-6">
        <div className="flex items-center gap-2 text-muted-foreground">
          <FileQuestion className="h-5 w-5" aria-hidden="true" />
          <h2 className="text-base font-semibold text-foreground">Page not found</h2>
        </div>
        <p className="text-sm text-muted-foreground">
          The page you&apos;re looking for doesn&apos;t exist or has been moved.
        </p>
        <Link
          href="/dashboard"
          className="inline-flex items-center justify-center rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          Back to dashboard
        </Link>
      </div>
    </div>
  )
}
