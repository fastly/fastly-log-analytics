'use client'

// Next.js App Router segment-level error boundary. Catches uncaught
// render errors anywhere in the route tree below the root layout —
// the layout's <AppLayout> shell (sidebar + header + filter bar) keeps
// rendering, only the page body is replaced with the fallback.
//
// Pair with global-error.tsx (which replaces the entire <html> if the
// root layout itself throws). Without these two files, every render
// error bubbles to the single class-based ErrorBoundary wired around
// {children} in layout.tsx — that catches but loses route-segment
// isolation (the boundary stays tripped across navigations until the
// app reloads). This file resets automatically on navigation per the
// Next.js convention.

import { createRouteErrorBoundary } from '@/components/RouteErrorBoundary'

export default createRouteErrorBoundary()
