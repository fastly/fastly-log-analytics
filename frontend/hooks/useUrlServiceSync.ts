'use client'

import { startTransition, useEffect, useRef } from 'react'
import { useSearchParams, useRouter, usePathname } from 'next/navigation'
import { useServiceStore } from '@/stores/serviceStore'

export function useUrlServiceSync() {
  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const services = useServiceStore(state => state.services)
  const isInitialized = useServiceStore(state => state.isInitialized)
  const setActiveServiceId = useServiceStore(state => state.setActiveServiceId)
  const searchParams = useSearchParams()
  const router = useRouter()
  const pathname = usePathname()
  const isInitialMount = useRef(true)

  // 1. Sync FROM URL to Store on mount
  useEffect(() => {
    const urlServiceId = searchParams.get('service')
    if (urlServiceId && urlServiceId !== activeServiceId) {
      setActiveServiceId(urlServiceId)
    }
    isInitialMount.current = false
  }, []) // Only on mount

  // 2. Sync FROM Store to URL when activeServiceId changes
  useEffect(() => {
    if (isInitialMount.current || !isInitialized) return

    const currentServiceId = searchParams.get('service')
    
    // If there are no services, we should never have a service ID in the URL
    const targetServiceId = services.length > 0 ? activeServiceId : null

    if (targetServiceId !== currentServiceId) {
      const newUrl = targetServiceId ? `${pathname}?service=${targetServiceId}` : pathname
      // Mark the URL-sync as a non-urgent transition so React paints
      // the current render (often a loading.tsx skeleton triggered by
      // the nav click) BEFORE processing the replace's re-render
      // cascade. Without startTransition every service-id change in
      // the store causes a synchronous router update that competes
      // with the page's own mount work for the main thread.
      startTransition(() => {
        router.replace(newUrl)
      })
    }
  }, [activeServiceId, services, isInitialized, pathname, router, searchParams])
}
