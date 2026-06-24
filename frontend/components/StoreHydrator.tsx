'use client'

import { useEffect } from 'react'
import { useServiceStore } from '@/stores/serviceStore'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { useDebugStore } from '@/stores/debugStore'

// Rehydrate the persisted Zustand stores AFTER mount.
//
// All three stores use persist({ skipHydration: true }), so their first
// client render uses the same default state the server rendered with
// (activeServiceId=null, services=[], timezone='UTC', debug=false).
// Without skipHydration, persist reads localStorage synchronously at
// module load, so the first client render diverges from the server (no
// localStorage there) and throws React #418 across every node that
// depends on the store — most visibly the sidebar nav `?service=` hrefs
// and the ServicesTable "Active" badge.
//
// rehydrate() runs here in an effect (client-only, after the first paint)
// so the persisted values land as an ordinary state update rather than a
// hydration mismatch.
export function StoreHydrator() {
  useEffect(() => {
    useServiceStore.persist.rehydrate()
    useDebugStore.persist.rehydrate()

    // Timezone: load the saved zone if the user picked one before;
    // otherwise adopt the browser's zone now. Doing this post-mount means
    // it never contradicts the server's deterministic 'UTC' default.
    const hadPersistedTz =
      typeof localStorage !== 'undefined' &&
      localStorage.getItem('timezone-storage') !== null
    useTimezoneStore.persist.rehydrate()
    if (!hadPersistedTz) {
      try {
        useTimezoneStore
          .getState()
          .setTimezone(Intl.DateTimeFormat().resolvedOptions().timeZone)
      } catch {
        /* keep the 'UTC' default */
      }
    }
  }, [])

  return null
}
