'use client'
import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { useBootstrap } from '@/hooks/useBootstrap'
import { useDebugStore } from '@/stores/debugStore'
import { DEBUG_RESPONSES_COOKIE } from '@/lib/debug-cookie'

export function DiagnosticsPanel() {
  const { enabled: debugEnabled, setEnabled: setDebugEnabled, apiCallsEnabled, setApiCallsEnabled } = useDebugStore()
  const queryClient = useQueryClient()

  // Mirrors the OR the backend header itself represents (x-debug-responses
  // doesn't distinguish which panel asked) — written so SSR's own fetch
  // (lib/ssr/_transport.ts) can see the toggle on the NEXT navigation/reload,
  // and immediately refetches active queries so the CURRENT page also picks
  // it up without needing one.
  function syncDebugCookie(nextEnabled: boolean, nextApiCallsEnabled: boolean) {
    const shouldAttach = nextEnabled || nextApiCallsEnabled
    document.cookie = `${DEBUG_RESPONSES_COOKIE}=${shouldAttach ? '1' : '0'}; path=/; max-age=31536000; samesite=lax`
  }

  // Invalidate (not refetchQueries({type:'active'})): data queries that are
  // momentarily enabled:false at flip time (service/filter hydration still
  // resolving) would be missed by a point-in-time refetch and then serve
  // their envelope-less cached data as fresh — same gap DebugPanel's
  // cookie-heal shim hit. Invalidation refetches active queries now AND
  // marks the rest stale so they refetch the moment they enable.
  const handleSetDebugEnabled = (val: boolean) => {
    setDebugEnabled(val)
    syncDebugCookie(val, apiCallsEnabled)
    queryClient.invalidateQueries()
  }
  const handleSetApiCallsEnabled = (val: boolean) => {
    setApiCallsEnabled(val)
    syncDebugCookie(debugEnabled, val)
    queryClient.invalidateQueries()
  }

  // Backend gate for the two "Show ... panel" toggles below. The frontend
  // panels render data from response.`_debug_queries` / `_debug_calls` —
  // when DEBUG_RESPONSES=false on the server (the prod default per the
  // 2026 security hardening) those arrays are stripped and the panel
  // shows nothing. Surface that so the toggle doesn't silently lie.
  //
  // Bootstrap folds the same flag in under ``debug_state`` so this
  // skips a dedicated /api/debug/state round-trip on every admin page
  // load. Env doesn't change without a restart, so the value is stable
  // for the session.
  const { data: bootstrapData } = useBootstrap()
  const debugState = (bootstrapData as { debug_state?: { debug_responses_enabled?: boolean } } | undefined)?.debug_state
  // Default to "enabled" on first paint so the toggle isn't briefly dimmed
  // before bootstrap resolves. Only mark disabled when we have a real
  // false from the backend.
  const debugBackendOn = debugState?.debug_responses_enabled !== false
  const debugDisabledTooltip = !debugBackendOn
    ? 'Backend debug responses are disabled — set DEBUG_RESPONSES=true in the server env (or .env file) and restart to see data here.'
    : undefined

  return (
    <>
      <div className={`flex flex-col p-3 border rounded-lg gap-3 ${!debugBackendOn ? 'opacity-60' : ''}`}>
        <div className="min-w-0 space-y-0.5">
          <Label id="diag-query-debug-label" className="text-sm font-medium">Query debugging panel</Label>
          <p className="text-xs text-muted-foreground">
            Bottom-of-screen panel with DuckDB SQL queries and execution times.
          </p>
          {!debugBackendOn && (
            <p className="text-[11px] text-amber-500" title={debugDisabledTooltip}>
              Disabled — backend ``DEBUG_RESPONSES`` env is off.
            </p>
          )}
        </div>
        <div className="flex items-center justify-end mt-auto" title={debugDisabledTooltip}>
          <Switch
            aria-labelledby="diag-query-debug-label"
            checked={debugEnabled}
            onCheckedChange={handleSetDebugEnabled}
            disabled={!debugBackendOn}
          />
        </div>
      </div>

      <div className={`flex flex-col p-3 border rounded-lg gap-3 ${!debugBackendOn ? 'opacity-60' : ''}`}>
        <div className="min-w-0 space-y-0.5">
          <Label id="diag-api-panel-label" className="text-sm font-medium">API call panel</Label>
          <p className="text-xs text-muted-foreground">
            Bottom-of-screen panel with all Fastly API calls and FOS operations per request.
          </p>
          {!debugBackendOn && (
            <p className="text-[11px] text-amber-500" title={debugDisabledTooltip}>
              Disabled — backend ``DEBUG_RESPONSES`` env is off.
            </p>
          )}
        </div>
        <div className="flex items-center justify-end mt-auto" title={debugDisabledTooltip}>
          <Switch
            aria-labelledby="diag-api-panel-label"
            checked={apiCallsEnabled}
            onCheckedChange={handleSetApiCallsEnabled}
            disabled={!debugBackendOn}
          />
        </div>
      </div>
    </>
  )
}
