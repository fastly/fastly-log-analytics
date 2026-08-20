'use client'
import React from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Label } from '@/components/ui/label'
import { useBootstrap } from '@/hooks/useBootstrap'
import { client, extractApiError } from '@/lib/api'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

export function DiagnosticsPanel() {
  const queryClient = useQueryClient()

  // 1. Fetch current debug visibility settings from the backend
  const { data: settings, isLoading: isSettingsLoading } = useQuery({
    queryKey: ['admin', 'debug-settings'],
    queryFn: async () => {
      const { data, error } = await client.GET('/api/admin/debug-settings')
      if (error) throw new Error(extractApiError(error) || 'Failed to fetch debug settings')
      return data
    },
  })

  // 2. Mutation to update debug visibility settings on the backend
  const updateSettingsMutation = useMutation({
    mutationFn: async (body: { query_debug_visibility?: string; api_call_debug_visibility?: string }) => {
      const { data, error } = await client.PATCH('/api/admin/debug-settings', {
        body,
      })
      if (error) throw new Error(extractApiError(error) || 'Failed to update debug settings')
      return data
    },
    onSuccess: () => {
      // Invalidate both settings and bootstrap queries so the application re-syncs state
      queryClient.invalidateQueries({ queryKey: ['admin', 'debug-settings'] })
      queryClient.invalidateQueries({ queryKey: ['bootstrap'] })
      queryClient.invalidateQueries()
    },
  })

  const queryVisibility = settings?.query_debug_visibility ?? 'disabled'
  const apiCallVisibility = settings?.api_call_debug_visibility ?? 'disabled'
  const isUpdating = updateSettingsMutation.isPending

  const handleSetQueryVisibility = (val: string | null) => {
    if (val) updateSettingsMutation.mutate({ query_debug_visibility: val })
  }

  const handleSetApiCallsVisibility = (val: string | null) => {
    if (val) updateSettingsMutation.mutate({ api_call_debug_visibility: val })
  }

  // Backend gate check
  const { data: bootstrapData } = useBootstrap()
  const debugState = (bootstrapData as { debug_state?: { debug_responses_enabled?: boolean } } | undefined)?.debug_state
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
          <Select
            value={queryVisibility}
            onValueChange={handleSetQueryVisibility}
            disabled={!debugBackendOn || isSettingsLoading || isUpdating}
          >
            <SelectTrigger className="h-8 text-xs w-[180px]">
              <SelectValue placeholder="Select visibility..." />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="disabled" className="text-xs">Off / Disabled</SelectItem>
              <SelectItem value="admins" className="text-xs">Admins Only</SelectItem>
              <SelectItem value="analysts" className="text-xs">Analysts Only</SelectItem>
              <SelectItem value="both" className="text-xs">Both Admins & Analysts</SelectItem>
            </SelectContent>
          </Select>
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
          <Select
            value={apiCallVisibility}
            onValueChange={handleSetApiCallsVisibility}
            disabled={!debugBackendOn || isSettingsLoading || isUpdating}
          >
            <SelectTrigger className="h-8 text-xs w-[180px]">
              <SelectValue placeholder="Select visibility..." />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="disabled" className="text-xs">Off / Disabled</SelectItem>
              <SelectItem value="admins" className="text-xs">Admins Only</SelectItem>
              <SelectItem value="analysts" className="text-xs">Analysts Only</SelectItem>
              <SelectItem value="both" className="text-xs">Both Admins & Analysts</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>
    </>
  )
}
