'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import type { components } from '@/types/api.generated'
import {
  RefreshCw,
} from 'lucide-react'
import { Button } from "@/components/ui/button"
import { ColumnVisibilityDropdown } from '@/components/DataTable/ColumnVisibilityDropdown'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
// Direct import (not via the barrel) so the page bundle drops the
// @dnd-kit tree that the reorder-enabled DataTable pulls in. The
// dropdown column picker above already covers hide/show.
import { DataTableReadonly as DataTable } from '@/components/DataTable'
import { ColumnDef } from '@tanstack/react-table'
import { queryKeys } from '@/lib/query-keys'

export function ServiceHistoryTab({
  auditColumns,
  auditLogs,
  isLoadingAudit,
  isFetchingAudit,
  eventFilter,
  setEventFilter,
  activeServiceId,
}: {
  auditColumns: ColumnDef<components['schemas']['AuditLogEntry']>[]
  auditLogs: components['schemas']['AuditLogsResponse'] | undefined
  isLoadingAudit: boolean
  isFetchingAudit: boolean
  eventFilter: string
  setEventFilter: (v: string) => void
  activeServiceId: string | null | undefined
}) {
  const queryClient = useQueryClient()

  return (
    <div className="p-0">
      <DataTable
        columns={auditColumns}
        data={auditLogs?.entries || []}
        isLoading={isLoadingAudit}
        initialSorting={[{ id: 'timestamp', desc: true }]}
        renderToolbar={(table) => (
          <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 border-b gap-4">
            <div className="flex items-center gap-4">
              <h3 className="text-sm font-medium whitespace-nowrap">Service History</h3>
              <div className="flex items-center gap-2">
                <Select value={eventFilter} onValueChange={(v) => setEventFilter(v || 'all')}>
                  <SelectTrigger className="h-8 w-[200px] text-xs">
                    <SelectValue placeholder="All events" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All events</SelectItem>
                    <SelectItem value="provision">Provision</SelectItem>
                    <SelectItem value="fastly_activation">Fastly Activation</SelectItem>
                    <SelectItem value="cron_settings_update">Cron Settings Update</SelectItem>
                    <SelectItem value="logging_settings_update">Log Settings Updated</SelectItem>
                    <SelectItem value="log_format_update">Log Format Update</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown table={table} />
              <Button
                variant="outline"
                size="sm"
                onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.admin.auditLogsFor(activeServiceId) })}
                disabled={isFetchingAudit}
                className="h-8 text-xs"
              >
                <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingAudit ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
          </div>
        )}
      />
    </div>
  )
}
