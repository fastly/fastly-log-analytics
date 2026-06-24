'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  RefreshCw,
  ChevronDown,
} from 'lucide-react'
import { Button, buttonVariants } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
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
import { DataTableReadonly as DataTable } from '@/components/DataTable/DataTableReadonly'
import { ColumnDef } from '@tanstack/react-table'

export function ServiceHistoryTab({
  auditColumns,
  auditLogs,
  isLoadingAudit,
  isFetchingAudit,
  eventFilter,
  setEventFilter,
  activeServiceId,
}: {
  auditColumns: ColumnDef<any>[]
  auditLogs: any
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
              <DropdownMenu>
                <DropdownMenuTrigger
                  className={buttonVariants({ variant: "outline", size: "sm", className: "h-8" })}
                >
                  <span className="flex items-center text-xs">
                    Columns <ChevronDown className="ml-2 h-4 w-4" />
                  </span>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-auto min-w-[200px]">
                  {table
                    .getAllColumns()
                    .filter((column: any) => column.getCanHide())
                    .map((column: any) => {
                      return (
                        <DropdownMenuCheckboxItem
                          key={column.id}
                          className="capitalize whitespace-nowrap"
                          checked={column.getIsVisible()}
                          onCheckedChange={(value) =>
                            column.toggleVisibility(!!value)
                          }
                        >
                          {column.id.replace(/_/g, ' ')}
                        </DropdownMenuCheckboxItem>
                      )
                    })}
                </DropdownMenuContent>
              </DropdownMenu>
              <Button
                variant="outline"
                size="sm"
                onClick={() => queryClient.invalidateQueries({ queryKey: ['admin', 'audit-logs', activeServiceId] })}
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
