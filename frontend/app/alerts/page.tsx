'use client'

import React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { ReportShell } from '@/components/ReportShell'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import {
  Bell,
  Plus,
  AlertTriangle,
  Clock,
  Zap,
  BellPlus,
  Info,
  Loader2,
} from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { ColumnVisibilityDropdown } from '@/components/DataTable'
import { VisibilityState } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import { AlertsList, ALERTS_AVAILABLE_COLUMNS } from './_sections/AlertsList'
import { CreateAlertForm } from './_sections/AlertEditor'

type Alert = components["schemas"]["Alert"]

export default function AlertsPage() {
  const { activeServiceId } = useServiceStore()
  const queryClient = useQueryClient()
  const [isFormOpen, setIsFormOpen] = React.useState(false)
  const [editingAlert, setEditingAlert] = React.useState<Alert | null>(null)
  const [deleteTarget, setDeleteTarget] = React.useState<string | null>(null)
  const [isDeleting, setIsDeleting] = React.useState(false)
  const [columnVisibility, setColumnVisibility] = React.useState<VisibilityState>({})
  const { full } = useDateFormat()

  const { data: loggingSettings } = useQuery({
    queryKey: ['loggingSettings', activeServiceId],
    queryFn: async ({ signal }) => {
      if (!activeServiceId) return null
      const { data } = await client.GET("/api/services/{service_id}/logging-settings", {
        signal,
        params: { path: { service_id: activeServiceId } }
      })
      return data as any
    },
    enabled: !!activeServiceId,
    // M4: this endpoint chains 3 sequential Fastly calls (~200ms total)
    // to resolve the active version + S3 endpoint + sampling condition.
    // None of that changes between window focuses, so cache the result
    // for 30s — eliminates the per-focus refetch on this page and on
    // every alerts-page mount within the window.
    staleTime: 30_000,
  })

  const logPeriodSeconds = (loggingSettings as any)?.period || 30

  const { data: alertsRes, isLoading, isFetching, refetch } = useQuery({
    queryKey: ['alerts', activeServiceId],
    queryFn: async ({ signal }) => {
      if (activeServiceId) {
        const { data } = await client.GET("/api/alerts/{service_id}", {
          signal,
          params: { path: { service_id: activeServiceId } }
        })
        return data
      } else {
        const { data } = await client.GET("/api/alerts/", { signal })
        return data
      }
    },
    refetchInterval: logPeriodSeconds * 1000,
  })

  const alerts = alertsRes?.data || []
  const lastChecked = alertsRes?.evaluated_at || new Date().toISOString()

  const confirmDelete = React.useCallback(async () => {
    if (!deleteTarget) return
    setIsDeleting(true)
    try {
      await client.DELETE("/api/alerts/{alert_id}", {
        params: { path: { alert_id: deleteTarget } }
      })
      queryClient.invalidateQueries({ queryKey: ['alerts'] })
    } finally {
      setIsDeleting(false)
      setDeleteTarget(null)
    }
  }, [deleteTarget, queryClient])

  const handleEdit = React.useCallback((alert: Alert) => {
    setEditingAlert(alert)
    setIsFormOpen(true)
  }, [])

  const handleCreate = React.useCallback(() => {
    setEditingAlert(null)
    setIsFormOpen(true)
  }, [])

  return (
    <ReportShell
      title="Alerts"
      headerActions={
        <div className="flex items-center gap-4">
          <Badge variant="secondary" className="gap-2 font-mono text-xs">
            <Clock className="w-3 h-3 text-muted-foreground" />
            {isFetching ? 'Refreshing...' : `Last Checked: ${full(lastChecked)}`}
          </Badge>
          <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isFetching || isLoading}>
             <Loader2 className={`w-4 h-4 mr-2 ${isFetching ? 'animate-spin' : 'hidden'}`} />
            Refresh Now
          </Button>
          <Button onClick={handleCreate}>
            <Plus className="w-4 h-4 mr-2" />
            Create Alert
          </Button>
        </div>
      }
      description={
        <div className="flex items-center gap-2">
          <span>Configure threshold-based alerts and webhook notifications.</span>
          {activeServiceId && (
            <Popover>
              <PopoverTrigger>
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded-md hover:bg-accent transition-colors">
                  <Clock className="h-3 w-3" />
                  <span>Evaluated every {logPeriodSeconds}s</span>
                  <Info className="h-3 w-3" />
                </div>
              </PopoverTrigger>
              <PopoverContent className="w-80 text-sm">
                <div className="space-y-2">
                  <h4 className="font-medium flex items-center gap-2">
                    <Zap className="h-4 w-4 text-yellow-500" />
                    Evaluation Frequency
                  </h4>
                  <p className="text-muted-foreground">
                    Alerts are evaluated dynamically based on your Fastly service's log batching period (currently <strong>{logPeriodSeconds} seconds</strong>).
                  </p>
                  <p className="text-muted-foreground">
                    This ensures alerts trigger the moment a new batch of logs arrives from Fastly, without uselessly spamming the database in between batches.
                  </p>
                  <div className="bg-muted p-2 rounded text-xs mt-2 text-muted-foreground">
                    Note: Alert queries execute in the background server process, so they do not appear in the DuckDB Queries debug panel.
                  </div>
                </div>
              </PopoverContent>
            </Popover>
          )}
        </div>
      }
      icon={Bell}
      requireService={false}
      isReadyOverride={true}
    >
      <Dialog open={isFormOpen} onOpenChange={setIsFormOpen}>
        <DialogContent className="sm:max-w-5xl max-h-[90vh] flex flex-col p-6">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {editingAlert ? <Bell className="h-5 w-5 text-primary" /> : <BellPlus className="h-5 w-5 text-primary" />}
              {editingAlert ? `Edit Alert: ${editingAlert.name}` : 'Create New Alert'}
            </DialogTitle>
              <DialogDescription>
                Threshold alerts evaluate logs in real-time every {logPeriodSeconds} seconds.
              </DialogDescription>
            </DialogHeader>
            <CreateAlertForm
              initialAlert={editingAlert}
              onSuccess={() => setIsFormOpen(false)}
            />
          </DialogContent>
        </Dialog>
      <AnalyticsCard
        title="Configured Alerts"
        icon={<Bell className="h-4 w-4" />}
        isLoading={isLoading}
        className="min-h-[300px]"
        contentClassName="p-0"
        headerAction={
          <ColumnVisibilityDropdown
            columns={ALERTS_AVAILABLE_COLUMNS}
            visibility={columnVisibility}
            onChange={(id, visible) => setColumnVisibility(prev => ({ ...prev, [id]: visible }))}
          />
        }
      >
        <AlertsList
          alerts={alerts}
          columnVisibility={columnVisibility}
          setColumnVisibility={setColumnVisibility}
          onEdit={handleEdit}
          onDelete={setDeleteTarget}
        />
      </AnalyticsCard>

      <ConfirmDialog
        open={deleteTarget !== null}
        onOpenChange={(open) => { if (!open && !isDeleting) setDeleteTarget(null) }}
        onConfirm={confirmDelete}
        isPending={isDeleting}
        isDangerous
        title="Delete Alert"
        description="Are you sure you want to delete this alert? This action cannot be undone."
        confirmLabel="Delete"
      />

      {!activeServiceId && (
        <div className="p-4 rounded-lg bg-yellow-500/10 border border-yellow-500/20 text-yellow-600 dark:text-yellow-400 flex items-center gap-3 mt-6">
          <AlertTriangle className="h-5 w-5" />
          <p className="text-sm">
            Showing alerts for <strong>all services</strong>. Select a service to filter.
          </p>
        </div>
      )}
    </ReportShell>
  )
}
