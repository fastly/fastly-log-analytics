'use client'
import React, { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useRouter } from 'next/navigation'
import { client } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import type { components } from '@/types/api.generated'
import { useServiceStore } from '@/stores/serviceStore'
// Direct import (not via the barrel) so the page bundle drops the
// @dnd-kit tree that the reorder-enabled DataTable pulls in.
import { DataTableReadonly as DataTable } from '@/components/DataTable/DataTableReadonly'
import { ProvisionWizard } from '@/components/ProvisionWizard/ProvisionWizard'
import { TeardownDialog } from '@/components/TeardownDialog'
import { CronSettingsModal } from '@/components/CronSettingsModal/CronSettingsModal'
import { LogSettingsModal } from '@/components/LogSettingsModal/LogSettingsModal'
import { InviteAnalystDialog } from '@/components/InviteAnalystDialog'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { AlertCircle, Plus } from 'lucide-react'

import { buildServiceColumns } from './ServicesTableColumns'
import { CredentialsDialog } from './CredentialsDialog'
import { NgwafDialog } from './NgwafDialog'

type ServiceConfig = components["schemas"]["ServiceConfig"]

export function ServicesTable() {
  const queryClient = useQueryClient()
  // Separate selectors — each subscribes to only its slice. Destructuring
  // useServiceStore() rebuilds the table on every setServices/setInitialized.
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const setActiveServiceId = useServiceStore(s => s.setActiveServiceId)
  const router = useRouter()
  const [cronService, setCronService] = useState<ServiceConfig | null>(null)
  const [settingsService, setSettingsService] = useState<ServiceConfig | null>(null)
  const [teardownService, setTeardownService] = useState<ServiceConfig | null>(null)
  const [inviteService, setInviteService] = useState<ServiceConfig | null>(null)
  const [credentialsService, setCredentialsService] = useState<ServiceConfig | null>(null)
  // Cached "initial mode" for the credentials dialog — picked when the
  // dialog opens so the child can re-init local state for each new service.
  const [credInitialMode, setCredInitialMode] = useState<'token' | 'manual'>('token')
  const [wizardOpen, setWizardOpen] = useState(false)
  const [ngwafService, setNgwafService] = useState<ServiceConfig | null>(null)

  function openCredentials(service: ServiceConfig) {
    setCredInitialMode(service.access_level === 'read_write' ? 'token' : 'manual')
    setCredentialsService(service)
  }

  const { data: services, isLoading, isError, error } = useQuery({
    queryKey: ['services'],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET("/api/services", { signal })
      return data
    },
  })

  const columns = React.useMemo(
    () => buildServiceColumns({
      activeServiceId,
      setActiveServiceId,
      router,
      servicesLength: services?.services?.length || 0,
      setCronService,
      setSettingsService,
      setTeardownService,
      setInviteService,
      openNgwaf: setNgwafService,
      openCredentials,
    }),
    [activeServiceId, setActiveServiceId, router, services?.services?.length],
  )

  return (
    <>
      <div className="space-y-4">
        <div className="flex items-center gap-4">
          <h2 className="text-xl font-semibold tracking-tight">Service Management</h2>
          <Button size="sm" onClick={() => setWizardOpen(true)}>
            <Plus className="h-4 w-4 mr-1" /> Add Service
          </Button>
        </div>

        {isError && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Couldn't load services</AlertTitle>
            <AlertDescription className="text-xs">
              {error instanceof Error ? error.message : 'The backend returned an error. The list below may be incomplete — investigate before provisioning new services.'}
            </AlertDescription>
          </Alert>
        )}

        <div className="border rounded-lg bg-card shadow-sm overflow-hidden">
          <DataTable
            columns={columns}
            data={services?.services || []}
            isLoading={isLoading}
            searchKey="name"
            emptyMessage="No services yet"
            emptyHint="Add a service to get started."
          />
        </div>
      </div>

      <ProvisionWizard
        open={wizardOpen}
        onOpenChange={setWizardOpen}
      />

      {cronService && (
        <CronSettingsModal
          service={cronService}
          open={!!cronService}
          onOpenChange={(open) => !open && setCronService(null)}
        />
      )}

      {settingsService && (
        <LogSettingsModal
          service={settingsService}
          open={!!settingsService}
          onOpenChange={(open) => !open && setSettingsService(null)}
        />
      )}

      <InviteAnalystDialog
        service={inviteService}
        open={!!inviteService}
        onOpenChange={(open) => !open && setInviteService(null)}
      />

      <TeardownDialog
        service={teardownService}
        open={!!teardownService}
        onOpenChange={(open) => !open && setTeardownService(null)}
        onComplete={() => {
          queryClient.invalidateQueries({ queryKey: ['services'] })
          queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() })
          setTeardownService(null)
        }}
      />

      <CredentialsDialog
        service={credentialsService}
        initialMode={credInitialMode}
        onClose={() => setCredentialsService(null)}
      />

      <NgwafDialog
        service={ngwafService}
        onClose={() => setNgwafService(null)}
      />
    </>
  )
}
