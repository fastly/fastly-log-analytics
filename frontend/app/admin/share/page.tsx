'use client'

import * as React from 'react'
import { ArrowLeft, ShieldAlert } from 'lucide-react'
import Link from 'next/link'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { buttonVariants } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { AuditLogPanel } from '@/components/share-dashboard/AuditLogPanel'
import { InvitationsPanel } from '@/components/share-dashboard/InvitationsPanel'
import { SessionsPanel } from '@/components/share-dashboard/SessionsPanel'
import { SharingControlPanel } from '@/components/share-dashboard/SharingControlPanel'
import type { ShareStatus } from '@/components/share-dashboard/utils'
import { client, extractApiError } from '@/lib/api'

export default function ShareDashboardPage() {
  const [status, setStatus] = React.useState<ShareStatus | null>(null)
  const [statusError, setStatusError] = React.useState('')
  const [actionError, setActionError] = React.useState('')
  const [activeTab, setActiveTab] = React.useState('invites')
  const [auditEmailFilter, setAuditEmailFilter] = React.useState('')

  const refresh = React.useCallback(async () => {
    setStatusError('')
    try {
      const { data, response } = await client.GET('/api/admin/share/status' as any, {})
      if (!response.ok) throw new Error(`status ${response.status}`)
      setStatus(data as any)
    } catch (e: any) {
      setStatusError(extractApiError(e) || 'unable to load status')
    }
  }, [])

  React.useEffect(() => {
    refresh()
    const id = setInterval(refresh, 10_000)
    return () => clearInterval(id)
  }, [refresh])

  return (
    <div className="space-y-6">
      <PageHeader
        title="Remote Dashboard Sharing"
        description="Start the share tunnel, manage analyst invitations, monitor live sessions, and review the audit trail."
        icon={ShieldAlert}
      >
        <Link href="/admin" className={buttonVariants({ variant: 'outline', size: 'sm' })}>
          <ArrowLeft className="h-4 w-4 mr-1" />
          Back to Admin
        </Link>
      </PageHeader>

      {statusError && (
        <Alert variant="destructive">
          <AlertDescription>{statusError}</AlertDescription>
        </Alert>
      )}
      {actionError && (
        <Alert variant="destructive">
          <AlertDescription>{actionError}</AlertDescription>
        </Alert>
      )}

      <SharingControlPanel status={status} onRefresh={refresh} onError={setActionError} />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
        <TabsList>
          <TabsTrigger value="invites">Invitations</TabsTrigger>
          <TabsTrigger value="sessions">Sessions</TabsTrigger>
          <TabsTrigger value="audit">Audit</TabsTrigger>
        </TabsList>

        <TabsContent value="invites" className="pt-4">
          <InvitationsPanel
            status={status}
            onRefresh={refresh}
            onError={setActionError}
            onViewAuditLogs={(email) => {
              setAuditEmailFilter(email)
              setActiveTab('audit')
            }}
          />
        </TabsContent>

        <TabsContent value="sessions" className="pt-4">
          <SessionsPanel status={status} onRefresh={refresh} onError={setActionError} />
        </TabsContent>

        <TabsContent value="audit" className="pt-4">
          <AuditLogPanel
            status={status}
            onError={setActionError}
            initialEmailFilter={auditEmailFilter}
            onClearInitialFilter={() => setAuditEmailFilter('')}
          />
        </TabsContent>
      </Tabs>
    </div>
  )
}
