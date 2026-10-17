'use client'

import React from 'react'
import { parseAsString, useQueryState } from 'nuqs'
import { ListChecks, Pencil, Plus, ScrollText, Trash2 } from 'lucide-react'
import { formatDate } from '@/lib/date'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import type { StreamingTabProps } from './QualityTab'
import { AssignmentDialog } from './AssignmentDialog'
import { PolicyEditorDialog } from './PolicyEditorDialog'
import { PolicySyncNotice } from './PolicySyncNotice'
import {
  describeScope,
  summarizePolicy,
  useAssignments,
  usePolicies,
  usePolicyActions,
  useSyncStatus,
  type Assignment,
  type Policy,
  modeLabel,
} from './policyApi'
import { suggestionsFromData, useContentSecurityData } from './securityData'

// ?policy=<id|new> opens the policy editor, so an editor deep-links (and Monitor's "Edit policy" lands in it).
const policyParser = parseAsString.withOptions({ history: 'replace', shallow: true })

const NEW_POLICY = 'new'

const utc = (iso: string) => `${formatDate(iso, 'UTC', 'yyyy-MM-dd HH:mm')} UTC`

const STATUS_LABEL = { scheduled: 'Scheduled', active: 'Active', ended: 'Ended' } as const
const STATUS_VARIANT = { scheduled: 'secondary', active: 'success', ended: 'ghost' } as const

type PendingDelete = { kind: 'policy'; policy: Policy } | { kind: 'assignment'; assignment: Assignment }

/** Default first, then content assignments by start time (newest first). */
function sortAssignments(rows: Assignment[]): Assignment[] {
  return [...rows].sort((a, b) => {
    if (a.scope_type !== b.scope_type) return a.scope_type === 'default' ? -1 : 1
    return (b.starts_at ?? '').localeCompare(a.starts_at ?? '')
  })
}

/** "Default, ev-123, ev-099 +2": where a policy is assigned. */
function assignedTo(policy: Policy): string {
  const labels = policy.used_by.map((u) => (u.scope_type === 'default' ? 'Default' : (u.content_id ?? '')))
  if (!labels.length) return 'Not assigned'
  const unique = [...new Set(labels)]
  return unique.length > 3 ? `${unique.slice(0, 3).join(', ')} +${unique.length - 3}` : unique.join(', ')
}

/** Streaming → Content Security → Policies: what is assigned where, and the policies themselves. */
export default function PoliciesView(props: StreamingTabProps & { bucketSeconds: number }) {
  const { activeServiceId, timezone } = props
  const [policyParam, setPolicyParam] = useQueryState('policy', policyParser)
  const policies = usePolicies(activeServiceId)
  const assignments = useAssignments(activeServiceId)
  const sync = useSyncStatus(activeServiceId)
  const { savePolicy, deletePolicy, saveAssignment, deleteAssignment } = usePolicyActions()

  const [assignmentDialog, setAssignmentDialog] = React.useState<{ assignment: Assignment | null } | null>(null)
  const [pendingDelete, setPendingDelete] = React.useState<PendingDelete | null>(null)
  const [deleting, setDeleting] = React.useState(false)

  const policyList = React.useMemo(
    () => [...(policies.data?.policies ?? [])].sort((a, b) => a.name.localeCompare(b.name)),
    [policies.data?.policies],
  )
  const assignmentList = React.useMemo(() => sortAssignments(assignments.data?.assignments ?? []), [assignments.data?.assignments])

  const editorPolicy = policyParam && policyParam !== NEW_POLICY ? policyList.find((p) => p.id === policyParam) : undefined
  const editorOpen = policyParam === NEW_POLICY || editorPolicy != null
  // Suggestions come from the Monitor view's own data; only fetch them while a dialog needs them.
  const observed = useContentSecurityData(props, null, {
    enabled: editorOpen || assignmentDialog != null,
    // The most the API returns, so the country list can be ranked by traffic.
    topN: 100,
  })
  const suggestions = React.useMemo(() => suggestionsFromData(observed.data), [observed.data])

  // The blocker is behind the saved revision: live-looking rows are not live yet.
  const notLive = sync.data?.configured === true && !sync.data.in_sync

  const confirmDelete = async () => {
    if (!pendingDelete) return
    setDeleting(true)
    try {
      if (pendingDelete.kind === 'policy') await deletePolicy(pendingDelete.policy)
      else await deleteAssignment(pendingDelete.assignment)
    } catch {
      // The api client already toasts DELETE failures (e.g. a policy that is still assigned).
    } finally {
      setDeleting(false)
      setPendingDelete(null)
    }
  }

  return (
    <div className="space-y-6">
      <PolicySyncNotice activeServiceId={activeServiceId} timezone={timezone} />

      <AnalyticsCard
        title="Policy schedule"
        description="Which policy applies to which content, and when. A content ID's own entry overrides the all-content one."
        icon={<ListChecks className="h-4 w-4" />}
        isLoading={assignments.isLoading}
        error={assignments.error as AnalyticsCardError | null}
        className="min-h-[160px]"
        headerAction={
          <Button
            size="sm"
            className="h-8 text-xs"
            disabled={policyList.length === 0}
            title={policyList.length === 0 ? 'Create a policy first' : undefined}
            onClick={() => setAssignmentDialog({ assignment: null })}
          >
            <Plus className="h-3.5 w-3.5 mr-1" aria-hidden="true" />
            Schedule a policy
          </Button>
        }
      >
        {assignmentList.length ? (
          <Table aria-label="Policy schedule">
            <TableHeader>
              <TableRow>
                <TableHead>Scope</TableHead>
                <TableHead>Policy</TableHead>
                <TableHead>Mode</TableHead>
                <TableHead>Starts</TableHead>
                <TableHead>Ends</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="w-24">
                  <span className="sr-only">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {assignmentList.map((a) => {
                const scope = describeScope(a.scope_type, a.content_id)
                return (
                  <TableRow key={a.id}>
                    <TableCell className="max-w-[16rem] truncate font-mono text-xs" title={scope}>
                      {a.scope_type === 'default' ? (
                        <>
                          All content <span className="text-muted-foreground">(default)</span>
                        </>
                      ) : (
                        `Content: ${a.content_id}`
                      )}
                    </TableCell>
                    <TableCell className="max-w-[14rem] truncate">{a.policy_name ?? a.policy_id}</TableCell>
                    <TableCell>
                      <Badge variant={a.mode === 'WARN' ? 'warning' : 'outline'}>{modeLabel(a.mode)}</Badge>
                    </TableCell>
                    <TableCell className="whitespace-nowrap tabular-nums text-xs">{a.starts_at ? utc(a.starts_at) : '(always)'}</TableCell>
                    <TableCell className="whitespace-nowrap tabular-nums text-xs">{a.ends_at ? utc(a.ends_at) : '(none)'}</TableCell>
                    <TableCell>
                      <span className="inline-flex flex-wrap items-center gap-1">
                        <Badge variant={STATUS_VARIANT[a.status]}>{STATUS_LABEL[a.status]}</Badge>
                        {notLive && a.status !== 'ended' && <Badge variant="warning">Not yet live</Badge>}
                      </span>
                    </TableCell>
                    <TableCell className="text-right">
                      <span className="inline-flex gap-0.5">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7"
                          aria-label={`Edit schedule for ${scope}`}
                          onClick={() => setAssignmentDialog({ assignment: a })}
                        >
                          <Pencil className="h-3.5 w-3.5" aria-hidden="true" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7"
                          aria-label={`Remove schedule for ${scope}`}
                          onClick={() => setPendingDelete({ kind: 'assignment', assignment: a })}
                        >
                          <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
                        </Button>
                      </span>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        ) : (
          <p className="px-2 py-6 text-center text-sm text-muted-foreground">
            Nothing is scheduled. Until you schedule a policy, no rules are enforced.
          </p>
        )}
      </AnalyticsCard>

      <AnalyticsCard
        title="Policies"
        description="Named rule sets for this service. Editing a policy applies everywhere it is scheduled."
        icon={<ScrollText className="h-4 w-4" />}
        isLoading={policies.isLoading}
        error={policies.error as AnalyticsCardError | null}
        className="min-h-[160px]"
        headerAction={
          <Button size="sm" className="h-8 text-xs" onClick={() => void setPolicyParam(NEW_POLICY)}>
            <Plus className="h-3.5 w-3.5 mr-1" aria-hidden="true" />
            New policy
          </Button>
        }
      >
        {policyList.length ? (
          <Table aria-label="Policies">
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Rules</TableHead>
                <TableHead>Assigned to</TableHead>
                <TableHead>Updated</TableHead>
                <TableHead className="w-24">
                  <span className="sr-only">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {policyList.map((p) => (
                <TableRow key={p.id}>
                  <TableCell className="max-w-[14rem] truncate font-medium" title={p.name}>
                    <button
                      type="button"
                      className="max-w-full truncate text-left hover:underline focus-visible:underline focus-visible:outline-none"
                      onClick={() => void setPolicyParam(p.id)}
                    >
                      {p.name}
                    </button>
                  </TableCell>
                  <TableCell className="max-w-[24rem] truncate text-xs text-muted-foreground" title={summarizePolicy(p)}>
                    {summarizePolicy(p)}
                  </TableCell>
                  <TableCell className="max-w-[14rem] truncate text-xs" title={assignedTo(p)}>
                    {assignedTo(p)}
                  </TableCell>
                  <TableCell className="whitespace-nowrap text-xs tabular-nums" title={`by ${p.updated_by}`}>
                    {formatDate(p.updated_at, timezone, 'yyyy-MM-dd HH:mm')}
                  </TableCell>
                  <TableCell className="text-right">
                    <span className="inline-flex gap-0.5">
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-7 w-7"
                        aria-label={`Edit policy ${p.name}`}
                        onClick={() => void setPolicyParam(p.id)}
                      >
                        <Pencil className="h-3.5 w-3.5" aria-hidden="true" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-7 w-7"
                        aria-label={`Delete policy ${p.name}`}
                        onClick={() => setPendingDelete({ kind: 'policy', policy: p })}
                      >
                        <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
                      </Button>
                    </span>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <p className="px-2 py-6 text-center text-sm text-muted-foreground">
            No policies yet. Create one, or block a value from the Monitor view to create a Default policy.
          </p>
        )}
      </AnalyticsCard>

      {editorOpen && (
        <PolicyEditorDialog
          key={editorPolicy?.id ?? NEW_POLICY}
          policy={editorPolicy ?? null}
          suggestions={suggestions}
          timezone={timezone}
          onSave={savePolicy}
          onClose={() => void setPolicyParam(null)}
        />
      )}

      <AssignmentDialog
        open={assignmentDialog != null}
        onOpenChange={(open) => !open && setAssignmentDialog(null)}
        assignment={assignmentDialog?.assignment ?? null}
        policies={policyList}
        contentIds={observed.data?.content_ids ?? []}
        onSave={saveAssignment}
      />

      <ConfirmDialog
        open={pendingDelete != null}
        onOpenChange={(open) => !open && !deleting && setPendingDelete(null)}
        title={pendingDelete?.kind === 'policy' ? 'Delete this policy?' : 'Remove this scheduled policy?'}
        description={
          pendingDelete?.kind === 'policy'
            ? `Delete ${pendingDelete.policy.name}. A policy that is still scheduled cannot be deleted; remove it from the schedule first.`
            : pendingDelete?.kind === 'assignment'
              ? `${describeScope(pendingDelete.assignment.scope_type, pendingDelete.assignment.content_id)} stops using ${pendingDelete.assignment.policy_name ?? 'its policy'} straight away.`
              : ''
        }
        confirmLabel="Delete"
        isDangerous
        isPending={deleting}
        onConfirm={() => void confirmDelete()}
      />
    </div>
  )
}
