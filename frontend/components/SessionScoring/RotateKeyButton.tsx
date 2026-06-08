'use client'

import * as React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { KeyRound, Loader2 } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { client } from '@/lib/api'

interface RotateKeyButtonProps {
  serviceId: string
  onRotated?: () => void
}

interface RotateKeyResponse {
  ok?: boolean
  rotated_at?: string
  [k: string]: unknown
}

/**
 * Rotate the AES cookie key used to encrypt session scoring cookies.
 *
 * The new key becomes 'current'; the previous 'current' key moves to the
 * 'previous' slot so already-issued cookies stay decodable for one grace
 * cycle. Two rotations back-to-back will drop the original key entirely
 * and mark its cookies as 'tampered' at the edge.
 */
export function RotateKeyButton({ serviceId, onRotated }: RotateKeyButtonProps) {
  const qc = useQueryClient()
  const [open, setOpen] = React.useState(false)
  const [status, setStatus] = React.useState<
    { kind: 'success' | 'error'; message: string } | null
  >(null)

  const mutation = useMutation({
    mutationFn: async (): Promise<RotateKeyResponse> => {
      const { data, response } = await client.POST(
        '/api/services/{service_id}/scoring/rotate-key' as any,
        {
          params: { path: { service_id: serviceId } },
        } as any,
      )
      if (!response.ok) {
        const msg = (data as any)?.detail?.error ?? `status ${response.status}`
        throw new Error(msg)
      }
      return data as RotateKeyResponse
    },
    onSuccess: () => {
      setStatus({ kind: 'success', message: 'Key rotated; previous slot updated' })
      setOpen(false)
      qc.invalidateQueries({ queryKey: ['scoring-status', serviceId] })
      qc.invalidateQueries({ queryKey: ['scoring-audit', serviceId] })
      onRotated?.()
      window.setTimeout(() => setStatus(null), 4000)
    },
    onError: (err: unknown) => {
      const message = err instanceof Error ? err.message : 'Failed to rotate key'
      setStatus({ kind: 'error', message })
    },
  })

  return (
    <div className="inline-flex flex-col items-end gap-1">
      <Button
        variant="destructive"
        size="sm"
        onClick={() => {
          setStatus(null)
          setOpen(true)
        }}
        disabled={mutation.isPending}
        title="Rotate the AES key used to encrypt scoring cookies"
      >
        {mutation.isPending ? (
          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
        ) : (
          <KeyRound className="h-4 w-4 mr-1" />
        )}
        Rotate AES key
      </Button>

      {status && (
        <p
          className={`text-[11px] ${
            status.kind === 'success' ? 'text-emerald-600' : 'text-destructive'
          }`}
          role="status"
        >
          {status.message}
        </p>
      )}

      <ConfirmDialog
        open={open}
        onOpenChange={(o) => {
          if (!mutation.isPending) setOpen(o)
        }}
        isDangerous
        isPending={mutation.isPending}
        title="Rotate AES cookie key?"
        description={
          <div className="space-y-2 text-sm">
            <p>
              Generates a new AES key and promotes it to the <strong>current</strong>{' '}
              slot. The old current key moves to <strong>previous</strong> and is still
              accepted for one grace cycle.
            </p>
            <ul className="list-disc list-inside space-y-1 text-xs">
              <li>In-flight sessions keep working — their cookies decode under the previous-slot key.</li>
              <li>New sessions get cookies encrypted with the new current key.</li>
              <li>
                <strong>Warning:</strong> rotating twice in quick succession discards the
                original key entirely. Cookies issued under it will be treated as{' '}
                <span className="font-mono">tampered</span> at the edge.
              </li>
            </ul>
          </div>
        }
        confirmLabel="Rotate key"
        cancelLabel="Cancel"
        onConfirm={() => mutation.mutate()}
      />
    </div>
  )
}
