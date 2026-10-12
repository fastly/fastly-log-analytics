'use client'
import React, { useEffect, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { KeyRound, Loader2, Save, X } from 'lucide-react'
import { client, extractApiError } from '@/lib/api'
import { showToast } from '@/lib/toast'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

const QUERY_KEY = ['content-security-settings']

/** Overall Settings → Content Security: the blocklist HTTP function used by Streaming → Content Security. */
export function ContentSecuritySettingsPanel() {
  const queryClient = useQueryClient()
  const { data } = useQuery({
    queryKey: QUERY_KEY,
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/content-security/settings', { signal })
      if (error) throw error
      return data
    },
    staleTime: 60_000,
  })
  const saved = data?.blocklist_url ?? ''
  const savedHeaderName = data?.auth_header_name ?? null
  const [url, setUrl] = useState(saved)
  // The saved header's value never reaches the browser: this box only ever
  // holds a NEW header to send; blank keeps the saved one.
  const [header, setHeader] = useState('')
  const [removeHeader, setRemoveHeader] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setUrl(saved)
  }, [saved])

  async function save() {
    setSaving(true)
    setError(null)
    try {
      const newHeader = header.trim()
      const { data: next, error: apiError } = await client.PUT('/api/admin/content-security/settings', {
        body: {
          blocklist_url: url.trim(),
          // Omitted keeps the saved header; '' removes it.
          ...(newHeader ? { auth_header: newHeader } : removeHeader ? { auth_header: '' } : {}),
        },
      })
      if (apiError) throw apiError
      queryClient.setQueryData(QUERY_KEY, next)
      setHeader('')
      setRemoveHeader(false)
      // The Content Security tab's "configured" flag rides on the blocks query.
      queryClient.invalidateQueries({ queryKey: ['content-security', 'blocks'] })
      showToast(next?.blocklist_url ? 'Blocklist endpoint saved' : 'Blocklist endpoint cleared', 'success')
    } catch (e) {
      setError(extractApiError(e))
    } finally {
      setSaving(false)
    }
  }

  const dirty = url.trim() !== saved || header.trim() !== '' || removeHeader
  let headerHint: React.ReactNode = 'Optional. Sent with every call, e.g. Authorization: Bearer <token>.'
  if (removeHeader) {
    headerHint = `The ${savedHeaderName} header will be removed when you save.`
  } else if (savedHeaderName && !header) {
    headerHint = `Saved header: ${savedHeaderName} (value hidden). Type a new one to replace it.`
  }

  return (
    <div className="flex flex-col p-3 border rounded-lg gap-3">
      <div className="min-w-0 space-y-0.5">
        <Label htmlFor="gs-blocklist-url" className="text-sm font-medium">
          Content Security
        </Label>
        <p className="text-xs text-muted-foreground">
          Blocklist endpoint for Streaming → Content Security. Blocking sends{' '}
          <code className="font-mono">POST</code> with JSON{' '}
          <code className="font-mono">{'{"kbps": 0, "conditions": [{"category": "country", "value": "CN"}]}'}</code>.
          All conditions must match; <code className="font-mono">kbps</code> 0 blocks, a positive value throttles to that
          rate. Undo sends <code className="font-mono">DELETE</code> with the same body. Leave empty
          to turn blocking off.
        </p>
      </div>
      <form
        className="flex flex-col gap-2"
        onSubmit={(e) => {
          e.preventDefault()
          void save()
        }}
      >
        <Input
          id="gs-blocklist-url"
          type="url"
          inputMode="url"
          placeholder="https://example.com/blocklist"
          className="h-8 font-mono text-xs"
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          aria-invalid={error ? true : undefined}
          aria-describedby={error ? 'gs-blocklist-url-error' : undefined}
        />
        <div className="flex items-center gap-2">
          <div className="relative flex-1">
            <KeyRound
              className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground"
              aria-hidden="true"
            />
            <Input
              // A credential: masked, and kept out of browser autofill/spellcheck.
              type="password"
              autoComplete="off"
              spellCheck={false}
              aria-label="Authorization header"
              aria-describedby="gs-blocklist-header-hint"
              placeholder={savedHeaderName ? `${savedHeaderName}: ••••••••` : 'Authorization: Bearer <token>'}
              className="h-8 pl-8 font-mono text-xs"
              value={header}
              onChange={(e) => {
                setHeader(e.target.value)
                if (e.target.value) setRemoveHeader(false)
              }}
            />
          </div>
          {savedHeaderName && !removeHeader && (
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-8"
              onClick={() => {
                setHeader('')
                setRemoveHeader(true)
              }}
            >
              <X className="h-3 w-3 mr-1" />
              Remove
            </Button>
          )}
          <Button type="submit" size="sm" className="h-8" disabled={saving || !dirty}>
            {saving ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : <Save className="h-3 w-3 mr-1" />}
            {saving ? 'Saving…' : 'Save'}
          </Button>
        </div>
        <p id="gs-blocklist-header-hint" className="text-xs text-muted-foreground">
          {headerHint}
        </p>
      </form>
      {error && (
        <p id="gs-blocklist-url-error" role="alert" className="text-xs text-destructive">
          {error}
        </p>
      )}
    </div>
  )
}
