'use client'

import { Keyboard } from 'lucide-react'

import { HelpDialog } from '@/components/ui/help-dialog'

export function ShortcutsHelp({
  open,
  onOpenChange,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  return (
    <HelpDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Keyboard shortcuts"
      icon={<Keyboard className="h-4 w-4" />}
    >
      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm text-foreground">
        <Kbd>/</Kbd>
        <dd>Focus the search field</dd>

        <Kbd>j</Kbd>
        <dd>Move focus to the next active row</dd>

        <Kbd>k</Kbd>
        <dd>Move focus to the previous active row</dd>

        <Kbd>Enter</Kbd>
        <dd>Expand / collapse the focused row</dd>

        <Kbd>x</Kbd>
        <dd>Cancel the focused query (admins only — same kind-aware confirm as the Kill button)</dd>

        <Kbd>Esc</Kbd>
        <dd>Close the expanded row, the kill confirm dialog, or this help overlay</dd>

        <Kbd>?</Kbd>
        <dd>Show this help overlay</dd>
      </dl>
      <p className="mt-4 text-xs text-muted-foreground">
        Shortcuts are disabled while typing in the search box (except <kbd>Esc</kbd>, which always
        works).
      </p>
    </HelpDialog>
  )
}

function Kbd({ children }: { children: React.ReactNode }) {
  return (
    <dt className="self-center">
      <kbd className="inline-flex items-center justify-center min-w-[1.5rem] h-6 px-1.5 rounded border border-border bg-muted/50 text-xs font-mono">
        {children}
      </kbd>
    </dt>
  )
}
