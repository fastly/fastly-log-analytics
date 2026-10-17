import * as React from 'react'
import { ChevronDown } from 'lucide-react'
import { cn } from '@/lib/utils'

/**
 * A native `<select>` with the app's input look: the browser arrow is replaced by a chevron
 * with room around it, so the label never crowds the edge. Use where a native control is wanted
 * (inline table rows, keyboard-first forms); otherwise prefer `Select`.
 */
function NativeSelect({ className, children, ...props }: React.ComponentProps<'select'>) {
  return (
    <span className="relative inline-flex">
      <select
        data-slot="native-select"
        className={cn(
          'h-8 w-full appearance-none rounded-lg border border-input bg-transparent py-1 pr-8 pl-2.5 text-xs font-medium outline-none transition-colors',
          'focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50 dark:bg-input/30',
          className
        )}
        {...props}
      >
        {children}
      </select>
      <ChevronDown
        className="pointer-events-none absolute top-1/2 right-2.5 size-3.5 -translate-y-1/2 text-muted-foreground"
        aria-hidden="true"
      />
    </span>
  )
}

export { NativeSelect }
