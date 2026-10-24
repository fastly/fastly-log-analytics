import * as React from 'react'

/**
 * Shared `@/components/ui/select` mock factory for jsdom tests.
 *
 * The real Select uses base-ui portals and native pointer event sequencing
 * that are awkward to drive in jsdom. This shim renders each `SelectItem`
 * as a `<button data-select-item-value="..."/>` that invokes `onValueChange`
 * when clicked.
 */
export function selectMockFactory() {
  const SelectCtx = React.createContext<((v: string) => void) | null>(null)

  const Select = ({
    value,
    onValueChange,
    children,
  }: {
    value?: string
    onValueChange?: (v: string) => void
    children?: React.ReactNode
  }) => (
    <SelectCtx.Provider value={onValueChange ?? null}>
      <div data-testid="mock-select" data-select-value={value ?? ''}>
        {children}
      </div>
    </SelectCtx.Provider>
  )

  const SelectTrigger = ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="mock-select-trigger">{children}</div>
  )

  const SelectValue = ({ children }: { children?: React.ReactNode }) => (
    <span data-testid="mock-select-value">{children}</span>
  )

  const SelectContent = ({ children }: { children?: React.ReactNode }) => (
    <div data-testid="mock-select-content">{children}</div>
  )

  const SelectSeparator = () => <hr data-testid="mock-select-separator" />

  const SelectItem = ({
    value,
    children,
  }: {
    value: string
    children?: React.ReactNode
  }) => {
    const onValueChange = React.useContext(SelectCtx)
    return (
      <button
        type="button"
        data-select-item-value={value}
        onClick={() => onValueChange?.(value)}
      >
        {children}
      </button>
    )
  }

  return {
    Select,
    SelectTrigger,
    SelectValue,
    SelectContent,
    SelectItem,
    SelectSeparator,
  }
}
