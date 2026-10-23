import { createContext, useContext } from 'react'

/**
 * Create a strongly-typed React Context and consumer hook for a route section.
 * Throws a descriptive error if the hook is called outside its Provider.
 */
export function createSectionContext<T>(sectionName: string) {
  const Context = createContext<T | null>(null)
  Context.displayName = `${sectionName}Context`

  function useSectionContext(): T {
    const value = useContext(Context)
    if (value === null) {
      throw new Error(`use${sectionName}Context must be used within a ${sectionName}Provider`)
    }
    return value
  }

  return [Context.Provider, useSectionContext, Context] as const
}
