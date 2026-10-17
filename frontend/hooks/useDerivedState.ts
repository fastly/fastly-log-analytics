import { useState, type Dispatch, type SetStateAction } from 'react'

/**
 * State that starts from `source` and is reset to it whenever `source` changes (compared with
 * `Object.is`). The reset happens during render — React's sanctioned way to derive state from
 * props — so there is no extra committed render and no effect that sets state. Between changes
 * the setter edits freely (a draft that follows an upstream value until the user types).
 */
export function useDerivedState<T>(source: T): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState(source)
  const [seen, setSeen] = useState(source)
  if (!Object.is(seen, source)) {
    setSeen(source)
    setState(source)
  }
  return [state, setState]
}
