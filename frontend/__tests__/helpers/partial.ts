type DeepPartial<T> = T extends readonly (infer U)[]
  ? DeepPartial<U>[]
  : T extends object
    ? { [K in keyof T]?: DeepPartial<T[K]> }
    : T

/** Build a fixture from only the fields a test reads; the rest of `T` stays unset. */
export function partial<T>(value: DeepPartial<T>): T {
  return value as T
}

/** Copy of `obj` with `key` removed, for tests that exercise a missing required field. */
export function without<T extends object, K extends keyof T>(obj: T, key: K): Omit<T, K> {
  const copy: Partial<T> = { ...obj }
  delete copy[key]
  return copy as Omit<T, K>
}
