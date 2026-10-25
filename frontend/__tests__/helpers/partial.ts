import type { client } from '@/lib/api'

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

/**
 * The single sanctioned type escape in the test tree. Use it ONLY to feed a
 * deliberately malformed value (a shape the compiler would reject) into code
 * under test to prove runtime handling. It returns `never`, so it is
 * assignable to whatever parameter the call site expects without inference
 * games. Not for silencing ordinary type mismatches: build those fixtures
 * with `partial` or typed payloads.
 */
export function invalidInput(value: unknown): never {
  return value as never
}

type GetResult = Awaited<ReturnType<typeof client.GET>>
type PostResult = Awaited<ReturnType<typeof client.POST>>

/** Successful openapi-fetch GET result carrying `data`; type the payload at the call site with `satisfies`. */
export function getOk(data: NonNullable<GetResult['data']>): GetResult {
  return partial<GetResult>({ data, error: undefined })
}

/** Successful openapi-fetch POST result carrying `data`. */
export function postOk(data: NonNullable<PostResult['data']>): PostResult {
  return partial<PostResult>({ data, error: undefined })
}
