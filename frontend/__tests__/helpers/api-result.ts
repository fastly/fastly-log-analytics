import type { client } from '@/lib/api'
import { partial } from './partial'

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
