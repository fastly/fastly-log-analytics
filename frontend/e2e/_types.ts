import type { DebugQuery as WireDebugQuery, DebugCall as WireDebugCall } from '../types/api'

// `params` is emitted by some debug payloads but is not in the OpenAPI schema.
export type DebugQuery = Partial<WireDebugQuery> & { params?: unknown }
export type DebugCall = Partial<WireDebugCall>
