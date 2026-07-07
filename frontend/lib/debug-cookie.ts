// Cookie name shared between the server SSR transport (reads it via
// next/headers to decide whether to attach x-debug-responses to its own
// upstream fetch) and the client DiagnosticsPanel (writes it when either
// debug toggle flips). No 'use client' directive — see lib/sidebar-cookie.ts
// for why: importing a string export FROM a 'use client' module returns
// undefined during SSR.
export const DEBUG_RESPONSES_COOKIE = 'fla.debugResponses'
