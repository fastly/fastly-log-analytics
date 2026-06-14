// Cookie name shared between the server layout (reads it via next/headers
// for SSR initial state) and the client AppLayout (writes it on toggle).
// This file deliberately has no 'use client' directive — Next.js wraps
// every export of a 'use client' module as a client reference on the
// server, which made the previous import-from-AppLayout pattern return
// undefined during SSR, defeating the whole point of the cookie.
export const SIDEBAR_COLLAPSED_COOKIE = 'fla.sidebarCollapsed'
