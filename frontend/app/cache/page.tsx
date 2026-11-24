import type { Metadata } from 'next'
import { CacheClient } from './CacheClient'

export const metadata: Metadata = {
  title: 'Browser Cache · Fastly Log Analytics',
}

export default function CachePage() {
  return <CacheClient />
}
