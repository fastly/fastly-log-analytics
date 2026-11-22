import { installRawSocketProxyGuard } from './lib/rawSocketGuard'

export function register(): void {
  if (process.env.NEXT_RUNTIME === 'nodejs') {
    installRawSocketProxyGuard()
  }
}
