import { INITIAL_CONFIG } from '@/components/ProvisionWizard/types'

export const API_BASE = 'http://127.0.0.1:8000'

export const FULL_CONFIG = {
  ...INITIAL_CONFIG,
  endpoint_name: 'My EP',
  fos_region: 'us-east-1',
  fos_bucket_name: 'my-bucket',
  fos_prefix: 'logs/',
  fos_access_key: 'AK',
  fos_secret_key: 'SK',
  sample_rate: 1,
  edge_only: true,
  custom_condition: '',
  log_period: 60,
  cdn_service_name: 'My CDN',
  cdn_prefix: 'mycdn',
  cdn_shield: 'iad-va-us',
  cdn_url: '',
  cdn_secret: '',
  enable_cron_sync: true,
  delete_after: false,
  commit_interval_mins: 5,
  enable_cron_compact: true,
}

export function serviceStoreMockFactory() {
  const state = { activeServiceId: 'svc-test' as string | null }
  const useServiceStore: unknown = Object.assign(
    (selector?: (s: typeof state) => unknown) => (selector ? selector(state) : state),
    { getState: () => state },
  )
  return { useServiceStore }
}
