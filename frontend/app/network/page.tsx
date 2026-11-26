import { Suspense } from 'react'

import Loading from './loading'
import NetworkClient from './_sections/NetworkClient'

export {
  NetworkClient,
  GlobalHealthHelp,
  AvgRttHelp,
  WorstAsnHelp,
  WorstRegionHelp,
  HeatmapHelp,
  AsnLeaderboardHelp,
  MetroLeaderboardHelp,
  ShieldingHelp,
  HealthBadge,
  SHIELDING_COLUMNS,
  getShieldingLabels,
  adjustShieldingRows,
  SHIELDING_MIN_REQUESTS_DEFAULT,
  SHIELDING_MIN_REQUESTS_OPTIONS,
  type ShieldingRowInput,
} from './_sections/NetworkClient'

export default function NetworkPage() {
  return (
    <Suspense fallback={<Loading />}>
      <NetworkClient />
    </Suspense>
  )
}
