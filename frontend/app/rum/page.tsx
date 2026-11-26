import { Suspense } from 'react'

import RumLoading from './loading'
import RumPageClient from './_sections/RumClient'

export {
  RumPageClient,
  RumClient,
  RumStatusPanel,
  RumFaroVersionCard,
  RumNoDataBanner,
  RumEmptyWindowCard,
  RumInstallationDialog,
  RumVitalsTopSection,
  RumEnvironmentAndBeaconsSection,
  RumWorstPagesAndErrorsSection,
  RumLiveActivitySection,
  WORST_PAGE_COLUMNS,
  envName,
  getThresholdColors,
  buildEnvPieData,
  TREND_CHART_LAYOUT,
  STACKED_BAR_LAYOUT,
  DONUT_CHART_LAYOUT,
  type RumFaroVersionCardProps,
  type RumClientProps,
  type RumPageClientProps,
  type RumVersionsResponse,
  type RumWire,
  type RumAnalytics,
  type RumCwvDistribution,
  type CwvColors,
  type RumWorstPage,
  type RumJsError,
  type RumRawLog,
  type RumLiveEvent,
} from './_sections/RumClient'

export default function RumPage() {
  return (
    <Suspense fallback={<RumLoading />}>
      <RumPageClient />
    </Suspense>
  )
}
