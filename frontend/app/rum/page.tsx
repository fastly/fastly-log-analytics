import { HydrationBoundary } from '@tanstack/react-query';

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap';
import { fetchRumStatusServerSide } from '@/lib/ssr/rum';
import { seedDehydratedState } from '@/lib/ssr/seed';
import { resolveServerServiceId } from '@/lib/ssr/service';
import RumPageClient from './_sections/RumPageClient';
import { queryKeys } from '@/lib/query-keys';

export const dynamic = 'force-dynamic';
export const metadata = {
  title: 'Real User Monitoring',
};

export default async function RumPage({
  searchParams,
}: {
  searchParams: Promise<{ service?: string | string[] }>;
}) {
  const params = await searchParams;
  const bootstrap = await fetchBootstrapServerSide();
  const serviceId = resolveServerServiceId(params, bootstrap);

  let dehydratedState = null;
  if (serviceId) {
    try {
      const status = await fetchRumStatusServerSide(serviceId);
      if (status) {
        dehydratedState = seedDehydratedState(queryKeys.rumStatus(serviceId), status);
      }
    } catch (e) {
      // Ignore prefetch errors - client fallback will handle it
    }
  }

  return (
    <HydrationBoundary state={dehydratedState}>
      <RumPageClient initialServiceId={serviceId} />
    </HydrationBoundary>
  );
}
