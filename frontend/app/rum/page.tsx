import { HydrationBoundary } from '@tanstack/react-query';

import { fetchBootstrapServerSide } from '@/lib/ssr/bootstrap';
import { fetchRumStatusServerSide } from '@/lib/ssr/rum';
import { firstParam, seedDehydratedState } from '@/lib/ssr/seed';
import RumPageClient from './_sections/RumPageClient';

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
  const serviceId =
    firstParam(params.service) ??
    (bootstrap as { active_service_id?: string | null } | null)?.active_service_id ??
    undefined;

  let dehydratedState = null;
  if (serviceId) {
    try {
      const status = await fetchRumStatusServerSide(serviceId);
      if (status) {
        dehydratedState = seedDehydratedState(['rum-status', serviceId], status);
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
