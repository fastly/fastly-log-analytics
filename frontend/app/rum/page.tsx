import { HydrationBoundary } from '@tanstack/react-query';

import { fetchRumStatusServerSide } from '@/lib/ssr/upstream';
import { resolvePageServiceId, seedDehydratedState } from '@/lib/ssr/upstream';
import RumPageClient from './_sections/RumClient';
import { queryKeys } from "@/lib/api";

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
  const serviceId = await resolvePageServiceId(params);

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
