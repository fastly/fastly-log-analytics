import { QueryClient, dehydrate } from "@tanstack/react-query";
import { StreamedBootstrapSeed } from "@/components/QueryProvider";
import { queryKeys, seedBootstrapDependentCaches } from "@/lib/api";
import { fetchBootstrapServerSide, shouldLoadRumScript } from "@/lib/ssr/upstream";

/**
 * Streams `/api/bootstrap` inside a `<Suspense>` boundary so `RootLayout` can
 * flush the HTML document and `<AppLayout>` navigation shell immediately for
 * FCP without waiting on the backend bootstrap round-trip.
 *
 * SECURITY: `fetchBootstrapServerSide` sets `X-Remote-Analyst:1` whenever the
 * inbound request carries the `X-Proxied-By-Caddy` marker, scoping the response
 * to the analyst session (or returning the anonymous stub) instead of falling
 * back to loopback=admin.
 *
 * Failure path: `fetchBootstrapServerSide` returns `null` on any error and the
 * client `useBootstrap` hook takes over unchanged.
 */
export async function BootstrapHydration({
  host,
  proxiedByCaddy,
  nonce,
}: {
  host: string | null;
  proxiedByCaddy: boolean;
  nonce?: string;
}) {
  if (process.env.STATIC_EXPORT === "1") return null;
  const bootstrap = await fetchBootstrapServerSide();
  if (!bootstrap) return null;

  const client = new QueryClient();
  client.setQueryData(queryKeys.bootstrap(), bootstrap);
  seedBootstrapDependentCaches(client, bootstrap);
  const activeSvc = bootstrap.active_service_id
    ? bootstrap.services?.find(
        (service) => service.service_id === bootstrap.active_service_id,
      )
    : undefined;
  const loadRumScript = shouldLoadRumScript({
    host,
    proxiedByCaddy,
    rumEnabled: activeSvc?.rum_enabled === true,
  });

  return (
    <>
      {/* RUM collector script only when the active service has RUM
          enabled — an unconditional emit makes every page load proxy
          /js/rum.js to the backend, which fails (ECONNRESET log spam)
          or 404s on services without RUM. */}
      {loadRumScript && <script src="/js/rum.js" nonce={nonce} async />}
      <StreamedBootstrapSeed state={dehydrate(client)} />
    </>
  );
}
