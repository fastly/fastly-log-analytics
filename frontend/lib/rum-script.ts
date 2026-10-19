export function shouldLoadRumScript({
  host,
  proxiedByCaddy,
  rumEnabled,
}: {
  host: string | null;
  proxiedByCaddy: boolean;
  rumEnabled: boolean;
}): boolean {
  if (!rumEnabled || !proxiedByCaddy || !host) {
    return false;
  }

  const normalizedHost = host.toLowerCase();
  const hostname = normalizedHost.startsWith("[")
    ? normalizedHost.slice(1, normalizedHost.indexOf("]"))
    : normalizedHost.split(":")[0];
  return (
    hostname !== "localhost" &&
    hostname !== "127.0.0.1" &&
    hostname !== "::1" &&
    hostname !== "[::1]"
  );
}
