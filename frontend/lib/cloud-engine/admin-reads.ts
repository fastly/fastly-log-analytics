import { adminRequest, getLoggedTraffic, type QuarantineItem } from "./telemetry/admin";

export async function cloudLogAccounting(request: Request): Promise<Response> {
  const traffic = await getLoggedTraffic(request.signal);
  const hours = Math.min(720, Math.max(24, Number(new URL(request.url).searchParams.get("hours")) || 24));
  const from = Math.max(traffic.from, traffic.to - hours * 3600);
  const days = traffic.days.filter((day) => day.day >= from && day.day < traffic.to);
  if (!days.length || days.some((day) => day.expected_rows == null || day.ingested_rows == null)) {
    return Response.json({
      available: false, reason: "Daily sample-adjusted request and ingested-row counts are not both available for this period.",
      by: "day", from_ts: new Date(from * 1000).toISOString(),
      to_ts: new Date(traffic.to * 1000).toISOString(),
      measurement_basis: "native_sample_adjusted_daily",
    });
  }
  const buckets = days.map((day) => {
    const expected = Number(day.expected_rows);
    const rows = Number(day.ingested_rows);
    const gap = Math.max(0, expected - rows);
    return { ts: new Date(day.day * 1000).toISOString(),
      fastly_requests: expected, our_rows: rows, gap, gap_pct: expected ? gap / expected : 0 };
  });
  const expected = buckets.reduce((total, bucket) => total + bucket.fastly_requests, 0);
  const rows = buckets.reduce((total, bucket) => total + bucket.our_rows, 0);
  const gap = Math.max(0, expected - rows);
  const worst = [...buckets].sort((a, b) => b.gap_pct - a.gap_pct)[0];
  return Response.json({
    available: true,
    by: "day",
    from_ts: new Date(from * 1000).toISOString(),
    to_ts: new Date(traffic.to * 1000).toISOString(),
    buckets,
    totals: { fastly_requests: expected, our_rows: rows, gap,
      gap_pct: expected ? gap / expected : 0,
      worst_bucket_ts: worst?.ts ?? null, worst_bucket_gap_pct: worst?.gap_pct ?? null },
    sustained_loss: null,
    measurement_basis: "native_sample_adjusted_daily",
  });
}

export async function cloudQuarantine(request: Request): Promise<Response> {
  const items: QuarantineItem[] = [];
  let cursor: string | null = null;
  for (let pageIndex = 0; ; pageIndex++) {
    if (pageIndex >= 32) throw new Error("Cloud quarantine listing exceeds the browser page budget");
    request.signal.throwIfAborted();
    const page: { items: QuarantineItem[]; next_cursor?: string | null; complete: boolean } = await adminRequest(
      `/api/admin/quarantine${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""}`, { signal: request.signal },
    );
    items.push(...page.items);
    if (page.complete) break;
    if (!page.next_cursor || page.next_cursor === cursor) throw new Error("Cloud quarantine listing is incomplete");
    cursor = page.next_cursor;
  }
  const mapped = items.map((item, index) => ({
    id: index + 1,
    source_type: item.key.includes("/rum/") ? "rum" : "request",
    original_key: item.key,
    byte_length: item.size ?? 0,
    line_ordinal: null,
    byte_offset: null,
    error_category: item.status,
    error_text: item.reason ?? "",
    sha256: "",
    quarantined_at: item.quarantined_at ?? item.first_seen,
    native_record: item.record,
    native_key: item.key,
    evidence_metadata_available: false,
  }));
  const url = new URL(request.url);
  if (url.pathname.endsWith("/summary")) {
    const times = mapped.map((item) => item.quarantined_at).filter((value): value is string => typeof value === "string").sort();
    return Response.json({
      total_items: mapped.length,
      total_bytes: mapped.reduce((total, item) => total + Number(item.byte_length), 0),
      request_items: mapped.filter((item) => item.source_type === "request").length,
      rum_items: mapped.filter((item) => item.source_type === "rum").length,
      oldest_at: times[0] ?? null, newest_at: times.at(-1) ?? null,
      category_counts: mapped.reduce<Record<string, number>>((counts, item) => {
        counts[String(item.error_category)] = (counts[String(item.error_category)] ?? 0) + 1;
        return counts;
      }, {}),
    });
  }
  const category = url.searchParams.get("error_category");
  const filtered = category ? mapped.filter((item) => item.error_category === category) : mapped;
  const offset = Math.max(0, Number(url.searchParams.get("offset")) || 0);
  const limit = Math.min(1000, Math.max(1, Number(url.searchParams.get("limit")) || 20));
  return Response.json({ items: filtered.slice(offset, offset + limit), total: filtered.length });
}
