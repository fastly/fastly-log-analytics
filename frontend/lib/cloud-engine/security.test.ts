import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  ACTIVE_CLIENTS_LIMIT,
  buildConnReuseBucketCaseSql,
  buildReqHeaderBucketCaseSql,
  buildSecurityConnReuseDistSql,
  buildSecurityFingerprintCoverageSql,
  buildSecurityIpv6AdoptionSql,
  buildSecurityProxiesSql,
  buildSecurityProxyDistSql,
  buildSecurityReqSizeDistSql,
  buildSecurityThreatIntelSql,
  buildSecurityTlsFingerprintsSql,
  buildSecurityTopIpsHeaderSql,
  buildSecurityVerifiedBotsTsSql,
  evaluateImpossibleDistance,
  expandSecuritySections,
  formatProxiesExportPayload,
  handleSecurityRequest,
  maskIpValue,
  setSecurityEngineClient,
  shapeSecurityProxiesFromRawRows,
  shapeSecurityThreatIntelRows,
  summarizeWellKnownBotRows,
  well_known_bots,
} from "./security";

describe("security cloud-engine module (F20)", () => {
  beforeEach(() => {
    setSecurityEngineClient(null);
  });

  it("expands security sections including tls_fingerprints->fingerprint_coverage and ngwaf union", () => {
    const s1 = expandSecuritySections(["tls_fingerprints"]);
    expect(s1.has("tls_fingerprints")).toBe(true);
    expect(s1.has("fingerprint_coverage")).toBe(true);

    const s2 = expandSecuritySections(["ngwaf_verified_bots"]);
    expect(s2.has("ngwaf_verified_bots")).toBe(true);
    expect(s2.has("ngwaf_verified_bots_ts")).toBe(true);

    expect(expandSecuritySections().size).toBe(14);
  });

  it("builds SQL for fingerprints, header size buckets (0-256B..>32KB), connection reuse, ipv6, proxies, and threat-intel", () => {
    const req = {
      start_time: "2026-06-15T00:00:00Z",
      end_time: "2026-06-15T01:00:00Z",
      bucket_seconds: 300,
    };
    const cols = new Set([
      "timestamp",
      "ip",
      "tls_ciphers_sha",
      "req_header_bytes",
      "conn_requests",
      "is_ipv6",
      "p_type",
      "waf_sig",
      "rtt_min",
      "tcp_rtt",
      "lat",
      "lon",
      "pop",
      "asn",
      "country",
      "city",
      "ja3",
      "ja4",
      "is_bot",
      "waf_logged_rules",
      "waf_blocked_rules",
    ]);

    expect(buildReqHeaderBucketCaseSql("h")).toContain("0-256B");
    expect(buildReqHeaderBucketCaseSql("h")).toContain(">32KB");
    expect(buildConnReuseBucketCaseSql("c")).toContain("1 (New)");
    expect(buildConnReuseBucketCaseSql("c")).toContain(">100");

    expect(buildSecurityTlsFingerprintsSql(req, cols).sql).toContain("tls_ciphers_sha");
    expect(buildSecurityFingerprintCoverageSql(req, cols).sql).toContain("populated");
    expect(buildSecurityReqSizeDistSql(req, cols).sql).toContain("0-256B");
    expect(buildSecurityTopIpsHeaderSql(req, cols).sql).toContain("max_header");
    expect(buildSecurityConnReuseDistSql(req, cols).sql).toContain("conn_requests");
    expect(buildSecurityIpv6AdoptionSql(req, cols).sql).toContain("is_ipv6");
    expect(buildSecurityProxyDistSql(req, cols).sql).toContain("p_type");
    expect(buildSecurityVerifiedBotsTsSql(req, cols).sql).toContain("VERIFIED-BOT.");
    expect(buildSecurityProxiesSql(req, cols).sql).toContain("rtt_min");
    expect(buildSecurityThreatIntelSql(req, cols).sql).toContain("tls_fingerprint");
  });

  it("classifies well_known_bots from User-Agent rows", () => {
    expect(well_known_bots.length).toBeGreaterThanOrEqual(8);
    const summary = summarizeWellKnownBotRows([
      { ua: "Mozilla/5.0 (compatible; Googlebot/2.1)", ip: "66.249.66.1", cnt: 50 },
      { ua: "Mozilla/5.0 (compatible; GPTBot/1.0)", ip: "20.171.206.10", cnt: 30 },
    ]);
    expect(summary).toHaveLength(2);
    expect(summary[0]?.id).toBe("Googlebot");
    expect(summary[0]?.requests).toBe(50);
    expect(summary[1]?.id).toBe("GPTBot");
  });

  it("evaluates impossible distance and shapes Watchdog proxies up to 100 active_clients", () => {
    // SJC to SYD is ~11,950 km; with rttMinUs = 5,000 us (5 ms), max physical distance is 5 * 100 + 150 = 650 km => impossible!
    const imp = evaluateImpossibleDistance({
      rttMinUs: 5000,
      tcpRttUs: 20000,
      clientLat: -33.8688,
      clientLon: 151.2093,
      pop: "SJC-CA",
    });
    expect(imp.impossibleDist).toBe(true);
    expect(imp.riskLevel).toBe("High");

    const shaped = shapeSecurityProxiesFromRawRows(
      [
        {
          ip: "198.51.100.42",
          asn: 13335,
          rtt_min: 4000,
          tcp_rtt: 60000,
          lat: -33.8688,
          lon: 151.2093,
          pop: "SJC",
          country: "AU",
          city: "Sydney",
        },
        {
          ip: "203.0.113.9",
          asn: 15169,
          rtt_min: 15000,
          tcp_rtt: 20000,
          lat: 37.33,
          lon: -121.89,
          pop: "SJC",
          country: "US",
          city: "San Jose",
        },
      ],
      false,
    );
    expect(shaped.active_proxies_count).toBe(1);
    expect(shaped.distance_mismatches_count).toBe(1);
    expect(shaped.active_clients[0]?.ip).toBe("198.51.100.42");
    expect(shaped.active_clients[0]?.impossible_distance).toBe(true);
    expect(shaped.active_clients[0]?.risk_level).toBe("High");
    expect(shaped.suspicious_isps[0]?.asn).toBe(13335);
    expect(ACTIVE_CLIENTS_LIMIT).toBe(100);
  });

  it("formats fastly-acl CSV (ip,comment) and plain exports and masks IPs when requested", () => {
    expect(maskIpValue("198.51.100.42", true)).toBe("198.51.100.xxx");
    expect(maskIpValue("2001:db8:85a3::8a2e:370:7334", true)).toBe("2001:db8::xxxx");

    const clients = [
      { ip: "198.51.100.1", risk_level: "High" as const, impossible_distance: true },
      { ip: "198.51.100.2", risk_level: "Medium" as const, impossible_distance: false },
    ];

    const aclHigh = formatProxiesExportPayload(clients, { threshold: "High", format: "fastly-acl" });
    expect(aclHigh.mediaType).toBe("text/csv");
    expect(aclHigh.body).toContain("ip,comment\n");
    expect(aclHigh.body).toContain("198.51.100.1,Watchdog block: Impossible distance mismatch");
    expect(aclHigh.body).not.toContain("198.51.100.2");

    const plainMed = formatProxiesExportPayload(clients, { threshold: "Medium", format: "plain" });
    expect(plainMed.mediaType).toBe("text/plain");
    expect(plainMed.body).toBe("198.51.100.1\n198.51.100.2\n");
  });

  it("dispatches /api/security/aggregates, /bundle, /threat-intel, and /proxies/export", async () => {
    setSecurityEngineClient({
      getAvailableColumns: vi.fn().mockResolvedValue(
        new Set(["timestamp", "ip", "tls_ciphers_sha", "req_header_bytes", "conn_requests", "is_ipv6", "p_type", "waf_sig", "ua", "rtt_min", "tcp_rtt", "lat", "lon", "pop", "asn", "country", "city", "ja3", "ja4"]),
      ),
      query: vi.fn().mockResolvedValue([
        {
          fingerprint: "sha-1",
          ip_count: 4,
          request_count: 20,
          total_rows: 100,
          populated: 80,
          bucket: "0-256B",
          count: 25,
          ip: "198.51.100.77",
          max_header: 4096,
          bucket_ts: "2026-06-15T00:00:00Z",
          pct: 62.5,
          type: "vpn",
          bot_type: "GOOGLEBOT",
          ua: "Googlebot/2.1",
          cnt: 10,
          rtt_min: 4000,
          tcp_rtt: 80000,
          lat: -33.86,
          lon: 151.2,
          pop: "SJC",
          asn: 13335,
          country: "AU",
          city: "Sydney",
          tls_fingerprint: "ja4_abc",
          requests: 30,
          top_country: "US",
          matched_waf_rules_count: 3,
          bot_percentage: 50,
          is_anonymized_proxy: 1,
        },
      ]),
    });

    const bundleRes = await handleSecurityRequest(
      new Request("https://example.test/api/security/bundle?start_time=2026-06-15T00:00:00Z&end_time=2026-06-15T01:00:00Z"),
    );
    expect(bundleRes?.status).toBe(200);
    const bundleJson = await bundleRes!.json();
    expect(bundleJson.tls_fingerprints?.[0]?.fingerprint).toBe("sha-1");
    expect(bundleJson.active_proxies_count).toBe(1);
    expect(bundleJson.active_clients?.[0]?.ip).toBe("198.51.100.77");

    const maskedBundleRes = await handleSecurityRequest(
      new Request("https://example.test/api/security/bundle?start_time=2026-06-15T00:00:00Z&end_time=2026-06-15T01:00:00Z", {
        headers: { "x-session-role": "analyst-no-pii" },
      }),
    );
    const maskedBundleJson = await maskedBundleRes!.json();
    expect(maskedBundleJson.active_clients?.[0]?.ip).toBe("198.51.100.xxx");

    const tiRes = await handleSecurityRequest(
      new Request("https://example.test/api/security/threat-intel?start_time=2026-06-15T00:00:00Z&end_time=2026-06-15T01:00:00Z"),
    );
    const tiJson = await tiRes!.json();
    expect(tiJson.data[0]?.tls_fingerprint).toBe("ja4_abc");
    expect(shapeSecurityThreatIntelRows([]).data).toEqual([]);

    const exportRes = await handleSecurityRequest(
      new Request("https://example.test/api/security/proxies/export?start_time=2026-06-15T00:00:00Z&end_time=2026-06-15T01:00:00Z&threshold=High&format=fastly-acl"),
    );
    const csvText = await exportRes!.text();
    expect(csvText).toContain("ip,comment");
    expect(csvText).toContain("198.51.100.77");
  });
});
