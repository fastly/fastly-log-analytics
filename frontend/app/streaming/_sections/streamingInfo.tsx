import React from 'react'

// ── CMCD token label maps ─────────────────────────────────────────────────────
// CTA-5004 §3.1 Object Type tokens
export const OBJECT_TYPE_LABELS: Record<string, string> = {
  m: 'Manifest',
  a: 'Audio',
  v: 'Video',
  av: 'Muxed A/V',
  i: 'Init Segment',
  c: 'Caption',
  tt: 'Timed Text',
  k: 'Crypto Key',
  o: 'Other',
}

// CTA-5004 §3.1 Streaming Format tokens
export const STREAMING_FORMAT_LABELS: Record<string, string> = {
  d: 'DASH',
  h: 'HLS',
  s: 'Smooth',
  o: 'Other',
}

// CTA-5004 §3.1 Stream Type tokens
export const STREAM_TYPE_LABELS: Record<string, string> = {
  v: 'VOD',
  l: 'Live',
}

export function cmcdLabel(map: Record<string, string>, code: string | null | undefined): string {
  if (!code) return 'Unknown'
  return map[code] ?? code
}

// ── Help content for each section ─────────────────────────────────────────────

export const STREAMING_INFO = {
  active_sessions: {
    title: 'Total Sessions',
    body: (
      <p className="text-sm text-muted-foreground">
        Count of distinct CMCD session IDs (<code>sid</code>) seen in the selected time range.
        Each video player session generates a unique ID that persists across all segment requests.
      </p>
    ),
  },
  rebuffer_rate: {
    title: 'Rebuffer Rate',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>
          Percentage of sessions that experienced at least one buffer starvation event.
          Reported by the player via the CMCD <code>bs</code> (Buffer Starvation) flag.
        </p>
        <p>
          A high rebuffer rate indicates viewers are experiencing playback interruptions —
          typically caused by insufficient throughput relative to the selected bitrate.
        </p>
      </div>
    ),
  },
  avg_bitrate: {
    title: 'Average Bitrate',
    body: (
      <p className="text-sm text-muted-foreground">
        Mean encoded bitrate (<code>br</code>) across video segment requests, in kilobits per second.
        This reflects the quality level the player&apos;s ABR algorithm selected, not the delivery speed.
      </p>
    ),
  },
  avg_buffer: {
    title: 'Average Buffer Length',
    body: (
      <p className="text-sm text-muted-foreground">
        Mean buffer length (<code>bl</code>) in milliseconds at the time each segment was requested.
        Higher values indicate the player has more content buffered ahead — a healthy buffer is typically 10–30 seconds.
      </p>
    ),
  },
  median_throughput: {
    title: 'Median Throughput',
    body: (
      <p className="text-sm text-muted-foreground">
        Median measured throughput (<code>mtp</code>) in kbps, as reported by the player.
        This is the player&apos;s estimate of available bandwidth, used by ABR algorithms to select bitrate levels.
      </p>
    ),
  },
  peak_viewers: {
    title: 'Peak Viewers',
    body: (
      <p className="text-sm text-muted-foreground">
        Maximum number of concurrent sessions in any single time bucket during the selected range.
        Represents the peak audience size.
      </p>
    ),
  },
  avg_session_duration: {
    title: 'Avg Session Duration',
    body: (
      <p className="text-sm text-muted-foreground">
        Average time span between the first and last CDN request for each session (<code>sid</code>).
        Approximates viewing duration — actual watch time may be slightly longer since the last
        segment fetch occurs before playback ends.
      </p>
    ),
  },
  active_viewers: {
    title: 'Active Viewers',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>
          Number of distinct CMCD sessions (<code>sid</code>) active in each time bucket.
          A session is &ldquo;active&rdquo; if at least one request carrying its ID appears in the bucket.
        </p>
        <p>
          The <strong>Rebuffer Rate</strong> overlay shows what percentage of those sessions experienced
          buffer starvation (<code>bs</code>) — correlating audience size with quality degradation.
        </p>
      </div>
    ),
  },
  session_starts: {
    title: 'Session Starts',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>
          Number of new sessions that started in each time bucket — counted as sessions whose
          first CDN request falls within the bucket. Reveals traffic arrival patterns: release
          timing, marketing spikes, and audience ramp-up.
        </p>
        <p>
          Compare with Active Viewers above: a spike in starts without a corresponding rise in
          active viewers suggests short viewing sessions or high bounce rates.
        </p>
      </div>
    ),
  },
  session_duration: {
    title: 'Session Duration Distribution',
    body: (
      <p className="text-sm text-muted-foreground">
        Distribution of session durations, approximated as the time between first and last CDN
        request per session. Shows content engagement — are viewers watching for seconds or hours?
      </p>
    ),
  },
  buffer_health: {
    title: 'Buffer Health',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>
          Tracks buffer depth and starvation over time. The <strong>p50</strong> and <strong>p95</strong> lines
          show the median and 95th-percentile buffer length across all video segment requests in each time bucket.
        </p>
        <p>
          The <strong>Starvation Rate</strong> bars show what percentage of requests in each bucket had the
          buffer starvation flag (<code>bs</code>) set — indicating the player&apos;s buffer ran dry and playback stalled.
        </p>
      </div>
    ),
  },
  bitrate_quality: {
    title: 'Bitrate & Quality',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>
          <strong>Avg Bitrate</strong> shows the mean encoded bitrate (<code>br</code>) of video segments over time.
          Drops indicate the ABR algorithm is downshifting quality, typically due to congestion.
        </p>
        <p>
          <strong>Utilization</strong> is the ratio of selected bitrate to the top available bitrate
          (<code>br/tb</code>). A value of 1.0 means the player is at the highest quality tier.
        </p>
      </div>
    ),
  },
  throughput: {
    title: 'Measured Throughput',
    body: (
      <p className="text-sm text-muted-foreground">
        Throughput percentiles (p50, p95, p99) as measured by the player (<code>mtp</code>).
        This is the player&apos;s bandwidth estimate used for ABR decisions — a wide gap between p50 and p95
        suggests variable network conditions across your audience.
      </p>
    ),
  },
  startup: {
    title: 'Startup Requests',
    body: (
      <p className="text-sm text-muted-foreground">
        Percentage of requests marked as startup (<code>su</code>) over time.
        Startup requests are the initial segments fetched when a viewer begins playback.
        A sustained high ratio may indicate frequent session restarts or short viewing sessions.
      </p>
    ),
  },
  top_content: {
    title: 'Top Content',
    body: (
      <p className="text-sm text-muted-foreground">
        Breakdown by Content ID (<code>cid</code>). Shows session count, rebuffer rate, average bitrate,
        and average buffer length per content item. Useful for identifying specific titles with quality issues.
      </p>
    ),
  },
  object_type_dist: {
    title: 'Object Type Distribution',
    body: (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>
          Distribution of CMCD object types (<code>ot</code>) across all requests.
          Types: <strong>Video</strong> (v), <strong>Audio</strong> (a), <strong>Manifest</strong> (m),
          <strong> Init Segment</strong> (i), <strong>Caption</strong> (c), <strong>Crypto Key</strong> (k).
        </p>
        <p>
          Requests without a CMCD payload appear as &ldquo;Unknown&rdquo; — these are non-media
          requests (page loads, API calls) that pass through the same CDN service.
        </p>
      </div>
    ),
  },
  rebuffer_by_country: {
    title: 'Rebuffer Rate by Country',
    body: (
      <p className="text-sm text-muted-foreground">
        Rebuffer rate and session count broken down by viewer country (from the CDN edge POP&apos;s GeoIP).
        High rebuffer rates in specific regions may indicate peering issues or insufficient edge capacity.
      </p>
    ),
  },
  rebuffer_by_asn: {
    title: 'Rebuffer Rate by ASN',
    body: (
      <p className="text-sm text-muted-foreground">
        Rebuffer rate broken down by the viewer&apos;s Autonomous System (ISP/network provider).
        Useful for identifying ISP-specific delivery issues — e.g., a single ISP with a disproportionately
        high rebuffer rate may indicate a peering or last-mile problem.
      </p>
    ),
  },
}
