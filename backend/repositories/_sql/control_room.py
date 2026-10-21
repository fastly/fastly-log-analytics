"""SQL templates for `backend.repositories.control_room`.

``{view}`` / ``{col}`` are pre-validated identifiers (``_safe_table`` and the correlate
dimension allow-list), never raw request input.
"""

from __future__ import annotations

RECENT_STANDARD_ERRORS = """
SELECT
    md5(concat_ws('|', CAST(timestamp AS VARCHAR), CAST(status AS VARCHAR),
                  COALESCE(url, ''), COALESCE(pop, ''), COALESCE(ip, ''),
                  CAST(row_number() OVER (
                      ORDER BY timestamp DESC, status, url, pop, ip
                  ) AS VARCHAR))) AS event_id,
    timestamp,
    TRY_CAST(status AS INTEGER) AS status,
    COALESCE(url, '') AS url,
    COALESCE(pop, '') AS pop,
    COALESCE(ip, '') AS client_ip
FROM {view}
WHERE timestamp >= now() - INTERVAL 2 MINUTE
  AND TRY_CAST(status AS INTEGER) BETWEEN 400 AND 599
ORDER BY timestamp DESC
LIMIT 20
"""

# ClickHouse (high-scale) dialect; ``{service_id:String}`` is a ClickHouse bind parameter, not a format field.
RECENT_HIGH_SCALE_ERRORS = """
SELECT
    toString(event_id) AS event_id,
    event_timestamp AS timestamp,
    toInt32OrZero(custom_fields['status']) AS status,
    url,
    custom_fields['pop'] AS pop,
    client_ip
FROM request_facts
WHERE service_id={service_id:String}
  AND event_timestamp >= now() - INTERVAL 2 MINUTE
  AND toInt32OrZero(custom_fields['status']) BETWEEN 400 AND 599
  AND publication_state = 'visible'
ORDER BY event_timestamp DESC
LIMIT 20
"""

CORRELATE_TOP_VALUES = """
SELECT {col} AS value, COUNT(*) AS count
FROM {view}
WHERE timestamp >= ?
GROUP BY 1
ORDER BY 2 DESC
LIMIT ?
"""

LATEST_TIMESTAMP_WINDOWED = "SELECT MAX(timestamp) AS latest FROM {view} WHERE timestamp >= ?"

LATEST_TIMESTAMP = "SELECT MAX(timestamp) AS latest FROM {view}"
