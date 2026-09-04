# Dashboard Refresh Behavior

## Decision

Disable the dashboard bundle's default five-second automatic refetch. The
dashboard should load data on navigation and refetch when the user changes the
service, time range, filters, metric, interval, or selected sections. Existing
manual refresh actions and React Query's normal stale/focus behavior remain
unchanged.

## Rationale

The current five-second interval is intended to keep the automatic relative
range live, but replacing the composite bundle causes visible card and chart
flicker. The dashboard is primarily an analysis surface rather than a live
monitor, so the default should favor stable rendering and lower request volume.
This is a targeted change to the dashboard bundle; other polling surfaces such
as Control Room, streaming analytics, admin health, and analyst heartbeat have
separate live or operational requirements and are not changed.

## Implementation and testing

Remove the `isAutoRange`-dependent `refetchInterval` from
`useDashboardBundle`, while retaining `refetchIntervalInBackground: false` and
the stale-view retry behavior. Update the hook test to assert that a resolved
automatic-range query does not schedule recurring requests. Existing tests
continue to cover the query key and request-body behavior, ensuring range and
filter changes still produce a new fetch.
