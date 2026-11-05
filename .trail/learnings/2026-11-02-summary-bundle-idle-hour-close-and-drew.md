---
title: summary_bundle idle hour close and mid-hour presetRange bundle serving
author: Drew
date: 2026-11-02
branch: main
cost: { minutes: 95, tokens: 660720 }
id: l-37ff51a6
scope: repo
status: active
src: agent
touches:
  - app/src/ingest/bundle.rs
  - app/src/ingest/api.rs
---

When traffic stops mid-hour, watermark.key stays at the last raw object's minute forever, which kept summary_bundle's closed watermark inside that hour until new traffic arrived in a later hour. Using watermark.sweep.at when watermark.sweep.after == '' advances closed once the straggler sweep confirms no pending raw files remain. Also, presetRange clamps [from, to) to [earliest, latest + 1m], so the first/last hour of a range starts or ends mid-hour; api::files now serves hour_bundle whenever all windows of that hour fall inside [from, to).
