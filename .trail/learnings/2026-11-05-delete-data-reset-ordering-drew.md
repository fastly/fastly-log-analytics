---
title: Delete Data reset ordering
author: Drew
date: 2026-11-05
branch: feat/delete-data-and-fdlt-harness
id: l-16580c0d
scope: repo
status: active
src: agent
---

Delete Data resets must be ordered so no stale data re-enters. Every raw-candidate path applies a permanent ingest floor at the reset point, so raw files older than it are never converted. The fence waits out the svc read-cache TTL plus the plan lease (FENCE_WAIT_S=420) before cleanup, because a cached read or a leased plan can still act on pre-reset state until then. RUM is held until its watermark reset lands, otherwise RUM rows can be written against the old watermark. Cleanup deletes by LastModified < reset_cas_at plus head references, never by listing the current state, so files written after the reset survive.
