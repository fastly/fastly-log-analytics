# ADR-23 — Content Security Policies and Assignments

**Status:** Accepted
**Date:** 2026-10-16

---

## Context

Content Security started as a flat list of single-condition rules ("block country CN", "pace ASN 7922")
pushed one at a time to the operator's blocklist function. Every rule was live the moment it was
sent, rules could not be defined ahead of an event, and the blocker had no notion of a service or of
the content being requested.

## Decision

Rules are grouped into **policies**, and a policy enforces nothing until an **assignment** points at it.

- **Policy** (per service, no sharing across services): optional country mass-block regex; block / pace /
  exempt per value for country, ASN, referer, host, proxy type and proxy description; individual
  subscriber-ID and IP blocks. Revisions are kept.
- **Assignment**: the service's single *default* (all content), or one content ID, with an optional begin/end
  window and a **WARN / ENFORCE mode per assignment**. At most one default per service, and at most one
  active assignment per content ID (windows may not overlap). No regex scope.
- **Evaluation** (at the edge, per request): the content-ID assignment if active, else the default (most
  specific wins, one assignment only). Individual subscriber/IP blocks first (exemptions never override
  them), then exemptions, then block/pace rules; strictest wins (block beats pace, lowest kbps among paces).
- **Live edits** change the policy in effect in place. With nothing in effect, a `Default` policy and default
  assignment (ENFORCE, no end date) are created.
- **Publish model**: Postgres is the source of truth (ADR-22). Each change is committed, then the service's
  whole active state is pushed idempotently to the blocker (`PUT /v2/services/{service_id}/policy-set`). A failed push
  leaves the change saved and "not yet live"; `published_rev` vs `rev` tracks it and a retry or the Sync button
  repairs it. The blocker stores one `svc:` document per service plus one KV key per individual entry.
- **VCL**: `config["block-mode"]` is now a master switch (`OFF`, `WARN` forces warn for everything, `ENFORCE`
  defers to each assignment's mode). `cs_mode` is logged as the effective mode, and `cs_policy_id` /
  `cs_policy_reason` are logged so reports can attribute outcomes to a policy and rule.

The wire contract is summarized in AGENTS.md (Content Security policies); the enforcing implementation
(Compute function + VCL) lives in the operator's `fastly-test-player` repository.

## Consequences

- Blocks can be prepared and rehearsed (WARN) before an event, and begin/end without anyone being online.
- A live edit of a shared policy affects every assignment of it within the service; the UI shows the reach.
- Fastly KV is eventually consistent, so "live" means "accepted by the blocker".
- There is no backward compatibility with the earlier flat blocklist: its API, table and Compute routes were removed, not migrated. A leftover `content_security_blocklist` table is simply unused.
- Publishes retry a 429/5xx and report the blocker's reason, since Fastly KV limits writes to one per second per key.
- Session-ID blocking is deliberately out of scope here.
