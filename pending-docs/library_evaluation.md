# Library Evaluation

Tracks spike outcomes for libraries the cleanup plan flags as "evaluate, then adopt if clear win." Each entry: what the spike tried, what it measured, the verdict.

Status legend:
- 🟢 **adopt** — clear net win, ship in the named phase
- 🟡 **partial** — adopt for some surfaces, custom code wins for others
- 🔴 **skip** — custom code stays, reason documented
- ⏳ **pending** — spike not yet run

---

## fastly SDK (`pip install fastly`)

**Phase:** 7 (field registry + provision spike)

**Hypothesis:** the official `fastly` SDK can replace large parts of [backend/provision/fastly_api.py](../backend/provision/fastly_api.py) (1,214 lines) with less code and equivalent edge-case handling.

**Spike target:** one workflow — VCL snippet upload (`create_vcl_snippet`, `update_vcl_snippet`, `delete_vcl_snippet`). Tightly scoped, hits the same endpoints today's custom client uses, exercises retry + error handling paths.

**Measurements to record:**

| Dimension | Custom client today | SDK | Δ |
|---|---|---|---|
| Lines of code (snippet workflow only) | TBD | TBD | TBD |
| VCR cassette length (request/response bytes) | TBD | TBD | TBD |
| Edge cases covered (shield-map, conditions, dependency-ordered upserts) | yes | TBD | TBD |
| Retry semantics (tenacity backoff parity) | tenacity decorators | TBD | TBD |
| Auth header handling (`fastly-key` vs SDK helpers) | manual | TBD | TBD |
| Test impact (pytest count needing rewrite) | — | TBD | TBD |

**Adoption rule:** adopt only if the spike shows ≥ 200 lines saved AND parity on edge cases (shield-map, conditions, ordered upserts).

**Verdict:** ⏳ pending (Phase 7 spike)

---

## APScheduler v4 (alpha)

**Phase:** 6 (cron isolation)

**Hypothesis:** APScheduler v4 supports separate-process scheduling natively. If Phase 6 picks separate-process based on Phase 1 thread-wait data, v4 would replace custom IPC plumbing.

**Spike trigger:** ONLY if Phase 6.1 picks separate-process. Otherwise the spike is skipped.

**Spike target:** stand up a prototype using v4's process-pool execution against a copy of the scheduler config. Compare against a custom-IPC approach.

**Measurements to record:**

| Dimension | Custom IPC | APScheduler v4 | Δ |
|---|---|---|---|
| Lines of code (worker + IPC plumbing) | TBD | TBD | TBD |
| Behavioral differences (job restart, retry, log forwarding) | — | TBD | TBD |
| Alpha-stability risk (open issues, beta cadence, public users) | n/a | TBD | TBD |
| Test impact | — | TBD | TBD |

**Adoption rule:** adopt v4 only if the win is clear AND stability acceptable (no blocker-tagged open issues in the SDK we exercise).

**Verdict:** ⏳ pending (conditional on Phase 6.1 outcome)

---

## Final summary (Phase 10.11)

Phase 10 closes this document with:
- One sentence per spike (verdict + LOC delta)
- Updated `pyproject.toml` if any library got adopted
- Updated `MONKEYPATCHES.md` if any patch got obsoleted (notably #6 may stay)

No "deferred to v2.1" outcomes. Every spike resolves in v2.0.
