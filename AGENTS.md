# Agent Operating Policy

This is the single repository-wide operating policy. Other agent entrypoints
refer here; do not copy this policy into them. Architecture, shared helpers and
numbered traps live in [CONTRIBUTOR_GUIDE.md](CONTRIBUTOR_GUIDE.md). Read only the
sections relevant to the task. This policy overrides older workflow examples in
reference docs and skills.

## Start or resume

- Inspect git status, staging, branch, worktrees and active deployment ownership.
  Preserve parallel work; never reset, amend or clean another session's changes. Verify
  local and remote SHAs before publication or deployment.
- Read the relevant page/cron spec and
  contributor-reference traps. For frontend work, also read the relevant locally
  vendored Next.js documentation before changing components, pages or hooks.

## Execute

- Once the requested outcome is clear, implement, verify and fix routine issues
  without approval checkpoints. Ask only for a material unresolved decision or
  an action outside the authorization already given.
- Keep changes surgical. Reuse existing domain helpers; preserve API contracts,
  exact time bounds, filters, totals, tenancy, masking and error behavior.
- Explain discovered bugs in plain English, then fix them or record them in the
  active task list. Do not leave findings only in conversation.
- Use a small coordinated fleet only for genuinely independent work. Do not
  profile concurrently or duplicate another agent's investigation.
- Keep credentials, deployment identifiers, private origins and evidence out of
  tracked files. Never commit ignored operator configuration.

## Verify and publish

- **Local is for basic checks:** focused tests, changed-file lint/format and
  relevant type checks. Do not start full local CI, Docker builds or Next.js
  builds by default. Use `uv` for Python and the existing frontend package manager.
- **Hosted CI runs full gates.** Preserve tests, coverage floors and ratchets.
  Never weaken a gate, bypass a hook or retry an unexplained failure blindly.
- Commit only owned changes using explicit pathspecs and normal hooks. Push the
  intended feature/release branch; verify its exact remote SHA and hosted results.
  A push to a different branch does not establish that release image builds ran.
- Never push main/master, force-push, create or merge PRs, publish releases,
  deploy or repair source data without explicit authorization for that action.
  A requested branch merge is not permission to merge a PR.
- Documentation/configuration cleanup does not imply a new deployment. Report
  gates that are still pending rather than claiming the change fully qualified.

## Deployed verification

- Deploy using the procedures in [docs/deploy/](docs/deploy/), from a pushed commit
  whose required gates pass. Do not improvise a manual rollout, and do not
  overwrite newer parallel published work.
- Reach remote environments through the configured admin mTLS gateway; never
  print credentials.
- Require positive browser checks and monitored-error checks after a rollout.
  Footer parity alone does not prove loaded backend/worker revision parity.
- After deployment, report request-event age per environment, with unavailable
  values explicit. Do not call event age clock-qualified ingestion latency.
- Traffic generators, tracker publication and cache purges require specific
  authorization.

## Performance and handoff

- For section-level performance work, check `scripts/lib/performance_budgets.json`.
  Work one page and one measured bottleneck at a time.
- Separate owner computations, cache hits, followers and historical diagnostics.
  Never sum overlapping spans, call wall time CPU or claim painted completion
  without browser evidence. Preserve exact-origin guards and analyst cleanup.
- Before stopping or compacting, record the branch/SHA, exact gate results,
  deployment outcome, remaining blockers and next action in the active task list.
  Label historical notes as historical.
- Remove only your temporary verification artifacts. Preserve sources referenced
  by active runtime mounts and work owned by other sessions.
