# Analyst Path A Scalable-Topology Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent independent Analyst Path A invites and joins from being used with the scalable Celery/DuckLake topology while preserving synchronous/legacy Path A and live shared-instance Path B.

**Architecture:** The backend owns one topology predicate and exposes its result as a typed capability in bootstrap service data. Invite creation, the CLI, and any registered join handler call that predicate; the ProvisionWizard consumes the capability to disable unsupported Join choices without inventing a second topology rule.

**Tech Stack:** FastAPI, Pydantic, Next.js 16 App Router, React 19, TanStack Query, openapi-fetch, Vitest, pytest, `uv`.

## Global Constraints

- Preserve Path A for synchronous/legacy deployments.
- Disable Path A only when `INGEST_MODE=celery`.
- Leave Path B sharing, cookies, middleware, and analyst-session logic unchanged.
- Return HTTP 409 with the standard error envelope for unsupported topology.
- Never silently continue with an empty dashboard or success-shaped join stream.
- Regenerate `frontend/openapi.json` and `frontend/types/api.generated.ts` after backend model changes.
- Run `make ci` after implementation; do not alter unrelated worktree changes.

---

## File Map

- Modify `backend/provision/orchestrator.py`: define the shared Path A capability and reuse it in invite generation.
- Modify `backend/provision/__init__.py`: export the shared capability for router/bootstrap consumers.
- Modify `backend/routers/services/core.py`: preserve the invite endpoint’s 409 mapping.
- Modify `backend/routers/bootstrap.py`: include the capability in each service’s bootstrap projection.
- Modify `backend/models/common.py`: type the capability and optional reason in `BootstrapService`.
- Modify `frontend/components/ProvisionWizard/types.ts`: represent the capability in wizard state.
- Modify `frontend/components/ProvisionWizard/steps/ModeStep.tsx`: disable/explain Join for unsupported topology.
- Modify `frontend/components/ProvisionWizard/useWizardState.ts`: initialize capability from bootstrap data and prevent direct Join transitions when unsupported.
- Modify `frontend/components/ProvisionWizard/steps/JoinStep.tsx`: render the explicit unsupported explanation for stale/manual entry.
- Modify `frontend/hooks/useBootstrap.ts`: preserve the generated capability type while hydrating bootstrap state.
- Modify `tests/test_provision_orchestrator.py`: test the shared predicate and sync/Celery invite behavior.
- Modify `tests/routers/test_invite_analyst.py`: test the 409 contract.
- Modify the existing bootstrap router tests: test capability projection for sync and Celery.
- Modify `tests/test_provision_cli_handlers.py`: test CLI failure and unchanged sync success.
- Create or modify frontend ProvisionWizard tests: test enabled sync Join, disabled Celery Join, and stale-entry messaging.
- Modify `frontend/openapi.json` and `frontend/types/api.generated.ts`: regenerate, never hand-edit.
- Modify `docs/adr/17-analyst-path-a-ducklake.md`: already records the accepted behavior; update only if implementation details differ.

### Task 1: Add and expose the backend topology capability

**Files:**
- Modify: `backend/provision/orchestrator.py`
- Modify: `backend/provision/__init__.py`
- Modify: `backend/models/common.py`
- Modify: `backend/routers/bootstrap.py`
- Test: `tests/test_provision_orchestrator.py`
- Test: existing bootstrap router test file found by `rg "BootstrapService|/api/bootstrap" tests`

**Interfaces:**
- Produces `analyst_path_a_supported() -> bool`.
- Produces `BootstrapService.analyst_path_a_supported: bool`.
- Produces `BootstrapService.analyst_path_a_reason: str | None`.
- Consumes `backend.config.INGEST_MODE`.

- [ ] **Step 1: Write the failing predicate and bootstrap tests**

```python
def test_analyst_path_a_supported_only_for_non_celery(monkeypatch):
    monkeypatch.setattr(svcconfig, "INGEST_MODE", "sync")
    assert orchestrator.analyst_path_a_supported() is True

    monkeypatch.setattr(svcconfig, "INGEST_MODE", "celery")
    assert orchestrator.analyst_path_a_supported() is False
```

Add a bootstrap assertion that a sync service serializes `analyst_path_a_supported: true`, while a Celery service serializes `false` and a non-empty reason containing `Path B`.

- [ ] **Step 2: Run the focused tests and verify failure**

Run:

```bash
uv run pytest tests/test_provision_orchestrator.py -k analyst_path_a_supported tests/routers -k bootstrap -q
```

Expected: FAIL because the predicate and typed bootstrap fields do not exist.

- [ ] **Step 3: Implement the capability and projection**

Add the module-level predicate:

```python
def analyst_path_a_supported() -> bool:
    from backend import config as svcconfig

    return svcconfig.INGEST_MODE != "celery"
```

Add the two optional fields to `BootstrapService`, and populate them from the predicate in the existing service projection. Use this exact reason for unsupported services:

```text
Independent analyst access is unavailable for scalable Celery/DuckLake services. Use live shared-instance analyst access (Path B).
```

Export `analyst_path_a_supported` from `backend.provision`.

- [ ] **Step 4: Run the focused tests and verify success**

Run the command from Step 2. Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/provision/orchestrator.py backend/provision/__init__.py backend/models/common.py backend/routers/bootstrap.py tests/test_provision_orchestrator.py tests/routers
git commit -m "feat: expose analyst path capability"
```

### Task 2: Complete join-boundary enforcement

**Files:**
- Modify: the registered independent-join handler, if repository inspection identifies one
- Test: the corresponding registered join-handler test file, if a handler exists
- Test: frontend `runJoin` tests in Task 3 when no backend join handler is registered

**Interfaces:**
- Existing invite and CLI boundaries already consume the shared predicate from the
  previously committed guard.
- Any registered independent-join handler calls `analyst_path_a_supported()`
  before FOS access, config writes, or SSE success events.

- [ ] **Step 1: Confirm the current join boundary**

```bash
rg -n 'provision/join|@router\.(get|post)\("/join"|def .*join' backend tests
```

Expected: no registered backend independent-join handler is found in the
current tree; the existing client-side `runJoin` boundary is the only reachable
stale/manual-entry path. The already committed invite and CLI guards remain the
backend protection for newly issued credentials.

- [ ] **Step 2: Add the client-side boundary test**

Run:

```bash
cd frontend && npm test -- --run frontend/__tests__/components/ProvisionWizard
```

Expected: the new `runJoin` test fails until the capability guard is added in
Task 3.

- [ ] **Step 3: Implement the join guard when a handler exists**

Keep request validation first. Immediately after it, call the shared predicate
and return HTTP 409 with the existing error helper:

```python
raise HTTPException(
    status_code=409,
    detail=make_error(
        "analyst_path_a_unsupported",
        "Independent analyst access is unavailable for scalable Celery/DuckLake "
        "services. Use live shared-instance analyst access (Path B).",
    ),
)
```

If a future branch adds a join handler, it must call the shared predicate before
any FOS access, config write, or SSE success event and return the same 409
envelope. This plan intentionally does not create a replacement endpoint.

- [ ] **Step 4: Run the focused tests and verify success**

Run the command from Step 2 after Task 3. Expected: the join guard test passes.

- [ ] **Step 5: Commit**

```bash
git add frontend/components/ProvisionWizard frontend/__tests__
git commit -m "fix: guard independent analyst join"
```

### Task 3: Make the ProvisionWizard topology-aware

**Files:**
- Modify: `frontend/components/ProvisionWizard/types.ts`
- Modify: `frontend/components/ProvisionWizard/steps/ModeStep.tsx`
- Modify: `frontend/components/ProvisionWizard/steps/JoinStep.tsx`
- Modify: `frontend/components/ProvisionWizard/useWizardState.ts`
- Modify: `frontend/hooks/useBootstrap.ts` only if its local bootstrap type needs the new fields
- Test: frontend ProvisionWizard and bootstrap tests under `frontend/__tests__/`

**Interfaces:**
- Wizard reads `BootstrapService.analyst_path_a_supported`.
- Unsupported Join renders a disabled control and the exact backend reason.
- Direct `handleJoin` rejects unsupported state before opening an SSE request.

- [ ] **Step 1: Write the failing frontend tests**

```typescript
it("disables Join for unsupported scalable topology", () => {
  render(<ModeStep {...stateWithCapability(false)} />);
  expect(screen.getByRole("button", { name: /analyst: join/i })).toBeDisabled();
  expect(screen.getByText(/Path B/i)).toBeInTheDocument();
});
```

Add a sync test asserting the Join control is enabled and a `runJoin` test asserting `start` is not called when `analystPathASupported` is false. Add a JoinStep test asserting the stale/manual-entry warning is visible.

- [ ] **Step 2: Run the focused tests and verify failure**

Run:

```bash
cd frontend && npm test -- --run frontend/__tests__/components/ProvisionWizard
```

Expected: FAIL because the wizard has no capability state or disabled Join rendering.

- [ ] **Step 3: Add capability state and guard transitions**

Extend `WizardState` with:

```typescript
analystPathASupported: boolean;
analystPathAReason: string | null;
```

Initialize the values from the active bootstrap service, defaulting to `true` for compatibility with existing synchronous installs that do not yet return the fields. In `ModeStep`, set `disabled={!s.analystPathASupported}` and render the reason below the option. In `runJoin`, return before `setIsDeploying` when unsupported and let JoinStep render the same reason if a stale draft reaches that step.

Use the existing Next.js and React Query conventions: do not add polling, intervals, or a new capability request; consume hydrated bootstrap state.

- [ ] **Step 4: Run the focused tests and verify success**

Run the command from Step 2. Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add frontend/components/ProvisionWizard frontend/hooks/useBootstrap.ts frontend/__tests__
git commit -m "feat: disable scalable analyst join"
```

### Task 4: Regenerate contracts, update docs, and run release validation

**Files:**
- Modify: `frontend/openapi.json` (generated)
- Modify: `frontend/types/api.generated.ts` (generated)
- Modify: `docs/adr/17-analyst-path-a-ducklake.md` if implementation wording needs correction
- Test: all targeted backend/frontend tests

**Interfaces:**
- Generated frontend types match the backend bootstrap schema.
- Documentation states that Celery Path A is rejected while sync Path A and Path B remain supported.

- [ ] **Step 1: Regenerate OpenAPI types**

Run:

```bash
make gen-types
```

Expected: generated artifacts are updated only if the bootstrap schema changed.

- [ ] **Step 2: Run targeted backend and frontend validation**

Run:

```bash
uv run pytest tests/test_provision_orchestrator.py tests/routers/test_invite_analyst.py tests/test_provision_cli_handlers.py -q
cd frontend && npm test -- --run frontend/__tests__/components/ProvisionWizard
```

Expected: all selected tests pass.

- [ ] **Step 3: Review the final diff**

Run:

```bash
git diff --check
git status --short
git diff --stat HEAD~3..HEAD
```

Confirm unrelated PostgreSQL/cron edits and `.agents/` are not staged.

- [ ] **Step 4: Run the full repository gate**

Run:

```bash
make ci
```

Expected: the application tests and checks pass. If Terraform provider handshake failures recur on macOS, report them as environment failures and rerun the Terraform portion in the supported Linux/container environment rather than weakening tests or changing provider code.

- [ ] **Step 5: Commit generated artifacts and documentation**

```bash
git add frontend/openapi.json frontend/types/api.generated.ts docs/adr/17-analyst-path-a-ducklake.md
git commit -m "chore: refresh analyst path contracts"
```
