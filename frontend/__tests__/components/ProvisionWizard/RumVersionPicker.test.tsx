/**
 * @vitest-environment jsdom
 *
 * RumVersionPicker (Task 7) — the wizard's Faro Web SDK version selector,
 * shown in StorageStep when RUM is enabled.
 *
 * The npm registry backing GET /rum/versions is a third party and WILL be
 * down sometimes (backend surfaces that as a 503). Coverage here pins the
 * contract that matters most: a registry outage or an empty version list
 * must never block the operator from continuing the wizard unpinned — only
 * the happy path lets them choose an explicit version.
 */
import * as React from "react";
import { describe, it, expect, afterEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClientProvider } from "@tanstack/react-query";
import { http, HttpResponse } from "msw";

import { createTestQueryClient } from "../../helpers/query";
import { server } from "../../../tests/msw/server";
import { getApiBase } from "@/lib/api";
import { RumVersionPicker } from "@/components/ProvisionWizard/RumVersionPicker";

const API_BASE = getApiBase();
const SVC = "svc-new-1";

function renderPicker(
  onChange: (v: string | null) => void,
  value: string | null = null,
) {
  const qc = createTestQueryClient();
  return render(
    <QueryClientProvider client={qc}>
      <RumVersionPicker serviceId={SVC} value={value} onChange={onChange} />
    </QueryClientProvider>,
  );
}

afterEach(() => {
  server.resetHandlers();
});

describe("RumVersionPicker", () => {
  it("renders the loading state without crashing while the fetch is in flight", () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, async () => {
        // Never resolves within the test — asserts the loading branch renders
        // cleanly (a skeleton, not a crash or blank pane).
        await new Promise(() => {});
        return HttpResponse.json({ available: [] });
      }),
    );
    const onChange = () => {};
    const { container } = renderPicker(onChange);
    expect(screen.getByText(/faro web sdk version/i)).toBeInTheDocument();
    expect(container.querySelector('[data-slot="skeleton"]')).toBeTruthy();
  });

  it("lists available versions, marks latest, and reports a selection", async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json({
          available: ["1.9.0", "1.8.0", "1.7.0"],
          current: null,
          latest: "1.9.0",
          update_available: false,
        }),
      ),
    );
    const seen: (string | null)[] = [];
    const onChange = (v: string | null) => seen.push(v);
    const user = userEvent.setup();
    renderPicker(onChange);

    // Defaults to latest for a new service — recorded via onChange, not
    // provisioned (selection alone must not trigger any deploy call).
    await waitFor(() => expect(seen).toContain("1.9.0"));

    await user.click(screen.getByRole("combobox", { name: /faro web sdk version/i }));
    await user.click(await screen.findByRole("option", { name: /1\.7\.0/ }));
    expect(seen).toContain("1.7.0");
  });

  it("degrades to an inline message + retry on a registry 503, without ever calling onChange", async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(
          { error: "faro_registry_unavailable", message: "registry down" },
          { status: 503 },
        ),
      ),
    );
    const onChange = () => {
      throw new Error("onChange must not fire on a degraded fetch");
    };
    renderPicker(onChange);

    expect(await screen.findByText(/couldn't reach the npm registry/i)).toBeInTheDocument();
    expect(screen.getByText(/provision unpinned/i)).toBeInTheDocument();
    // The picker itself must not appear — nothing to select from.
    expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /retry/i })).toBeEnabled();
  });

  it("degrades the same way on an empty version list (not an error, but nothing to pick)", async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json({ available: [], current: null, latest: null, update_available: false }),
      ),
    );
    const onChange = () => {
      throw new Error("onChange must not fire when there is nothing to default to");
    };
    renderPicker(onChange);

    expect(await screen.findByText(/no versions available/i)).toBeInTheDocument();
    expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
  });

  it("retry re-fires the request after a registry outage clears", async () => {
    let calls = 0;
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () => {
        calls += 1;
        if (calls === 1) {
          return HttpResponse.json({ error: "faro_registry_unavailable" }, { status: 503 });
        }
        return HttpResponse.json({
          available: ["2.0.0"],
          current: null,
          latest: "2.0.0",
          update_available: false,
        });
      }),
    );
    const seen: (string | null)[] = [];
    const user = userEvent.setup();
    renderPicker((v) => seen.push(v));

    await screen.findByText(/couldn't reach the npm registry/i);
    await user.click(screen.getByRole("button", { name: /retry/i }));

    await waitFor(() => expect(seen).toContain("2.0.0"));
    expect(calls).toBe(2);
  });
});
