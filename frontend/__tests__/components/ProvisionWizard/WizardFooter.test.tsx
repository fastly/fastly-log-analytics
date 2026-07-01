import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, it, expect, vi } from "vitest";
import { WizardFooter } from "@/components/ProvisionWizard/WizardFooter";
import type { WizardState } from "@/components/ProvisionWizard/useWizardState";

// WizardFooter is a pure presentational component over a WizardState bag — it
// only reads fields and invokes injected handlers, so a minimal mock cast via
// `as unknown as WizardState` (not `as any`) covers every branch we exercise.
function makeState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    step: "terraform",
    setStep: vi.fn(),
    mode: "provision",
    isDeploying: false,
    status: "idle",
    isDone: false,
    handleModalClose: vi.fn(),
    isAnalyzing: false,
    token: "tok",
    isLoadingServices: false,
    handleTokenSubmit: vi.fn(),
    selectedService: { id: "svc", name: "Svc" },
    validateMutation: { isPending: false },
    domainStatus: "available",
    config: { fos_bucket_name: "bucket", ngwaf_workspace_id: "" },
    fetchTerraformPreview: vi.fn(),
    handleDeploy: vi.fn(),
    handleAdminIngest: vi.fn(),
    fosStatus: "idle",
    handleAnalyzeLake: vi.fn(),
    importMode: "all",
    importRange: { start: "", end: "" },
    handleJoin: vi.fn(),
    joinPhase: "form",
    stop: vi.fn(),
    ...overrides,
  } as unknown as WizardState;
}

describe("WizardFooter — terraform (Terraform & VCL preview) step", () => {
  // The Terraform & VCL preview is a read-only side-trip: its only footer
  // navigation is the prominent "Back to Review" primary (the generic ghost
  // Back is suppressed on this step). Deploy / Complete Setup live only on the
  // Review (execute) step, so neither appears here in any mode.
  it("provision mode: 'Back to Review' returns to Review without deploying", async () => {
    const setStep = vi.fn();
    const handleDeploy = vi.fn();
    const handleAdminIngest = vi.fn();
    const s = makeState({
      mode: "provision",
      setStep,
      handleDeploy,
      handleAdminIngest,
    });
    render(<WizardFooter s={s} />);

    // No deploy/complete entry point on the preview step.
    expect(
      screen.queryByRole("button", { name: /deploy to fastly/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /complete setup/i }),
    ).not.toBeInTheDocument();

    await userEvent.click(
      screen.getByRole("button", { name: /back to review/i }),
    );
    expect(setStep).toHaveBeenCalledWith("execute");
    expect(handleDeploy).not.toHaveBeenCalled();
    expect(handleAdminIngest).not.toHaveBeenCalled();
  });

  it("ingest mode: 'Back to Review' returns to Review without ingesting", async () => {
    const setStep = vi.fn();
    const handleDeploy = vi.fn();
    const handleAdminIngest = vi.fn();
    const s = makeState({
      mode: "ingest",
      setStep,
      handleDeploy,
      handleAdminIngest,
    });
    render(<WizardFooter s={s} />);

    expect(
      screen.queryByRole("button", { name: /complete setup/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /deploy to fastly/i }),
    ).not.toBeInTheDocument();

    await userEvent.click(
      screen.getByRole("button", { name: /back to review/i }),
    );
    expect(setStep).toHaveBeenCalledWith("execute");
    expect(handleAdminIngest).not.toHaveBeenCalled();
    expect(handleDeploy).not.toHaveBeenCalled();
  });
});
