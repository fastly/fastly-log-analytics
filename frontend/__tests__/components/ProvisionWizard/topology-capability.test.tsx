import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ModeStep } from "@/components/ProvisionWizard/steps/ModeStep";
import { JoinStep } from "@/components/ProvisionWizard/steps/JoinStep";
import {
  ANALYST_PATH_A_UNSUPPORTED_REASON,
  INITIAL_CONFIG,
  type WizardState,
} from "@/components/ProvisionWizard/types";

function makeState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    step: "mode",
    setStep: vi.fn(),
    mode: "join",
    setMode: vi.fn(),
    analystPathASupported: true,
    analystPathAReason: null,
    joinPhase: "form",
    config: INITIAL_CONFIG,
    setConfig: vi.fn(),
    isAnalyzing: false,
    lines: [],
    status: "idle",
    sseError: null,
    icebergMetadataLocation: "",
    setIcebergMetadataLocation: vi.fn(),
    handleCheckFos: vi.fn(),
    fosStatus: "idle",
    fosError: "",
    selectedService: null,
    selectedCdnService: null,
    servicesData: [],
    configStatus: null,
    isCheckingConfig: false,
    handleCheckConfig: vi.fn(),
    ...overrides,
  } as unknown as WizardState;
}

describe("ProvisionWizard topology capability", () => {
  it("keeps Analyst: Join enabled for supported sync topology", () => {
    render(<ModeStep s={makeState({ analystPathASupported: true })} />);

    expect(
      screen.getByRole("button", { name: /analyst: join/i }),
    ).toBeEnabled();
  });

  it("disables Analyst: Join for unsupported scalable topology", () => {
    render(
      <ModeStep
        s={makeState({
          analystPathASupported: false,
          analystPathAReason: ANALYST_PATH_A_UNSUPPORTED_REASON,
        })}
      />,
    );

    expect(
      screen.getByRole("button", { name: /analyst: join/i }),
    ).toBeDisabled();
    expect(screen.getByText(ANALYST_PATH_A_UNSUPPORTED_REASON)).toBeVisible();
  });

  it("warns when a stale invite or manual entry reaches JoinStep", () => {
    render(
      <JoinStep
        s={makeState({
          analystPathASupported: false,
          analystPathAReason: ANALYST_PATH_A_UNSUPPORTED_REASON,
        })}
      />,
    );

    expect(
      screen.getByText(/independent analyst join is unavailable/i),
    ).toBeVisible();
    expect(screen.getByText(/old invite or entered credentials manually/i)).toBeVisible();
    expect(screen.getByText(/Path B/i)).toBeVisible();
  });
});
