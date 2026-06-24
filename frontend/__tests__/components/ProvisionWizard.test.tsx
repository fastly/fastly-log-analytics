import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ProvisionWizard } from "@/components/ProvisionWizard/ProvisionWizard";
import { ResumeBanner } from "@/components/ProvisionWizard/ResumeBanner";
import {
  WIZARD_DRAFT_KEY,
  saveDraft,
  stripSecretsFromConfig,
} from "@/components/ProvisionWizard/wizard-draft";
import {
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  type WizardDraft,
} from "@/components/ProvisionWizard/types";
import { afterEach, beforeEach, vi, describe, it, expect } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

// ── Mocks ────────────────────────────────────────────────────────────────────

vi.mock("@/stores/serviceStore", () => ({
  useServiceStore: () => ({
    setActiveServiceId: vi.fn(),
    setServices: vi.fn(),
    services: [],
  }),
}));

vi.mock("@/stores/timezoneStore", () => ({
  useTimezoneStore: () => ({
    timezone: "UTC",
  }),
}));

vi.mock("@/hooks/useSSE", () => ({
  useSSE: () => ({
    progress: [],
    error: null,
    isComplete: false,
    start: vi.fn(),
    reset: vi.fn(),
  }),
}));

// Lucide icons often cause issues in Vitest if they use SVG primitives that
// jsdom doesn't fully support or if they are imported as ES modules.
vi.mock("lucide-react", async () => {
  const actual = await vi.importActual("lucide-react");
  return {
    ...actual as any,
    // Provide simple component mocks for the ones used in the first steps
    ChevronRight: () => <div data-testid="icon-chevron-right" />,
    Plus: () => <div data-testid="icon-plus" />,
    Database: () => <div data-testid="icon-database" />,
    Zap: () => <div data-testid="icon-zap" />,
  };
});

// ── Test Setup ───────────────────────────────────────────────────────────────

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: false },
  },
});

const wrapper = ({ children }: { children: React.ReactNode }) => (
  <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
);

function makeDraft(overrides: Partial<WizardDraft> = {}): WizardDraft {
  return {
    version: WIZARD_DRAFT_VERSION,
    draftId: "draft-test",
    mode: "provision",
    step: "fields",
    currentStep: "fields",
    selectedServiceId: "svc-1",
    selectedServiceName: "My Service",
    selectedCdnServiceId: null,
    selectedCdnServiceName: null,
    tokenInfo: { id: "tok-1", name: "tok", type: "user" },
    config: stripSecretsFromConfig({ ...INITIAL_CONFIG, endpoint_name: "EP" }),
    importMode: "all",
    importRange: { start: "", end: "" },
    syncEnabled: true,
    syncIntervalMins: "2",
    icebergMetadataLocation: "",
    updatedAt: new Date().toISOString(),
    createdAt: new Date().toISOString(),
    ...overrides,
  };
}

describe("ResumeBanner", () => {
  it("renders the step label from the saved draft and fires the action callbacks", async () => {
    const user = userEvent.setup();
    const onResume = vi.fn();
    const onStartFresh = vi.fn();
    const draft = makeDraft({ currentStep: "fields" });
    render(
      <ResumeBanner
        draft={draft}
        onResume={onResume}
        onStartFresh={onStartFresh}
      />,
    );
    expect(screen.getByText(/Resume previous wizard/i)).toBeInTheDocument();
    expect(
      screen.getByTestId("resume-banner-resume"),
    ).toHaveTextContent(/Resume from Log Fields/i);
    await user.click(screen.getByTestId("resume-banner-resume"));
    expect(onResume).toHaveBeenCalledOnce();
    await user.click(screen.getByTestId("resume-banner-start-fresh"));
    expect(onStartFresh).toHaveBeenCalledOnce();
  });

  it("hydrates the saved draft from localStorage when the wizard opens", () => {
    saveDraft(makeDraft({ currentStep: "fields" }));
    render(<ProvisionWizard open={true} onOpenChange={vi.fn()} />, {
      wrapper,
    });
    expect(screen.getByText(/Resume previous wizard/i)).toBeInTheDocument();
  });
});

afterEach(() => {
  window.localStorage.removeItem(WIZARD_DRAFT_KEY);
});

beforeEach(() => {
  window.localStorage.removeItem(WIZARD_DRAFT_KEY);
});

// NOTE (coverage honesty): the mode-select / token-step / empty-token unit
// tests that used to live here were `describe.skip`'d since v1.0.0 and so never
// ran. Re-enabling them surfaces a jsdom "Maximum update depth exceeded" render
// loop: the full ProvisionWizard mounts its draft-hydration + store-subscription
// effects, and the lightweight store mocks above return a fresh object on every
// call, so effects keyed on those values re-fire without converging. Rather than
// leave an inert skip (a false coverage signal), they were removed — the same
// mode-select → token → provision flow is exercised end-to-end (real stores, no
// mock-identity loop) by frontend/e2e/provision-wizard.spec.ts. The ResumeBanner
// unit tests above stay (they render the small presentational component and the
// wizard's draft-hydration entry, both of which are loop-free).
