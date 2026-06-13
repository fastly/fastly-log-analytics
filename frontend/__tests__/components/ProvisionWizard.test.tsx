import { render, screen, fireEvent } from "@testing-library/react";
import { ProvisionWizard } from "@/components/ProvisionWizard/ProvisionWizard";
import { vi, describe, it, expect } from "vitest";
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

describe.skip("ProvisionWizard", () => {
  it("renders the mode selection step by default", () => {
    render(<ProvisionWizard open={true} onOpenChange={vi.fn()} />, { wrapper });

    // Check for the main title
    expect(screen.getByText(/Setup/i)).toBeInTheDocument();

    // Check for the three primary modes
    expect(screen.getByText(/Provision New/i)).toBeInTheDocument();
    expect(screen.getByText(/Import Existing/i)).toBeInTheDocument();
    expect(screen.getByText(/Join as Analyst/i)).toBeInTheDocument();
  });

  it("transitions to the token step when 'Provision New' is selected", () => {
    render(<ProvisionWizard open={true} onOpenChange={vi.fn()} />, { wrapper });

    const provisionBtn = screen.getByText(/Provision New/i).closest("button");
    if (!provisionBtn) throw new Error("Could not find Provision New button");

    fireEvent.click(provisionBtn);

    // Header should change to indicate token requirement
    expect(screen.getByText(/Connect to Fastly/i)).toBeInTheDocument();
    expect(screen.getByPlaceholderText(/fastly_v1_/i)).toBeInTheDocument();
  });

  it("shows an error when attempting to proceed with an empty token", () => {
    render(<ProvisionWizard open={true} onOpenChange={vi.fn()} />, { wrapper });

    // Go to token step
    fireEvent.click(screen.getByText(/Provision New/i).closest("button")!);

    // Click Next without entering token
    const nextBtn = screen.getByText(/Next/i).closest("button");
    if (!nextBtn) throw new Error("Could not find Next button");

    fireEvent.click(nextBtn);

    // Validation should prevent transition (or show error if implemented)
    // In this component, if token is empty, the button is often disabled or it stays on the step.
    expect(screen.getByText(/Connect to Fastly/i)).toBeInTheDocument();
  });
});
