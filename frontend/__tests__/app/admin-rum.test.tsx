/**
 * @vitest-environment jsdom
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { QueryClientProvider } from '@tanstack/react-query';
import { createTestQueryClient } from '../helpers/query';
import { http, HttpResponse } from 'msw';
import { server } from '@/__tests__/helpers/msw/server';
import RumAdminPage from '@/app/admin/rum/page';
import Loading from '@/app/admin/rum/loading';
import { useServiceStore } from '@/stores/serviceStore';

const SVC = 'svc-admin-rum-test';

function makeWrapper() {
  const qc = createTestQueryClient();
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  };
}

describe('Admin RUM Configuration Page (/admin/rum)', () => {
  beforeEach(() => {
    useServiceStore.setState({
      activeServiceId: SVC,
      services: [
        { id: SVC, name: 'Test RUM Service' },
      ],
      isInitialized: true,
    });
  });

  it('renders route-level loading skeleton with zero-CLS reservations', () => {
    render(<Loading />);
    expect(screen.getByTestId('admin-rum-loading')).toBeInTheDocument();
    expect(screen.getByTestId('admin-rum-toolbar-skeleton')).toBeInTheDocument();
    expect(screen.getByTestId('admin-rum-status-skeleton-card')).toBeInTheDocument();
    expect(screen.getByText('Inspecting RUM beacon pipeline...')).toBeInTheDocument();
  });

  it('renders in-place skeleton while RUM status query is pending', async () => {
    server.use(
      http.get('*/api/services/:service_id/rum/status', async () => {
        await new Promise((r) => setTimeout(r, 200));
        return HttpResponse.json({ enabled: false });
      }),
      http.get('*/api/services', () =>
        HttpResponse.json({
          services: [{ service_id: SVC, name: 'Test RUM Service' }],
        })
      )
    );

    render(<RumAdminPage />, { wrapper: makeWrapper() });

    expect(screen.getByTestId('admin-rum-root')).toBeInTheDocument();
    expect(screen.getByTestId('admin-rum-status-reservation')).toBeInTheDocument();
    expect(screen.getByTestId('admin-rum-status-skeleton')).toBeInTheDocument();
    expect(screen.getByText('Inspecting RUM beacon pipeline...')).toBeInTheDocument();
  });

  it('renders enabled RUM status, collectors summary, and Faro SDK version card', async () => {
    server.use(
      http.get('*/api/services/:service_id/rum/status', () =>
        HttpResponse.json({
          enabled: true,
          enabled_at: '2026-10-18T10:00:00Z',
          deployed_vcl_sha: 'sha-123',
          current_vcl_sha: 'sha-123',
          vcl_drift: false,
          capture_vitals: true,
          capture_performance: true,
          capture_errors: true,
          capture_events: false,
          custom_condition: 'req.url !~ "^/internal"',
        })
      ),
      http.get('*/api/services/:service_id/rum/versions', () =>
        HttpResponse.json({
          available: ['2.3.1', '2.2.0'],
          current: '2.2.0',
          latest: '2.3.1',
          update_available: true,
        })
      ),
      http.get('*/api/services', () =>
        HttpResponse.json({
          services: [{ service_id: SVC, name: 'Test RUM Service' }],
        })
      )
    );

    render(<RumAdminPage />, { wrapper: makeWrapper() });

    await waitFor(() => {
      expect(screen.getByTestId('admin-rum-status-line')).toHaveTextContent('✅ Enabled');
    });

    expect(screen.getByTestId('admin-rum-status-badge')).toHaveTextContent(
      'Beacon Pipeline Active'
    );
    expect(screen.getByTestId('admin-rum-vcl-drift')).toHaveTextContent('✅ No');
    expect(screen.getByTestId('admin-rum-capture-summary')).toHaveTextContent(
      'Vitals, Performance, Errors'
    );
    expect(screen.getByTestId('admin-rum-enable-btn')).toBeDisabled();
    expect(screen.getByTestId('admin-rum-disable-btn')).not.toBeDisabled();

    await waitFor(() => {
      expect(screen.getByTestId('admin-rum-faro-version')).toHaveTextContent('2.2.0');
    });
    expect(screen.getByTestId('admin-rum-settings-btn')).toBeInTheDocument();
  });

  it('renders disabled RUM state and triggers manual refresh on click', async () => {
    let statusCalls = 0;
    server.use(
      http.get('*/api/services/:service_id/rum/status', () => {
        statusCalls += 1;
        return HttpResponse.json({
          enabled: false,
          vcl_drift: false,
        });
      }),
      http.get('*/api/services', () =>
        HttpResponse.json({
          services: [{ service_id: SVC, name: 'Test RUM Service' }],
        })
      )
    );

    render(<RumAdminPage />, { wrapper: makeWrapper() });

    await waitFor(() => {
      expect(screen.getByTestId('admin-rum-status-line')).toHaveTextContent('❌ Disabled');
    });

    expect(screen.getByTestId('admin-rum-status-badge')).toHaveTextContent(
      'Beacon Pipeline Inactive'
    );
    expect(screen.getByTestId('admin-rum-enable-btn')).not.toBeDisabled();
    expect(screen.getByTestId('admin-rum-disable-btn')).toBeDisabled();

    const callsBeforeRefresh = statusCalls;
    fireEvent.click(screen.getByTestId('admin-rum-refresh-btn'));

    await waitFor(() => {
      expect(statusCalls).toBeGreaterThan(callsBeforeRefresh);
    });
  });
});
