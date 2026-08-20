/**
 * RUM Admin Configuration Page
 * Mirrors session_scoring/page.tsx pattern:
 * - Enable/disable RUM via SSE modal
 * - Status display (enabled, drift, health)
 * - Admin-only access
 */

import { Metadata } from 'next';
import { RumStatusPanel } from '@/components/Rum/RumStatusPanel';

export const metadata: Metadata = {
  title: 'RUM Configuration',
};

export default function RumAdminPage() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <h1 className="text-3xl font-bold">Real User Monitoring</h1>
        <p className="text-muted-foreground">
          Configure and monitor RUM collection for your service
        </p>
      </div>

      <RumStatusPanel />
    </div>
  );
}
