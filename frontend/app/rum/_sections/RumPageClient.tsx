'use client';

import { Radio, BookOpen, Copy, Check } from 'lucide-react';
import { useState, useEffect, useRef } from 'react';
import { ReportLayout } from '@/components/ReportLayout';
import { RumClient } from './RumClient';
import { RumFaroVersionCard } from '@/components/Rum/RumFaroVersionCard';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';

interface RumPageClientProps {
  initialServiceId?: string;
}

interface ServiceSyncProps {
  serviceId: string | null;
  onSync: (id: string | null) => void;
}

function ServiceSync({ serviceId, onSync }: ServiceSyncProps) {
  useEffect(() => {
    onSync(serviceId);
  }, [serviceId, onSync]);
  return null;
}

export default function RumPageClient({ initialServiceId }: RumPageClientProps) {
  const [showInstructions, setShowInstructions] = useState(false);
  const [activeServiceId, setActiveServiceId] = useState<string | null>(initialServiceId || null);
  const [copied, setCopied] = useState(false);

  // Generate a simple hash from serviceId for cache-busting
  // This changes if we modify RUM settings, so users get fresh versions
  const generateScriptHash = (serviceId: string | null) => {
    if (!serviceId) return '';
    const input = `${serviceId}-rum-v1`;
    let hash = 0;
    for (let i = 0; i < input.length; i++) {
      const char = input.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return Math.abs(hash).toString(16).substring(0, 8);
  };

  const scriptTag = activeServiceId
    ? `<script src="/js/rum.js?v=${generateScriptHash(activeServiceId)}"></script>`
    : '<script src="/js/rum.js?v=1"></script>';

  return (
    <>
      <ReportLayout
        title="Real User Monitoring"
        description="Monitor real user performance metrics including Core Web Vitals, JavaScript errors, and session analytics."
        icon={Radio}
        serviceId={initialServiceId}
        headerActions={
          <div className="flex items-center gap-3">
            <RumFaroVersionCard serviceId={activeServiceId} />
            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowInstructions(true)}
              className="flex items-center gap-2"
            >
              <BookOpen className="h-4 w-4" />
              Installation Instructions
            </Button>
          </div>
        }
      >
        {({ startTime, endTime, activeServiceId: sid, filterPayload }) => {
          return (
            <>
              <ServiceSync serviceId={sid} onSync={setActiveServiceId} />
              <RumClient
                serviceId={sid}
                startTime={startTime}
                endTime={endTime}
                filterPayload={filterPayload}
              />
            </>
          );
        }}
      </ReportLayout>

      <Dialog open={showInstructions} onOpenChange={setShowInstructions}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Installation Instructions</DialogTitle>
            <DialogDescription>
              Add Real User Monitoring to your website to start collecting performance data.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <h4 className="font-semibold text-sm">Step 1: Add the RUM Script</h4>
              <p className="text-sm text-muted-foreground">
                Add this single line to the `&lt;head&gt;` section of your HTML or any template that renders across your entire site. The version hash automatically updates when we release new RUM features:
              </p>
              <div className="bg-muted rounded-md overflow-hidden border">
                <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/50">
                  <span className="text-[10px] font-mono text-muted-foreground">installation code</span>
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label={copied ? 'Copied script tag' : 'Copy script tag'}
                    className="h-6 w-6 hover:bg-muted-foreground/10"
                    onClick={() => {
                      navigator.clipboard.writeText(scriptTag)
                      setCopied(true)
                      setTimeout(() => setCopied(false), 2000)
                    }}
                  >
                    {copied ? (
                      <Check className="h-3 w-3 text-emerald-500" />
                    ) : (
                      <Copy className="h-3 w-3" />
                    )}
                  </Button>
                </div>
                <div className="p-3 font-mono text-xs overflow-x-auto">
                  <code>{scriptTag}</code>
                </div>
              </div>
            </div>

            <div className="space-y-2">
              <h4 className="font-semibold text-sm">Step 2: Works Everywhere</h4>
              <p className="text-sm text-muted-foreground">
                This script works on any website regardless of framework:
              </p>
              <ul className="text-sm text-muted-foreground list-disc list-inside space-y-1">
                <li>Plain HTML, PHP, Django, Rails, etc.</li>
                <li>React, Vue, Svelte, Angular, Next.js</li>
                <li>Static sites, dynamic apps, and everything in between</li>
              </ul>
            </div>

            <div className="space-y-2">
              <h4 className="font-semibold text-sm">Step 3: Start Collecting Data</h4>
              <p className="text-sm text-muted-foreground">
                Once deployed, real user beacons will automatically be sent to your analytics dashboard when users visit your site. After collecting 1+ beacons, you&apos;ll see real performance data appear on this page.
              </p>
            </div>

            <div className="bg-blue-50 dark:bg-blue-950 p-3 rounded-md space-y-2">
              <p className="text-sm font-semibold text-blue-900 dark:text-blue-100">💡 Tips:</p>
              <ul className="text-xs text-blue-800 dark:text-blue-200 space-y-1">
                <li>• Beacons capture Web Vitals, JavaScript errors, and session info automatically</li>
                <li>• The script is lightweight and asynchronous — no performance impact</li>
                <li>• Data is sent securely to your Fastly-backed analytics backend</li>
                <li>• The version hash automatically updates when we release RUM improvements — just copy the script tag again from these instructions</li>
              </ul>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
