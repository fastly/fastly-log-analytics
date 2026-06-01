/**
 * Shared className strings for the "panel" dialog pattern used across
 * the app's larger modals (CronSettingsModal, InviteAnalystDialog,
 * PopLocationsModal, TeardownDialog, SyncFromCloudModal,
 * CreateInviteDialog, LogSettingsModal, ProvisionWizard, SSEModal).
 *
 * Pattern: fixed-max-height panel with flex column layout, scrollable
 * body sandwiched between a sticky header (border-b) and footer
 * (border-t bg-muted/10). The body itself is left to each caller since
 * padding / scroll behavior varies (some use ScrollArea, some plain
 * overflow-y-auto, some inline form layouts).
 *
 * Headers come in two flavors:
 *  - `panelDialogHeaderSolid` (bg-background): used when the header has
 *    busy UI (badges, step indicators) and needs more visual weight.
 *  - `panelDialogHeaderMuted` (bg-muted/10): the default for simpler
 *    headers — pairs visually with the matching footer background.
 *
 * Compose with `cn()` so callers can append width / showCloseButton /
 * conditional classes without fighting tailwind-merge ordering.
 */

export const panelDialogContent =
  "max-h-[90vh] flex flex-col p-0 overflow-hidden"

export const panelDialogHeaderMuted =
  "px-6 pt-6 pb-4 border-b bg-muted/10 shrink-0"

export const panelDialogHeaderSolid =
  "px-6 pt-6 pb-4 border-b bg-background shrink-0"

export const panelDialogFooter =
  "px-6 py-4 border-t bg-muted/10 shrink-0"
