---
title: Avoid flexRender unmount when TanStack Table columnDefs depend on interactive state
author: Drew
date: 2026-10-28
branch: release/v3.0.0-beta4
id: l-0d7c9397
scope: repo
status: active
src: agent
touches:
  - frontend/app/admin/share/InvitationsPanel.tsx
---

In TanStack Table components whose columnDefs useMemo depends on interactive row state (e.g. InvitationsPanel copied/editingServicesFor), invoke function cell definitions directly via cell.column.columnDef.cell(cell.getContext()) rather than flexRender(cell.column.columnDef.cell, cell.getContext()), because flexRender mounts function cell defs as <Comp {...props} /> which changes component type identity on every state update and unmounts/remounts interactive Button DOM nodes mid-click.
