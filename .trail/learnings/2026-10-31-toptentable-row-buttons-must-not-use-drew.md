---
title: TopTenTable row buttons must use pl-4.5 pr-2 -mx-2 to provide visible left padding while preserving fractional-grid compositor parity
author: Drew
date: 2026-10-31
branch: v3.0.0-rc1
id: l-11bb9e06
scope: repo
status: active
src: agent
touches:
  - frontend/__tests__/components/widgets.test.tsx
  - frontend/components/Dashboard/TopTenTable.tsx
  - frontend/e2e/pages/dashboard.spec.ts
  - frontend/components/ui/table.tsx
  - tests/scripts/dashboard_card_paint.test.cjs
---

## Decided
- Use `pl-4.5 pr-2 -mx-2` (`18px` left padding minus `8px` negative left margin = `10px` visible left padding) on row buttons and static IP-masked row divs in `frontend/components/Dashboard/TopTenTable.tsx`.
- Because `Table` (`frontend/components/ui/table.tsx`) wraps `<table>` in `<div data-slot="table-container" className="relative w-full overflow-x-auto">`, `-mx-2` shifts the button's left `rounded-sm` corner `-8px` into the clipped region while `pl-4.5` (`18px`) offsets the inner label text `+10px` to the right of `table-container`'s visible left edge.

## Tried and ruled out
- `px-2 -mx-2`: `overflow-x-auto` clips the `-8px` negative left margin, canceling out `px-2` (`8px`) and leaving `0px` visible left padding between the row bar edge and the label text.
- Removing `-mx-2` (`px-2.5` without `-mx-2`): places the `<button>`'s `rounded-sm` border-radius corner clip directly on fractional column boundaries (`x = 759.59375 .. 932.390625`), causing a 1–2 RGB unit Skia anti-aliasing delta between the scrolled `<main>` container and the absolute reference container in `tests/scripts/dashboard_card_paint.test.cjs`.
