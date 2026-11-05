---
title: Base UI Dialog focus restoration race when step-advancing radio unmounts
author: Drew
date: 2026-10-29
branch: main
id: l-510ae09a
scope: repo
status: active
src: agent
touches:
  - app/web/components/provision/ProvisionWizard.tsx
---

Base UI Dialog FloatingFocusManager schedules a next-frame restoreFocusFrame.request(() => floatingFocusElement.focus()) when document.activeElement inside the dialog is unmounted and focus drops to document.body. When a step-advancing radio in ProvisionWizard unmounts on click, moving focus to [data-slot='dialog-content'] before setStep prevents that delayed focus steal from blurring elements focused on the new step.
