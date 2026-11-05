---
title: "Phase 4b /share-login: cache share ToS/settings reads and gate invite-claim loading state to eliminate CLS"
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-cd2f2269
scope: repo
status: active
src: agent
touches:
  - backend/core/share_db/__init__.py
  - frontend/__tests__/app/share-login.test.tsx
  - frontend/app/share-login/page.tsx
  - tests/remote_access/test_share_db.py
---

GET /api/admin/share/tos and GET /api/admin/share/settings ran uncached PostgreSQL queries on every call (93-195ms warm). Added 30s BoundedTTLCache instances (_TOS_CACHE, _SETTINGS_CACHE) in backend/core/share_db/__init__.py registered with CacheRegistry and invalidated on publish_tos_version/set_setting. Also fixed CLS=0.003383 on /share-login?invite=<token> where auth-config resolved before claim/{token}, mounting the form and then shifting it down +56px when the invite banner or error alert rendered; ShareLoginForm now waits for both authConfigQuery and initial claimQuery before transitioning out of the loading skeleton.
