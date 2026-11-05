---
title: hosted_archive OCI manifest digest on containerd overlayfs
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-af33b30b
scope: repo
status: active
src: agent
touches:
  - scripts/lib/hosted_archive.py
  - tests/deploy/test_hosted_images.py
---

Because GitHub Actions runners use classic Docker graphdriver (overlay2) while Colima and GCE use the containerd image store (overlayfs), docker image save records manifest['image_id'] as the OCI config digest whereas docker image load on overlayfs indexes untagged archives under the OCI manifest digest in index.json. validate_archive now hash-verifies index.json -> blobs/sha256/<oci_id> -> config.digest == manifest['image_id'] so inspect_loaded, load_images, and remote_compose accept the verified OCI manifest digest without mutating manifest.
