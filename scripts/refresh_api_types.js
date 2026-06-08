#!/usr/bin/env node
// Regenerate frontend/types/api.generated.ts from openapi.json only when
// openapi.json is newer (or the types file is missing). Run after
// generate_openapi.py — which preserves openapi.json's mtime when the
// schema is unchanged, so the common "nothing changed since last run"
// path skips the ~155ms openapi-typescript invocation entirely.
//
// Freshness is preserved because generate_openapi.py always introspects
// the live FastAPI app; we only skip the downstream typescript regen
// when there's provably nothing new to generate.

const fs = require('fs')
const { execSync } = require('child_process')

const SCHEMA = 'openapi.json'
const TYPES = 'types/api.generated.ts'

const mtime = (p) => {
  try {
    return fs.statSync(p).mtimeMs
  } catch {
    return 0
  }
}

if (mtime(SCHEMA) <= mtime(TYPES)) {
  console.log(`${TYPES} already up-to-date with ${SCHEMA}, skipping openapi-typescript`)
  process.exit(0)
}

execSync(`openapi-typescript ${SCHEMA} -o ${TYPES}`, { stdio: 'inherit' })
