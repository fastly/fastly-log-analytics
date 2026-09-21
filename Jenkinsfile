#!/usr/bin/env groovy

@Library('pipeline@v2-stable') _

// Builds and pushes the backend + frontend images for this repo's k8s
// deployment. Only two images exist: worker/beat/metadata-schema-init all
// reuse the backend image with a different `command` (see
// docker-compose.multipod.yml and the tracked chart's worker-/
// beat-deployment.yaml, both pointing at "-backend"). caddy is the
// GCE/Compose reverse proxy, superseded by k8s ingress, so it's not built
// here. Postgres/valkey/observability images are upstream, not ours.
//
// containers[] shape (imageName/dockerFile/cache/pushImage) matches the
// confirmed-working SigSci-RandomHack and SigSci-Demo-Site Jenkinsfiles.
// No `dockerContextPath` override needed: it defaults to env.WORKSPACE
// (repo root) per the fastlyDockerBuild reference, which is what both
// Dockerfiles require (backend/Dockerfile COPYs scripts/, compute/, and
// backend/ from outside its own dir; frontend/Dockerfile's COPY paths are
// `frontend/...`, implying repo-root context too).
//
// Both Dockerfiles also use BuildKit's `RUN --mount=type=cache` (uv's wheel
// cache, npm's tarball cache, Next's webpack/SWC cache) — a different
// caching mechanism from the Kaniko whole-layer remote cache `cache: true`
// enables here. Unverified whether Kaniko actually persists these mounts
// across builds; if not, a cache MISS on this layer (e.g. uv.lock/
// package-lock.json changes) reinstalls everything cold instead of reusing
// unchanged packages. Doesn't affect the cache-HIT case (Kaniko's own
// layer cache still serves the whole RUN layer when inputs are unchanged).
// Check a recent build's console output for `--mount` warnings/no-ops
// before assuming this is giving the speedup the Dockerfile comments claim.
def backendImage = 'fastly/se-demo/fastly-log-analytics-backend'
def frontendImage = 'fastly/se-demo/fastly-log-analytics-frontend'

def cache = true
def buildContainers = false
def makeRelease = false

// TEMPORARY: also builds off release/v3.0.0-beta2 to validate the pipeline
// before this lands on main. Drop that branch name once it merges.
def ref = getBuildRef()
if (ref.name in ['main', 'origin/main', 'release/v3.0.0-beta2', 'origin/release/v3.0.0-beta2']) {
  cache = true
  buildContainers = true
} else if (ref.name =~ '^v[0-9]+') {
  makeRelease = true
}

fastlyPipeline(script: this) {

  if (buildContainers) {
    stage('Build Containers') {
      def commitTag = env.GIT_COMMIT ? env.GIT_COMMIT.take(12) : (env.BUILD_NUMBER ?: 'latest')
      def containers = [
        [
          imageName: backendImage,
          dockerFile: 'backend/Dockerfile',
          cache: cache,
          reproducibleDigest: true,
          tagImage: commitTag,
          pushImage: true,
          timeout: 45,
        ],
        [
          imageName: frontendImage,
          dockerFile: 'frontend/Dockerfile',
          cache: cache,
          reproducibleDigest: true,
          tagImage: commitTag,
          pushImage: true,
          timeout: 45,
        ],
      ]
      fastlyDockerBuild(
        script: this,
        containers: containers,
        parallelBuild: true,
        submodules: false,
      )
      fastlyTagContainer(script: this,
        containerName: backendImage,
        containerVersion: "${commitTag}-${env.BUILD_NUMBER}",
        containerTag: [commitTag],
      )
      fastlyTagContainer(script: this,
        containerName: frontendImage,
        containerVersion: "${commitTag}-${env.BUILD_NUMBER}",
        containerTag: [commitTag],
      )
    }
  } else if (makeRelease) {
    stage('Tag Release') {
      def commitTag = env.GIT_COMMIT ? env.GIT_COMMIT.take(12) : env.GIT_COMMIT
      fastlyTagContainer(script: this,
        containerName: backendImage,
        containerVersion: commitTag,
        containerTag: [ref.name],
      )
      fastlyTagContainer(script: this,
        containerName: frontendImage,
        containerVersion: commitTag,
        containerTag: [ref.name],
      )
    }
  }
}
