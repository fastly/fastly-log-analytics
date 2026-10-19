#!/usr/bin/env groovy

@Library('pipeline@v2-stable') _

// Builds and pushes the backend, frontend and admin-gateway images for the
// Kubernetes deployment. The admin-gateway image (deploy/admin-gateway) is the
// client-mTLS admin entrypoint. The worker, beat and metadata-schema-init pods
// reuse the backend image with a different `command` (see
// docker-compose.multipod.yml and the chart's worker/beat deployments). Caddy
// is the single-VM Compose reverse proxy, replaced by ingress on Kubernetes, so
// it is not built here. Postgres, Valkey and observability images are upstream.
//
// Both Dockerfiles expect the repository root as their build context:
// backend/Dockerfile copies scripts/, compute/ and backend/, and
// frontend/Dockerfile copies from frontend/.
//
// The Dockerfiles use BuildKit `RUN --mount=type=cache` mounts for the uv, npm
// and Next.js caches. `cache: true` below enables whole-layer caching, which is
// a separate mechanism; whether the cache mounts persist between builds depends
// on the builder, so a changed lockfile may reinstall dependencies cold.
def backendImage = env.FLA_BACKEND_IMAGE ?: 'fastly/fastly-log-analytics-backend'
def frontendImage = env.FLA_FRONTEND_IMAGE ?: 'fastly/fastly-log-analytics-frontend'
def adminGatewayImage = env.FLA_ADMIN_GATEWAY_IMAGE ?: 'fastly/fastly-log-analytics-admin-gateway'

def cache = true
def buildContainers = false
def makeRelease = false

// Containers are built for main and for release/* branches. origin/main does
// not carry this Jenkinsfile yet (the release branch is the only place the
// pipeline exists and the source of the k8s deployments), so release/* must
// stay in the default. Override the whole pattern per job with
// FLA_BUILD_REF_PATTERN (a regex matched against the ref name).
def buildRefPattern = env.FLA_BUILD_REF_PATTERN ?: '^(origin/)?(main|release/.+)$'
def ref = getBuildRef()
if (ref.name =~ buildRefPattern) {
  cache = true
  buildContainers = true
} else if (ref.name =~ '^v[0-9]+') {
  makeRelease = true
}

fastlyPipeline(script: this) {

  if (buildContainers) {
    if (!backendImage || !frontendImage || !adminGatewayImage) {
      error('FLA_BACKEND_IMAGE, FLA_FRONTEND_IMAGE and FLA_ADMIN_GATEWAY_IMAGE must be configured in Jenkins')
    }
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
        [
          imageName: adminGatewayImage,
          dockerFile: 'deploy/admin-gateway/Dockerfile',
          cache: cache,
          reproducibleDigest: true,
          tagImage: commitTag,
          pushImage: true,
          timeout: 15,
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
      fastlyTagContainer(script: this,
        containerName: adminGatewayImage,
        containerVersion: "${commitTag}-${env.BUILD_NUMBER}",
        containerTag: [commitTag],
      )
    }
  } else if (makeRelease) {
    if (!backendImage || !frontendImage || !adminGatewayImage) {
      error('FLA_BACKEND_IMAGE, FLA_FRONTEND_IMAGE and FLA_ADMIN_GATEWAY_IMAGE must be configured in Jenkins')
    }
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
      fastlyTagContainer(script: this,
        containerName: adminGatewayImage,
        containerVersion: commitTag,
        containerTag: [ref.name],
      )
    }
  }
}
