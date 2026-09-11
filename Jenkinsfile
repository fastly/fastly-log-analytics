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
// Neither of those needed a cross-directory build context, so there's no
// confirmed `context` key here -- frontend/Dockerfile COPYs backend/ and
// scripts/, so it needs the repo root as build context. If fastlyDockerBuild
// doesn't already default to repo-root context, this will fail on the
// frontend build; check shared-pipeline-lib's actual step signature before
// trusting this to build correctly.
def backendImage = 'fastly-docker-playground/se-demo/fastly-log-analytics-backend'
def frontendImage = 'fastly-docker-playground/se-demo/fastly-log-analytics-frontend'

def cache = true
def buildContainers = false
def makeRelease = false

// TEMPORARY: also builds off release/v3.0.0-beta1 to validate the pipeline
// before this lands on main. Drop that branch name once it merges.
def ref = getBuildRef()
if (ref.name in ['main', 'origin/main', 'release/v3.0.0-beta1', 'origin/release/v3.0.0-beta1']) {
  cache = false
  buildContainers = true
} else if (ref.name =~ '^v[0-9]+') {
  makeRelease = true
}

fastlyPipeline(script: this) {

  if (buildContainers) {
    stage('Build Containers') {
      def containers = [
        [
          imageName: backendImage,
          dockerFile: 'backend/Dockerfile',
          cache: cache,
          pushImage: true,
        ],
        [
          imageName: frontendImage,
          dockerFile: 'frontend/Dockerfile',
          cache: cache,
          pushImage: true,
        ],
      ]
      fastlyDockerBuild(
        script: this,
        containers: containers,
        parallelBuild: true,
        checkout: true,
        submodules: false,
      )
    }
  } else if (makeRelease) {
    stage('Tag Release') {
      fastlyTagContainer(script: this,
        containerName: backendImage,
        containerVersion: env.GIT_COMMIT,
        containerTag: [ref.name],
      )
      fastlyTagContainer(script: this,
        containerName: frontendImage,
        containerVersion: env.GIT_COMMIT,
        containerTag: [ref.name],
      )
    }
  }
}
