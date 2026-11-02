#!/usr/bin/env groovy

@Library('pipeline@v2-stable') _

// Builds the backend, frontend and admin-gateway images from their Dockerfiles
// (repository root is the build context).
def backendImage = env.FLA_BACKEND_IMAGE ?: 'fastly/fastly-log-analytics-backend'
def frontendImage = env.FLA_FRONTEND_IMAGE ?: 'fastly/fastly-log-analytics-frontend'
def adminGatewayImage = env.FLA_ADMIN_GATEWAY_IMAGE ?: 'fastly/fastly-log-analytics-admin-gateway'

def cache = true
def buildContainers = false
def makeRelease = false

// Override the ref pattern per job with FLA_BUILD_REF_PATTERN.
def buildRefPattern = env.FLA_BUILD_REF_PATTERN ?: '^(origin/)?(main|release/.+|v[0-9]+.*)$'
def ref = getBuildRef()
if (ref.name =~ buildRefPattern) {
  cache = true
  buildContainers = true
}
if (ref.name =~ '^(origin/)?v[0-9]+') {
  makeRelease = true
}

fastlyPipeline(script: this) {

  if (buildContainers) {
    if (!backendImage || !frontendImage || !adminGatewayImage) {
      error('FLA_BACKEND_IMAGE, FLA_FRONTEND_IMAGE and FLA_ADMIN_GATEWAY_IMAGE must be configured')
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
  }
  if (makeRelease) {
    if (!backendImage || !frontendImage || !adminGatewayImage) {
      error('FLA_BACKEND_IMAGE, FLA_FRONTEND_IMAGE and FLA_ADMIN_GATEWAY_IMAGE must be configured')
    }
    stage('Tag Release') {
      def commitTag = env.GIT_COMMIT ? env.GIT_COMMIT.take(12) : (env.BUILD_NUMBER ?: 'latest')
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
